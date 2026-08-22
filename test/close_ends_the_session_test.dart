// What `close()` returning is worth.
//
// A close that reports success while the socket is still up is worse than one
// that fails: the caller has no reason to look again, and a pool that has just
// been told every connection is shut removes them from its own bookkeeping, so
// nothing is left holding a reference to try again with. The socket then lives
// as long as the process, and the Postgres backend behind it sits `idle`
// holding a connection slot.
//
// The orderly shutdown -- Terminate, flush, close the sink -- is the path that
// makes the server reclaim the slot promptly, so it stays. What these tests
// pin is that failing to complete it is not allowed to mean failing to close.
//
// A proxy sits between the client and the real server so the test can watch
// the socket instead of asking the client whether it thinks it closed it.

import 'dart:async';
import 'dart:io';

import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'docker.dart';

/// Forwards to the real server, and can be told to stop.
class _WatchingProxy {
  _WatchingProxy._(this._server, this._targetPort);

  final ServerSocket _server;
  final int _targetPort;
  final _clientsStillOpen = <Socket>{};
  final _clientClosed = <Completer<void>>[];

  /// When true, nothing the client says reaches the server any more, and
  /// nothing the server says reaches the client. The sockets on both sides
  /// stay up: this is a wedge, not a disconnection, and it is the shape of a
  /// close that cannot complete its handshake.
  bool wedged = false;

  int get port => _server.port;

  /// Client sockets the proxy has accepted and not seen close.
  int get openClients => _clientsStillOpen.length;

  static Future<_WatchingProxy> start(int targetPort) async {
    final socket =
        await ServerSocket.bind(InternetAddress.loopbackIPv4, 0, shared: false);
    final proxy = _WatchingProxy._(socket, targetPort);
    socket.listen(proxy._accept);
    return proxy;
  }

  Future<void> _accept(Socket client) async {
    _clientsStillOpen.add(client);
    final closed = Completer<void>();
    _clientClosed.add(closed);

    late Socket upstream;
    try {
      upstream =
          await Socket.connect(InternetAddress.loopbackIPv4, _targetPort);
    } catch (_) {
      client.destroy();
      _clientsStillOpen.remove(client);
      if (!closed.isCompleted) closed.complete();
      return;
    }

    void finish() {
      _clientsStillOpen.remove(client);
      if (!closed.isCompleted) closed.complete();
      client.destroy();
      upstream.destroy();
    }

    unawaited(client.done.catchError((Object _) => client));
    unawaited(upstream.done.catchError((Object _) => upstream));

    client.listen(
      (data) {
        if (!wedged) {
          try {
            upstream.add(data);
          } catch (_) {}
        }
      },
      onDone: finish,
      onError: (Object _) => finish(),
    );
    upstream.listen(
      (data) {
        if (!wedged) {
          try {
            client.add(data);
          } catch (_) {}
        }
      },
      onDone: finish,
      onError: (Object _) => finish(),
    );
  }

  /// Fails unless every client socket the proxy accepted has been closed by
  /// the client.
  Future<void> expectAllClientsClosed() async {
    for (final closed in _clientClosed) {
      await closed.future.timeout(
        const Duration(seconds: 15),
        onTimeout: () => fail(
          'close() returned but the client socket is still open, so the '
          'server is still holding the session',
        ),
      );
    }
  }

  Future<void> shutdown() async {
    await _server.close();
    for (final client in _clientsStillOpen.toList()) {
      client.destroy();
    }
  }
}

void main() {
  withPostgresServer('close ends the session it reports having ended',
      (server) {
    late _WatchingProxy proxy;

    Future<Connection> openThroughProxy() async {
      final endpoint = await server.endpoint();
      return Connection.open(
        Endpoint(
          host: InternetAddress.loopbackIPv4.address,
          port: proxy.port,
          database: endpoint.database,
          username: endpoint.username,
          password: endpoint.password,
        ),
        settings: ConnectionSettings(sslMode: SslMode.disable),
      );
    }

    setUp(() async {
      proxy = await _WatchingProxy.start(await server.port);
    });

    tearDown(() async {
      await proxy.shutdown();
    });

    test('when the orderly shutdown completes', () async {
      final conn = await openThroughProxy();
      expect(await conn.execute('SELECT 1'), hasLength(1));

      await conn.close();

      expect(conn.isOpen, isFalse);
      await proxy.expectAllClientsClosed();
    });

    test('when the orderly shutdown cannot complete', () async {
      // The wedge stops the Terminate from being acknowledged and stops the
      // server ever closing its end, which is what a close on a loaded or
      // half-broken link looks like. `close()` is still required to return,
      // and to have ended the socket by the time it does.
      final conn = await openThroughProxy();
      expect(await conn.execute('SELECT 1'), hasLength(1));

      proxy.wedged = true;
      await conn.close().timeout(
            const Duration(seconds: 20),
            onTimeout: () => fail('close() never returned'),
          );

      expect(conn.isOpen, isFalse);
      await proxy.expectAllClientsClosed();
    });

    test('when a forced close lands on top of an orderly one', () async {
      // `close(force: true)` is the fallback callers reach for when the
      // polite close is not finishing. It used to see that a close was
      // already running and return having done nothing at all, which made it
      // useless in exactly the case it exists for.
      final conn = await openThroughProxy();
      expect(await conn.execute('SELECT 1'), hasLength(1));

      proxy.wedged = true;
      final polite = conn.close();
      final forced = conn.close(force: true);

      await Future.wait([polite, forced]).timeout(
        const Duration(seconds: 20),
        onTimeout: () => fail('neither close returned'),
      );

      expect(conn.isOpen, isFalse);
      await proxy.expectAllClientsClosed();
    });

    test('and a second close does not report success before the first is done',
        () async {
      final conn = await openThroughProxy();
      expect(await conn.execute('SELECT 1'), hasLength(1));

      var firstDone = false;
      final first = conn.close().then((_) => firstDone = true);
      await conn.close();
      expect(firstDone, isTrue,
          reason: 'the second close returned while the first was still '
              'running, so its caller cannot tell when the socket is gone');

      await first;
      await proxy.expectAllClientsClosed();
    });
  });
}
