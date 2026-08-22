// What a failed `connect` leaves behind, measured at the other end of the
// socket.
//
// `Socket.connect` succeeding is only the first step of opening a connection,
// and every step after it can fail: the SSL negotiation can time out, the
// server can turn out not to speak SSL, the certificate can be rejected, the
// startup handshake can time out. Until `connect` returns, nothing outside it
// holds a reference to the socket -- so a step that throws without closing it
// first strands it for the life of the process.
//
// That is not merely a file descriptor. By the time the startup handshake is
// under way the server has already forked a backend, and it is a `client
// backend` sitting `idle`, having run no query, holding one of
// `max_connections` until the server works out on its own that nobody is
// coming back. A client that retries -- a pool, a health check, a reconnect
// loop -- loses one slot per attempt.
//
// These tests use a fake server rather than a real Postgres because the
// interesting question is only ever "did the client close its socket", and a
// server that says nothing at all provokes that far more reliably than a real
// one under load.

import 'dart:async';
import 'dart:io';

import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

/// The byte a server sends to refuse an SSLRequest.
const _sslDeclined = 0x4e; // 'N'

/// A server that accepts connections and then behaves badly on purpose.
class _RudeServer {
  _RudeServer._(this._socket);

  final ServerSocket _socket;

  /// Every connection the server has accepted, in order, each paired with a
  /// future that completes once the client closes its end.
  final _sessions = <_Session>[];

  /// When set, the reply to send to the first thing the client says. Used to
  /// decline the SSLRequest; left null to say nothing at all.
  int? replyByte;

  int get port => _socket.port;

  List<_Session> get sessions => List.unmodifiable(_sessions);

  static Future<_RudeServer> start() async {
    final socket =
        await ServerSocket.bind(InternetAddress.loopbackIPv4, 0, shared: false);
    final server = _RudeServer._(socket);
    socket.listen(server._accept);
    return server;
  }

  void _accept(Socket socket) {
    final session = _Session(socket);
    _sessions.add(session);
    socket.listen(
      (_) {
        final reply = replyByte;
        if (reply != null && !session._replied) {
          session._replied = true;
          socket.add([reply]);
        }
      },
      onDone: session._finish,
      onError: (_) => session._finish(),
      cancelOnError: true,
    );
  }

  /// Waits until the server has accepted [count] connections.
  Future<void> awaitSessions(int count) async {
    final deadline = DateTime.now().add(const Duration(seconds: 10));
    while (_sessions.length < count) {
      if (DateTime.now().isAfter(deadline)) {
        fail('server saw ${_sessions.length} connection(s), expected $count');
      }
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }

  Future<void> shutdown() async {
    await _socket.close();
    for (final session in _sessions) {
      session.socket.destroy();
    }
  }
}

class _Session {
  _Session(this.socket);

  final Socket socket;
  final _closedByClient = Completer<void>();
  bool _replied = false;

  void _finish() {
    if (!_closedByClient.isCompleted) _closedByClient.complete();
  }

  /// Fails unless the client closes its end of this connection.
  Future<void> expectClosedByClient() async {
    await _closedByClient.future.timeout(
      const Duration(seconds: 10),
      onTimeout: () => fail(
        'the client gave up on this connection but never closed the socket, '
        'so the server is still holding the session open',
      ),
    );
  }
}

Endpoint _endpointFor(_RudeServer server) => Endpoint(
      host: InternetAddress.loopbackIPv4.address,
      port: server.port,
      database: 'testdb',
      username: 'testuser',
      password: 'testpass',
    );

void main() {
  late _RudeServer server;

  setUp(() async {
    server = await _RudeServer.start();
  });

  tearDown(() async {
    await server.shutdown();
  });

  group('a connect that fails closes its socket', () {
    test('when the startup handshake times out', () async {
      // The likeliest way in on a busy machine, and the one that leaves the
      // most expensive debris: the server has finished its side of the
      // handshake and is waiting on a client that did not read the reply in
      // time. Nothing here replies at all, which reaches the same code path
      // without having to lose a race on purpose.
      await expectLater(
        Connection.open(
          _endpointFor(server),
          settings: ConnectionSettings(
            sslMode: SslMode.disable,
            connectTimeout: const Duration(milliseconds: 300),
          ),
        ),
        throwsA(isA<TimeoutException>()),
      );

      await server.awaitSessions(1);
      await server.sessions.single.expectClosedByClient();
    });

    test('when the server declines SSL that was required', () async {
      server.replyByte = _sslDeclined;

      await expectLater(
        Connection.open(
          _endpointFor(server),
          settings: ConnectionSettings(
            sslMode: SslMode.require,
            connectTimeout: const Duration(milliseconds: 300),
          ),
        ),
        throwsA(isA<PgException>()),
      );

      await server.awaitSessions(1);
      await server.sessions.single.expectClosedByClient();
    });

    test('when the SSL negotiation times out', () async {
      await expectLater(
        Connection.open(
          _endpointFor(server),
          settings: ConnectionSettings(
            sslMode: SslMode.require,
            connectTimeout: const Duration(milliseconds: 300),
          ),
        ),
        throwsA(isA<TimeoutException>()),
      );

      await server.awaitSessions(1);
      await server.sessions.single.expectClosedByClient();
    });
  });

  group('a pool whose connects fail', () {
    test('leaves nothing open, and the cost does not grow with the retries',
        () async {
      // The shape of the production bug. A pool acquire fails after its socket
      // is up, so the connection is never entered into the pool's own
      // bookkeeping -- and `close`, even forced, can only reach what the pool
      // recorded. Every retry cost a connection slot, and closing the pool did
      // not give any of them back.
      final pool = Pool.withEndpoints(
        [_endpointFor(server)],
        settings: PoolSettings(
          maxConnectionCount: 2,
          connectTimeout: const Duration(milliseconds: 300),
          sslMode: SslMode.disable,
        ),
      );

      const attempts = 5;
      for (var i = 0; i < attempts; i++) {
        await expectLater(pool.execute('SELECT 1'), throwsA(isA<Object>()));
      }

      await pool.close(force: true);

      await server.awaitSessions(attempts);
      expect(server.sessions, hasLength(attempts));
      for (final session in server.sessions) {
        await session.expectClosedByClient();
      }
    });
  });
}
