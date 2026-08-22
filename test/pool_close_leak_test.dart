// A connection opened but not yet registered must still be closed.
//
// `_selectOrCreate` opens the socket and only then adds the connection to
// `_connections`, with an `await` in between. `close` works from a snapshot of
// that list, so a socket on its way in is invisible to it -- and a socket
// nobody can see is a socket nobody closes.
//
// The window is widened by `_connectLock`'s request timeout: a connect that
// outlives it is detached from any `withConnection`, so nothing releases a
// semaphore resource when it finally lands either. It arrives fully open,
// authenticated, owned by nobody, and sits idle on the server.
//
// Reproduced here with a proxy that holds the client's startup packet back for
// longer than the pool's connect timeout. The client gives up; the handshake
// completes anyway a moment later. That is what a loaded CI runner does to a
// two second connect timeout by accident.

import 'dart:async';
import 'dart:io';

import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'docker.dart';

/// Forwards to [targetPort], holding the first client->server write back by
/// [stall] so the client's connect outlives its own timeout.
class _StallingProxy {
  _StallingProxy({required this.targetPort, required this.stall});

  final int targetPort;
  final Duration stall;
  ServerSocket? _server;
  final _upstreams = <Socket>[];

  int get port => _server!.port;

  Future<void> start() async {
    _server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    _server!.listen((client) async {
      final upstream =
          await Socket.connect(InternetAddress.loopbackIPv4, targetPort);
      _upstreams.add(upstream);
      var stalled = false;
      client.listen((data) async {
        if (!stalled) {
          stalled = true;
          await Future<void>.delayed(stall);
        }
        try {
          upstream.add(data);
        } catch (_) {}
      }, onDone: () => upstream.destroy(), onError: (_) => upstream.destroy());
      upstream.listen((data) {
        try {
          client.add(data);
        } catch (_) {}
      }, onDone: () => client.destroy(), onError: (_) => client.destroy());
    });
  }

  Future<void> stop() async {
    await _server?.close();
    for (final s in _upstreams) {
      s.destroy();
    }
  }
}

void main() {
  withPostgresServer('pool close', (server) {
    test('a connect that outlives its timeout is still closed by close()',
        () async {
      final control = await server.newConnection();
      Future<int> backends() async {
        final r = await control.execute(
          "SELECT count(*)::int FROM pg_stat_activity "
          "WHERE datname = 'postgres' AND backend_type = 'client backend' "
          "AND pid <> pg_backend_pid()",
        );
        return r.first.first as int;
      }

      final baseline = await backends();

      final proxy = _StallingProxy(
        targetPort: await server.port,
        stall: const Duration(seconds: 2),
      );
      await proxy.start();
      addTearDown(proxy.stop);

      final endpoint = await server.endpoint();
      final pool = Pool.withEndpoints(
        [
          Endpoint(
            host: 'localhost',
            port: proxy.port,
            database: endpoint.database,
            username: endpoint.username,
            password: endpoint.password,
          )
        ],
        settings: const PoolSettings(
          maxConnectionCount: 2,
          sslMode: SslMode.disable,
          // Shorter than the stall, so the pool gives up on a connect that
          // then succeeds behind its back.
          connectTimeout: Duration(milliseconds: 500),
        ),
      );

      // Expected to fail: the point is what it leaves behind, not the error.
      await pool
          .withConnection((c) async => c.execute('SELECT 1'))
          .then<void>((_) {}, onError: (_) {});

      await pool.close(force: true);

      // Give the abandoned handshake time to land after close has returned.
      // Without the fix this is exactly when the orphan shows up.
      await Future<void>.delayed(const Duration(seconds: 4));

      expect(
        await backends(),
        baseline,
        reason: 'a socket opened while the pool was closing must not outlive '
            'the pool: close() cannot see it in _connections, so it has to be '
            'drained from the pending connects',
      );

      await control.close();
    }, timeout: const Timeout(Duration(minutes: 2)));
  });
}
