// Reproduces tfc-hmi's leaking scenario against a real Postgres, so it can be
// run under Linux where CI sees it and macOS does not.
//
// Faithful to the failing test: connections go through an in-process TCP proxy
// (as tfc's integration tests do), the pool carries a monitor-style borrow that
// never runs a statement, and five short-lived pools are created and closed in
// a row.
//
// PGHOST/PGPORT select the server. Prints the backend census at each step.

import 'dart:async';
import 'dart:io';

import 'package:postgres/postgres.dart';

// -- the same proxy shape tfc uses -------------------------------------------

class _Pair {
  _Pair(this.client, this.server);
  final Socket client;
  final Socket server;
  bool closed = false;

  void start(void Function() onClose) {
    client.done.catchError((_) => client);
    server.done.catchError((_) => server);
    client.listen((d) {
      try {
        server.add(d);
      } catch (_) {}
    }, onDone: () => _close(onClose), onError: (_) => _close(onClose));
    server.listen((d) {
      try {
        client.add(d);
      } catch (_) {}
    }, onDone: () => _close(onClose), onError: (_) => _close(onClose));
  }

  void _close(void Function() onClose) {
    if (closed) return;
    closed = true;
    try {
      client.destroy();
    } catch (_) {}
    try {
      server.destroy();
    } catch (_) {}
    onClose();
  }
}

class TcpProxy {
  TcpProxy({required this.targetHost, required this.targetPort});
  final String targetHost;
  final int targetPort;
  ServerSocket? _server;
  final _pairs = <_Pair>[];

  int get port => _server!.port;
  int get livePairs => _pairs.length;

  Future<void> start() async {
    _server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    _server!.listen((client) async {
      try {
        final upstream = await Socket.connect(targetHost, targetPort);
        final pair = _Pair(client, upstream);
        _pairs.add(pair);
        pair.start(() => _pairs.remove(pair));
      } catch (_) {
        client.destroy();
      }
    });
  }

  Future<void> stop() async {
    await _server?.close();
    for (final p in [..._pairs]) {
      p._close(() {});
    }
    _pairs.clear();
  }
}

// -- probe --------------------------------------------------------------------

late Connection control;

Future<int> backends() async {
  final r = await control.execute(
    "SELECT count(*)::int FROM pg_stat_activity "
    "WHERE datname = 'postgres' AND backend_type = 'client backend' "
    "AND pid <> pg_backend_pid()",
  );
  return r.first.first as int;
}

Future<String> census() async {
  final r = await control.execute(
    "SELECT coalesce(state,'?'), coalesce(client_port,-1), "
    "round(extract(epoch from (state_change-backend_start)))::int, "
    "coalesce(left(query,30),'-') "
    "FROM pg_stat_activity WHERE datname='postgres' "
    "AND backend_type='client backend' AND pid <> pg_backend_pid() "
    "ORDER BY backend_start",
  );
  if (r.isEmpty) return 'none';
  return r
      .map((x) => '[${x[0]} port=${x[1]} idleAfter=${x[2]}s q=${x[3]}]')
      .join(' ');
}

Future<void> main() async {
  final host = Platform.environment['PGHOST'] ?? 'localhost';
  final port = int.parse(Platform.environment['PGPORT'] ?? '5432');

  control = await Connection.open(
    Endpoint(
      host: host,
      port: port,
      database: 'postgres',
      username: 'postgres',
      password: 'postgres',
    ),
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );

  final proxy = TcpProxy(targetHost: host, targetPort: port);
  await proxy.start();
  stdout.writeln('proxy on ${proxy.port} -> $host:$port');

  final baseline = await backends();
  stdout.writeln('baseline=$baseline  ${await census()}');

  final endpoint = Endpoint(
    host: 'localhost',
    port: proxy.port,
    database: 'postgres',
    username: 'postgres',
    password: 'postgres',
  );

  for (var i = 0; i < 5; i++) {
    final pool = Pool.withEndpoints(
      [endpoint],
      settings: const PoolSettings(
        maxConnectionCount: 2,
        sslMode: SslMode.disable,
        connectTimeout: Duration(seconds: 2),
        queryTimeout: Duration(seconds: 5),
        keepAliveInterval: Duration(seconds: 5),
        keepAliveCount: 3,
      ),
    );

    // Monitor-style borrow: held, never queried.
    final letGo = Completer<void>();
    final monitor = pool.withConnection((conn) async {
      await letGo.future;
    });
    // Work, so the second slot is used too.
    await pool.execute('SELECT 1');

    letGo.complete();
    await monitor;
    await pool.close();
    stdout.writeln('after pool $i: backends=${await backends()} '
        'pairs=${proxy.livePairs}');
  }

  // Same 60s patience the failing test has.
  final deadline = DateTime.now().add(const Duration(seconds: 60));
  var n = await backends();
  while (n > baseline && DateTime.now().isBefore(deadline)) {
    await Future<void>.delayed(const Duration(milliseconds: 200));
    n = await backends();
  }

  stdout.writeln('FINAL backends=$n baseline=$baseline '
      'pairs=${proxy.livePairs}');
  stdout.writeln('census: ${await census()}');
  stdout.writeln(n > baseline ? 'LEAKED ${n - baseline}' : 'CLEAN');

  await proxy.stop();
  await control.close();
  exit(n > baseline ? 1 : 0);
}
