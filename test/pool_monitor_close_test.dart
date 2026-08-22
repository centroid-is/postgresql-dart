// Replicates how tfc-hmi's pool health monitor borrows, so the pool close can
// be measured against it at the server rather than at `pool.isOpen`.
//
// The monitor borrows a connection and never runs a statement on it -- it
// holds it purely to watch it die. That is why the connections that leak in CI
// are `state=idle` with an empty query: a pooled connection that was acquired
// and never used.

import 'dart:async';

import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'docker.dart';

void main() {
  withPostgresServer('pool close with a monitor-style borrow', (server) {
    late Connection control;

    setUpAll(() async {
      control = await server.newConnection();
    });
    tearDownAll(() => control.close());

    Future<int> backends() async {
      final r = await control.execute(
        "SELECT count(*)::int FROM pg_stat_activity "
        "WHERE datname = 'postgres' AND backend_type = 'client backend' "
        "AND pid <> pg_backend_pid()",
      );
      return r.first.first as int;
    }

    Future<int> waitForBackends(int want) async {
      final deadline = DateTime.now().add(const Duration(seconds: 10));
      var n = await backends();
      while (n != want && DateTime.now().isBefore(deadline)) {
        await Future<void>.delayed(const Duration(milliseconds: 100));
        n = await backends();
      }
      return n;
    }

    Future<Pool> newPool() async => Pool.withEndpoints(
      [await server.endpoint()],
      settings: const PoolSettings(
        maxConnectionCount: 2,
        sslMode: SslMode.disable,
        connectTimeout: Duration(seconds: 2),
        queryTimeout: Duration(seconds: 5),
      ),
    );

    test('a borrow released just before close leaves nothing behind', () async {
      final baseline = await backends();
      final pool = await newPool();

      // The monitor: borrow, hold without querying, let go when asked.
      final letGo = Completer<void>();
      final monitor = pool.withConnection((conn) async {
        await letGo.future;
      });

      // Something else does the actual work, so both slots get used -- this is
      // the shape tfc has, one connection for the monitor and one for drift.
      await pool.execute('SELECT 1');

      letGo.complete();
      await monitor;
      await pool.close();

      expect(await waitForBackends(baseline), baseline,
          reason: 'every connection the pool opened must be closed with it');
    }, timeout: const Timeout(Duration(minutes: 2)));

    test('a graceful close with the borrow still out leaves nothing behind',
        () async {
      // The ordering tfc had before it started stopping the monitor first, and
      // the one `close(force: false)` documents as relying on the borrower
      // returning the connection afterwards.
      final baseline = await backends();
      final pool = await newPool();

      final letGo = Completer<void>();
      final monitor = pool.withConnection((conn) async {
        await letGo.future;
      });
      await pool.execute('SELECT 1');

      final closing = pool.close();
      await Future<void>.delayed(const Duration(milliseconds: 200));
      letGo.complete();
      await monitor;
      await closing;

      expect(await waitForBackends(baseline), baseline,
          reason: 'a connection still borrowed when close started must be '
              'closed when it is returned');
    }, timeout: const Timeout(Duration(minutes: 2)));

    test('five short-lived pools in a row leave nothing behind', () async {
      // The failing test's shape: create, use, close, five times over.
      final baseline = await backends();

      for (var i = 0; i < 5; i++) {
        final pool = await newPool();
        final letGo = Completer<void>();
        final monitor = pool.withConnection((conn) async {
          await letGo.future;
        });
        await pool.execute('SELECT 1');
        letGo.complete();
        await monitor;
        await pool.close();
      }

      expect(await waitForBackends(baseline), baseline,
          reason: 'the cost of a discarded pool must not scale with how many '
              'were discarded');
    }, timeout: const Timeout(Duration(minutes: 3)));
  });
}
