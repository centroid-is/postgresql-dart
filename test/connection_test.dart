// ignore_for_file: unawaited_futures

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:postgres/postgres.dart';
import 'package:postgres/src/v3/connection.dart';
import 'package:test/test.dart';

import 'docker.dart';

void main() {
  withPostgresServer('connection state', (server) {
    test('post-close failure', () async {
      final conn = await server.newConnection();
      final rs = await conn.execute('SELECT 1');
      expect(rs.first.first, 1);
      await conn.close();
      await expectLater(
        () => conn.execute('SELECT 1;'),
        throwsA(
          isA<Exception>().having(
            (e) => '$e',
            'text',
            contains(
              'Attempting to execute query, but connection is not open.',
            ),
          ),
        ),
      );
    });

    test(
      'Connecting with ReplicationMode.none uses Extended Query Protocol',
      () async {
        final conn = await server.newConnection(
          replicationMode: ReplicationMode.none,
        );

        // This would throw for ReplicationMode.logical or ReplicationMode.physical
        final result = await conn.execute('select 1');
        expect(result.affectedRows, equals(1));
      },
    );

    test('Connect with logical ReplicationMode.logical', () async {
      final conn = await server.newConnection(
        replicationMode: ReplicationMode.logical,
      );

      expect(
        await conn.execute('select 1', queryMode: QueryMode.simple),
        equals([
          [1],
        ]),
      );
    });

    test('IDENTIFY_SYSTEM returns system information', () async {
      final conn = await server.newConnection(
        replicationMode: ReplicationMode.logical,
      );

      // This query can only be executed in Streaming Replication Protocol
      // In addition, it can only be executed using Simple Query Protocol:
      // "In either physical replication or logical replication walsender mode,
      //  only the simple query protocol can be used."
      // source and more info:
      // https://www.postgresql.org/docs/current/protocol-replication.html
      final result = await conn.execute(
        'IDENTIFY_SYSTEM;',
        queryMode: QueryMode.simple,
      );

      expect(result.length, 1);
      expect(result.schema.columns.length, 4);
      expect(result.schema.columns[0].columnName, 'systemid');
      expect(result.schema.columns[1].columnName, 'timeline');
      expect(result.schema.columns[2].columnName, 'xlogpos');
      expect(result.schema.columns[3].columnName, 'dbname');
    });

    // TODO: add test for ReplicationMode.physical which requires tuning some
    //       settings in the pg_hba.conf
  });

  withPostgresServer('Connection lifecycle', initSqls: oldSchemaInit, (server) {
    late Connection conn;

    tearDown(() async {
      await conn.close();
    });

    test('Connect with md5 or scram-sha-256 auth required', () async {
      conn = await server.newConnection(sslMode: SslMode.disable);

      expect(await conn.execute('select 1'), hasLength(1));
    });

    test('SSL Connect with md5 or scram-sha-256 auth required', () async {
      conn = await server.newConnection(sslMode: SslMode.require);
      expect(await conn.execute('select 1'), hasLength(1));
    });

    test('Connect with no auth required', () async {
      conn = await Connection.open(
        Endpoint(
          host: 'localhost',
          database: 'dart_test',
          port: await server.port,
          username: 'darttrust',
        ),
        settings: ConnectionSettings(sslMode: SslMode.disable),
      );
      expect(await conn.execute('select 1'), hasLength(1));
    });

    test('Connect with no auth throws for non trusted users', () async {
      try {
        conn = await Connection.open(
          Endpoint(
            host: 'localhost',
            database: 'dart_test',
            port: await server.port,
            username: 'dart',
          ),
          settings: ConnectionSettings(sslMode: SslMode.disable),
        );
      } catch (e) {
        expect(e, isA<PgException>());
        expect(
          (e as PgException).message,
          contains('password authentication failed for user "'),
        );
      }
      conn = await server.newConnection();
    });

    test('SSL Connect with no auth required', () async {
      conn = await Connection.open(
        Endpoint(
          host: 'localhost',
          database: 'dart_test',
          port: await server.port,
          username: 'darttrust',
        ),
        settings: ConnectionSettings(sslMode: SslMode.require),
      );

      expect(await conn.execute('select 1'), hasLength(1));
    });
  });

  withPostgresServer('Successful queries over time', initSqls: oldSchemaInit, (
    server,
  ) {
    late Connection conn;

    setUp(() async {
      conn = await server.newConnection();
    });

    tearDown(() async {
      await conn.close();
    });

    test(
      'Issuing multiple queries and awaiting between each one successfully returns the right value',
      () async {
        expect(
          await conn.execute('select 1'),
          equals([
            [1],
          ]),
        );
        expect(
          await conn.execute('select 2'),
          equals([
            [2],
          ]),
        );
        expect(
          await conn.execute('select 3'),
          equals([
            [3],
          ]),
        );
        expect(
          await conn.execute('select 4'),
          equals([
            [4],
          ]),
        );
        expect(
          await conn.execute('select 5'),
          equals([
            [5],
          ]),
        );
      },
    );

    test(
      'Issuing multiple queries without awaiting are returned with appropriate values',
      () async {
        final futures = [
          conn.execute('select 1'),
          conn.execute('select 2'),
          conn.execute('select 3'),
          conn.execute('select 4'),
          conn.execute('select 5'),
        ];

        final results = await Future.wait(futures);

        expect(results, [
          [
            [1],
          ],
          [
            [2],
          ],
          [
            [3],
          ],
          [
            [4],
          ],
          [
            [5],
          ],
        ]);
      },
    );
  });

  withPostgresServer(
    'Unintended user-error situations',
    initSqls: oldSchemaInit,
    (server) {
      Connection? conn;

      tearDown(() async {
        await conn?.close();
      });

      test(
        'Invalid password reports error, conn is closed, disables conn',
        () async {
          try {
            await Connection.open(
              Endpoint(
                host: 'localhost',
                database: 'dart_test',
                port: await server.port,
                username: 'dart',
                password: 'notdart',
              ),
              settings: ConnectionSettings(sslMode: SslMode.disable),
            );
            expect(true, false);
          } on PgException catch (e) {
            expect(e.message, contains('password authentication failed'));
          }
        },
      );

      test(
        'SSL Invalid password reports error, conn is closed, disables conn',
        () async {
          try {
            await Connection.open(
              Endpoint(
                host: 'localhost',
                database: 'dart_test',
                port: await server.port,
                username: 'dart',
                password: 'notdart',
              ),
              settings: ConnectionSettings(sslMode: SslMode.require),
            );
            expect(true, false);
          } on PgException catch (e) {
            expect(e.message, contains('password authentication failed'));
          }
        },
      );

      test(
        'A query error maintains connectivity, allows future queries',
        () async {
          conn = await server.newConnection();

          await conn!.execute('CREATE TEMPORARY TABLE t (i int unique)');
          await conn!.execute('INSERT INTO t (i) VALUES (1)');
          try {
            await conn!.execute('INSERT INTO t (i) VALUES (1)');
            expect(true, false);
          } on PgException catch (e) {
            expect(e.message, contains('duplicate key value violates'));
          }

          await conn!.execute('INSERT INTO t (i) VALUES (2)');
        },
      );

      test(
        'A query error maintains connectivity, continues processing pending transactions',
        () async {
          conn = await server.newConnection();

          await conn!.execute('CREATE TEMPORARY TABLE t (i int unique)');
          await conn!.execute('INSERT INTO t (i) VALUES (1)');

          final orderEnsurer = [];

          // this will emit a query error
          conn!.execute('INSERT INTO t (i) VALUES ()').catchError((err) {
            orderEnsurer.add(1);
            // ignore
            return Result(rows: [], affectedRows: 0, schema: ResultSchema([]));
          });

          orderEnsurer.add(2);
          final res = await conn!.runTx((ctx) async {
            orderEnsurer.add(3);
            return await ctx.execute('SELECT i FROM t');
          });
          orderEnsurer.add(4);

          expect(res, [
            [1],
          ]);
          expect(orderEnsurer, [2, 1, 3, 4]);
        },
      );
    },
  );

  withPostgresServer('Network error situations', (server) {
    ServerSocket? serverSocket;
    Socket? socket;

    tearDown(() async {
      if (serverSocket != null) {
        await serverSocket!.close();
      }
      if (socket != null) {
        await socket!.close();
      }
    });

    test(
      'Socket fails to connect reports error, disables connection for future use',
      () async {
        final port = await selectFreePort();

        try {
          await Connection.open(
            Endpoint(host: 'localhost', database: 'dart_test', port: port),
            settings: ConnectionSettings(sslMode: SslMode.disable),
          );
          expect(true, false);
        } on SocketException {
          // ignore
        }
      },
    );

    test(
      'SSL Socket fails to connect reports error, disables connection for future use',
      () async {
        final port = await selectFreePort();

        try {
          await Connection.open(
            Endpoint(host: 'localhost', database: 'dart_test', port: port),
            settings: ConnectionSettings(sslMode: SslMode.require),
          );
          expect(true, false);
        } on SocketException {
          // ignore
        }
      },
    );

    test(
      'Connection that times out throws appropriate error and cannot be reused',
      () async {
        final port = await selectFreePort();
        serverSocket = await ServerSocket.bind(
          InternetAddress.loopbackIPv4,
          port,
        );
        serverSocket!.listen((s) {
          socket = s;
          // Don't respond on purpose
          s.listen((bytes) {});
        });

        try {
          await Connection.open(
            Endpoint(host: 'localhost', port: port, database: 'dart_test'),
            settings: ConnectionSettings(
              connectTimeout: Duration(seconds: 2),
              sslMode: SslMode.disable,
            ),
          );
          fail('unreachable');
        } on TimeoutException {
          // ignore
        }
      },
    );

    test(
      'SSL Connection that times out throws appropriate error and cannot be reused',
      () async {
        final port = await selectFreePort();
        serverSocket = await ServerSocket.bind(
          InternetAddress.loopbackIPv4,
          port,
        );
        serverSocket!.listen((s) {
          socket = s;
          // Don't respond on purpose
          s.listen((bytes) {});
        });

        try {
          await Connection.open(
            Endpoint(host: 'localhost', port: port, database: 'dart_test'),
            settings: ConnectionSettings(
              connectTimeout: Duration(seconds: 2),
              sslMode: SslMode.require,
            ),
          );
          fail('unreachable');
        } on TimeoutException {
          // ignore
        }
      },
    );
  });

  withPostgresServer('connection', (server) {
    test('If connection is closed, do not allow .execute', () async {
      final conn = await server.newConnection();
      await conn.close();
      try {
        await conn.execute('SELECT 1');
        fail('unreachable');
      } on PgException catch (e) {
        expect(e.toString(), contains('connection is not open'));
      }
    });
  });

  // Cross-platform test: verifies that the platform-specific socket option
  // constants (SO_KEEPALIVE, TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT) are
  // accepted by the OS and the values stick. No Docker or PG needed.
  group('TCP keep-alive socket options', () {
    test('sets and reads back keepalive options on all platforms', () async {
      final serverSocket = await ServerSocket.bind('localhost', 0);
      final rawSocket = await RawSocket.connect('localhost', serverSocket.port);

      try {
        final isMac = Platform.isMacOS || Platform.isIOS;
        final isWindows = Platform.isWindows;
        final isLinux = Platform.isLinux || Platform.isAndroid;

        // SO_KEEPALIVE
        final soKeepAlive = isLinux ? 0x0009 : 0x0008;
        rawSocket.setRawOption(
          RawSocketOption.fromBool(
              RawSocketOption.levelSocket, soKeepAlive, true),
        );
        final kaVal = rawSocket.getRawOption(
          RawSocketOption(RawSocketOption.levelSocket, soKeepAlive, Uint8List(4)),
        );
        expect(
          ByteData.sublistView(kaVal).getInt32(0, Endian.host),
          isNonZero,
          reason: 'SO_KEEPALIVE should be enabled',
        );

        // TCP_KEEPIDLE: Linux=4, macOS=0x10, Windows=3
        final idleOpt = isMac ? 0x10 : isWindows ? 3 : 4;
        rawSocket.setRawOption(
          RawSocketOption.fromInt(RawSocketOption.levelTcp, idleOpt, 42),
        );
        final idleVal = rawSocket.getRawOption(
          RawSocketOption(RawSocketOption.levelTcp, idleOpt, Uint8List(4)),
        );
        expect(
          ByteData.sublistView(idleVal).getInt32(0, Endian.host),
          42,
          reason: 'TCP_KEEPIDLE should be 42',
        );

        // TCP_KEEPINTVL: Linux=5, macOS=0x101, Windows=17
        final intvlOpt = isMac ? 0x101 : isWindows ? 17 : 5;
        rawSocket.setRawOption(
          RawSocketOption.fromInt(RawSocketOption.levelTcp, intvlOpt, 42),
        );
        final intvlVal = rawSocket.getRawOption(
          RawSocketOption(RawSocketOption.levelTcp, intvlOpt, Uint8List(4)),
        );
        expect(
          ByteData.sublistView(intvlVal).getInt32(0, Endian.host),
          42,
          reason: 'TCP_KEEPINTVL should be 42',
        );

        // TCP_KEEPCNT: Linux=6, macOS=0x102, Windows=16
        final cntOpt = isMac ? 0x102 : isWindows ? 16 : 6;
        rawSocket.setRawOption(
          RawSocketOption.fromInt(RawSocketOption.levelTcp, cntOpt, 7),
        );
        final cntVal = rawSocket.getRawOption(
          RawSocketOption(RawSocketOption.levelTcp, cntOpt, Uint8List(4)),
        );
        expect(
          ByteData.sublistView(cntVal).getInt32(0, Endian.host),
          7,
          reason: 'TCP_KEEPCNT should be 7',
        );
      } finally {
        rawSocket.close();
        await serverSocket.close();
      }
    });
  });

  // E2E test requires Linux — on macOS Docker Desktop, a userspace TCP proxy
  // sits between host and container, so keepalive probes are ACK'd locally
  // and never reach the container. On Linux, Docker uses kernel-level NAT
  // and probes reach the container directly.
  withPostgresServer(
    'TCP keep-alive',
    (server) {
      setUpAll(() async {
        // The default postgres image doesn't ship iptables.
        await server.exec(['bash', '-c', 'apt-get update -qq && apt-get install -y -qq iptables']);
      });

      test(
        'detects unresponsive peer via keepalive probes',
        () async {
          final conn = await PgConnectionImplementation.connect(
            await server.endpoint(),
            connectionSettings: ConnectionSettings(
              connectTimeout: Duration(seconds: 5),
              queryTimeout: Duration(seconds: 5),
              sslMode: SslMode.disable,
              // Aggressive keepalive: 2s idle, 2s interval, 3 probes → ~8s.
              keepAliveInterval: Duration(seconds: 2),
              keepAliveCount: 3,
            ),
          );

          // Verify connection works before black-holing traffic.
          final rs = await conn.execute('SELECT 1');
          expect(rs.first.first, 1);

          // Use iptables to silently DROP all TCP traffic on port 5432.
          // Unlike docker pause (kernel still ACKs) or docker network
          // disconnect (immediate ICMP error), DROP makes keepalive probes
          // go unanswered — the only way to trigger a real keepalive timeout.
          await server.exec(
            ['iptables', '-A', 'INPUT', '-p', 'tcp', '--dport', '5432', '-j', 'DROP'],
          );
          await server.exec(
            ['iptables', '-A', 'OUTPUT', '-p', 'tcp', '--sport', '5432', '-j', 'DROP'],
          );

          try {
            // The connection should be detected as dead purely by keepalive
            // probes — no query needed. idle(2s) + interval(2s) * count(3) = 8s.
            final sw = Stopwatch()..start();
            await expectLater(
              conn.closed.timeout(Duration(seconds: 30)),
              completes,
            );
            sw.stop();

            // Verify keepalive actually fired (not an immediate error).
            expect(sw.elapsed.inSeconds, greaterThanOrEqualTo(4));
            expect(conn.isOpen, isFalse);
          } finally {
            // Flush iptables rules so the container is usable for teardown.
            await server.exec(['iptables', '-F']);
          }
        },
        skip: !Platform.isLinux ? 'Keepalive test requires Linux (macOS Docker Desktop uses a userspace TCP proxy)' : null,
      );
    },
    dockerArgs: ['--cap-add=NET_ADMIN'],
  );
}
