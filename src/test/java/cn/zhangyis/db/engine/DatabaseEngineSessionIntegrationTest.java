package cn.zhangyis.db.engine;

import cn.zhangyis.db.dd.ddl.*;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.exception.MetadataLockTimeoutException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.XaId;
import cn.zhangyis.db.engine.xa.XaRecoveryMaintenance;
import cn.zhangyis.db.session.SessionOptions;
import cn.zhangyis.db.session.SessionState;
import cn.zhangyis.db.sql.executor.QueryResult;
import cn.zhangyis.db.sql.executor.UpdateResult;
import cn.zhangyis.db.sql.executor.CommandResult;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.executor.storage.SqlDurabilityMode;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.OnlineDdlConfig;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase;
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexBuildDuplicateKeyException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** DatabaseEngine registry/close 顺序到真实 INSERT+LOB rollback/restart 的集成验收。 */
class DatabaseEngineSessionIntegrationTest {
    @TempDir Path directory;

    /** engine close 先回滚活动 Session 并释放 metadata/row/LOB ownership，重开后未提交行不可见。 */
    @Test
    void closeRollsBackActiveLobSessionBeforeStorageShutdown() {
        EngineConfig config = config();
        DatabaseEngine database = new DatabaseEngine(config);
        database.open();
        createTable(database);
        var session = database.openSession(options(false));
        session.execute("INSERT INTO docs (id, email, body) VALUES (1, 'pending@example.test', '"
                + "长".repeat(300) + "')");
        assertTrue(session.snapshot().transactionActive());
        database.close();
        assertEquals(SessionState.CLOSED, session.snapshot().state());

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var reader = reopened.openSession(options(true))) {
                QueryResult result = assertInstanceOf(QueryResult.class,
                        reader.execute("SELECT * FROM docs WHERE email='PENDING@example.test'"));
                assertTrue(result.rows().isEmpty());
            }
        }
    }

    /** 公开 Session INSERT 必须维护 unique secondary；SELECT 由 Binder 选该索引并回表返回完整 LOB 投影。 */
    @Test
    void sessionSelectsCommittedRowThroughUniqueSecondaryAndHydratesLob() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            createTable(database);
            String body = "回表".repeat(220);
            try (var session = database.openSession(options(true))) {
                session.execute("INSERT INTO docs (id, email, body) VALUES (7, 'Reader@example.test', '"
                        + body + "')");
                QueryResult result = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT body, id FROM docs WHERE email='READER@example.test'"));
                assertEquals(1, result.rows().size());
                assertEquals(body, ((cn.zhangyis.db.sql.executor.SqlValue.StringValue)
                        result.rows().getFirst().values().get(0)).value());
                assertEquals(java.math.BigInteger.valueOf(7),
                        ((cn.zhangyis.db.sql.executor.SqlValue.IntegerValue)
                                result.rows().getFirst().values().get(1)).value());
            }
        }
    }

    /**
     * SQL XA 写分支必须经过 durable PREPARED、可被其它 Session RECOVER/COMMIT，并在完成后从 registry 消失。
     */
    @Test
    void sqlXaPrepareRecoverAndCrossSessionCommit() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            createTable(database);
            try (var owner = database.openSession(options(true));
                 var coordinator = database.openSession(options(true))) {
                assertInstanceOf(CommandResult.class,
                        owner.execute("XA START 'global-91', 'docs', 17"));
                owner.execute("INSERT INTO docs (id, email, body) VALUES "
                        + "(91, 'xa91@example.test', 'prepared body')");
                owner.execute("XA END 'global-91', 'docs', 17");
                CommandResult prepared = assertInstanceOf(CommandResult.class,
                        owner.execute("XA PREPARE 'global-91', 'docs', 17"));
                assertFalse(prepared.transactionStatus().transactionActive());

                QueryResult recover = assertInstanceOf(QueryResult.class,
                        coordinator.execute("XA RECOVER CONVERT XID"));
                assertEquals(1, recover.rows().size());
                coordinator.execute("XA COMMIT 'global-91', 'docs', 17");
                assertTrue(assertInstanceOf(QueryResult.class,
                        coordinator.execute("XA RECOVER")).rows().isEmpty());

                QueryResult row = assertInstanceOf(QueryResult.class,
                        coordinator.execute("SELECT id FROM docs WHERE id=91"));
                assertEquals(1, row.rows().size());
            }
        }
    }

    /**
     * XA 只读 prepare 使用 READ_ONLY 优化，不创建 registry PREPARED；ONE PHASE 也直接终结活动分支。
     */
    @Test
    void sqlXaReadOnlyAndOnePhaseDoNotLeavePreparedEntries() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            createTable(database);
            try (var session = database.openSession(options(true))) {
                session.execute("XA START 'read-only'");
                session.execute("SELECT * FROM docs WHERE id=404");
                session.execute("XA END 'read-only'");
                CommandResult readOnly = assertInstanceOf(CommandResult.class,
                        session.execute("XA PREPARE 'read-only'"));
                assertFalse(readOnly.transactionStatus().transactionActive());
                assertTrue(assertInstanceOf(QueryResult.class,
                        session.execute("XA RECOVER")).rows().isEmpty());

                session.execute("XA START 'one-phase'");
                session.execute("INSERT INTO docs (id, email, body) VALUES "
                        + "(92, 'xa92@example.test', 'one phase')");
                session.execute("XA END 'one-phase'");
                session.execute("XA COMMIT 'one-phase' ONE PHASE");
                assertTrue(assertInstanceOf(QueryResult.class,
                        session.execute("SELECT id FROM docs WHERE id=92")).rows().size() == 1);
            }
        }
    }

    /**
     * 未决 PREPARED 必须阻止普通 OPEN；离线工具只写 registry 决议，下一次启动由 storage recovery
     * 完成 prepared commit 并在成功后写 COMPLETED。
     */
    @Test
    void unresolvedPreparedBlocksOpenUntilOfflineDecision() {
        EngineConfig config = config();
        DatabaseEngine first = new DatabaseEngine(config);
        first.open();
        createTable(first);
        try (var session = first.openSession(options(true))) {
            session.execute("XA START 'restart-xa', 'docs', 23");
            session.execute("INSERT INTO docs (id, email, body) VALUES "
                    + "(93, 'xa93@example.test', 'restart prepared')");
            session.execute("XA END 'restart-xa', 'docs', 23");
            session.execute("XA PREPARE 'restart-xa', 'docs', 23");
        }
        first.close();

        DatabaseEngine blocked = new DatabaseEngine(config);
        assertThrows(cn.zhangyis.db.common.exception.DatabaseRuntimeException.class, blocked::open);
        assertEquals(DatabaseEngineState.FAILED, blocked.state());

        XaId xid = new XaId(23, "restart-xa".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "docs".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        new XaRecoveryMaintenance().decide(
                directory, Duration.ofSeconds(2), xid, true);

        try (DatabaseEngine recovered = new DatabaseEngine(config)) {
            recovered.open();
            try (var reader = recovered.openSession(options(true))) {
                QueryResult row = assertInstanceOf(QueryResult.class,
                        reader.execute("SELECT id FROM docs WHERE id=93"));
                assertEquals(1, row.rows().size());
                assertTrue(assertInstanceOf(QueryResult.class,
                        reader.execute("XA RECOVER")).rows().isEmpty());
            }
        }
    }

    /**
     * SQL CREATE INDEX 与 ALTER TABLE ADD INDEX 必须共享真实回填链；autocommit=0 在 DDL 前隐式提交，
     * 完成后重新建立 implicit transaction，重启后新索引仍可访问既有行。
     */
    @Test
    void sqlCreatesAndAltersSecondaryIndexesWithImplicitCommit() {
        EngineConfig config = config();
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var session = database.openSession(options(false))) {
                session.execute("INSERT INTO index_docs (id, category) VALUES (1, 7)");
                session.execute("INSERT INTO index_docs (id, category) VALUES (2, 7)");

                assertInstanceOf(CommandResult.class,
                        session.execute("CREATE INDEX idx_category ON index_docs (category DESC)"));
                assertTrue(session.snapshot().transactionActive(),
                        "autocommit=0 的 DDL 结束后必须恢复新的 implicit transaction");
                QueryResult byCategory = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT id FROM index_docs WHERE category=7"));
                assertEquals(2, byCategory.rows().size(),
                        "DDL 前 implicit commit 的两行必须被 backfill 并对当前 Session 可见");

                assertInstanceOf(CommandResult.class, session.execute(
                        "ALTER TABLE index_docs ADD UNIQUE INDEX uk_id_desc (id DESC)"));
            }
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(902), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                assertEquals(3, lease.table().indexes().size());
                assertEquals("uk_id_desc", lease.table().indexes().getLast().name().canonicalName());
            }
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var reader = reopened.openSession(options(true))) {
                QueryResult result = assertInstanceOf(QueryResult.class,
                        reader.execute("SELECT id FROM index_docs WHERE category=7"));
                assertEquals(2, result.rows().size());
            }
        }
    }

    /** base scan 持 SU 时真实 SQL INSERT/UPDATE/DELETE 必须提交，并在 final reconciliation 后进入新索引。 */
    @Test
    void onlineCreateIndexCapturesConcurrentSqlDml() throws Exception {
        EngineConfig config = config().withOnlineDdlConfig(new OnlineDdlConfig(
                128L * 1024 * 1024, 1, 4096));
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var loader = database.openSession(options(false))) {
                for (int id = 1; id <= 100; id++) {
                    loader.execute("INSERT INTO index_docs (id, category) VALUES ("
                            + id + ", 7)");
                }

                long tableId;
                try (var lease = database.dictionary().openTable(
                        MdlOwnerId.of(920), QualifiedTableName.of("app", "index_docs"),
                        cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                    tableId = lease.table().id().value();
                }

                try (var executor = Executors.newSingleThreadExecutor()) {
                    var build = executor.submit(() -> loader.execute(
                            "CREATE INDEX idx_category_online ON index_docs (category)"));
                    await(() -> database.storage().onlineDdlTableGate().phase(tableId)
                            == OnlineDdlTablePhase.CAPTURING, Duration.ofSeconds(5));
                    OnlineDdlOperationSnapshot running = database.onlineDdlControl()
                            .list().getFirst();
                    assertTrue(running.cancelCapable());
                    assertEquals(DdlLogPhase.PREPARED,
                            running.durablePhase().orElseThrow());
                    assertEquals(OnlineDdlTerminalResult.NONE, running.terminalResult());

                    try (var writer = database.openSession(options(true))) {
                        writer.execute("INSERT INTO index_docs (id, category) VALUES (1000, 9)");
                        writer.execute("UPDATE index_docs SET category=8 WHERE id=1");
                        writer.execute("DELETE FROM index_docs WHERE id=2");
                    }
                    assertInstanceOf(CommandResult.class, build.get(10, TimeUnit.SECONDS));
                }

                assertEquals(OnlineDdlTablePhase.ABSENT,
                        database.storage().onlineDdlTableGate().phase(tableId));
                OnlineDdlOperationSnapshot completed = database.onlineDdlControl()
                        .list().getFirst();
                assertEquals(OnlineDdlTerminalResult.COMPLETED, completed.terminalResult());
                assertEquals(DdlLogPhase.COMMITTED,
                        completed.durablePhase().orElseThrow());
                assertTrue(completed.batchesScanned() > 0);
                try (var reader = database.openSession(options(true))) {
                    QueryResult inserted = assertInstanceOf(QueryResult.class,
                            reader.execute("SELECT id FROM index_docs WHERE category=9"));
                    assertEquals(BigInteger.valueOf(1000), ((SqlValue.IntegerValue)
                            inserted.rows().getFirst().values().getFirst()).value());
                    QueryResult updated = assertInstanceOf(QueryResult.class,
                            reader.execute("SELECT id FROM index_docs WHERE category=8"));
                    assertEquals(BigInteger.ONE, ((SqlValue.IntegerValue)
                            updated.rows().getFirst().values().getFirst()).value());
                    assertTrue(assertInstanceOf(QueryResult.class,
                            reader.execute("SELECT id FROM index_docs WHERE id=2")).rows().isEmpty());
                }
                if (Files.exists(config.onlineDdlDirectory())) {
                    try (var files = Files.list(config.onlineDdlDirectory())) {
                        assertTrue(files.noneMatch(path ->
                                path.getFileName().toString().endsWith(".log")));
                    }
                }
            }
        }
    }

    /** final X upgrade 超时必须只回滚 Online DDL；持有 SW 的用户事务随后仍可提交，且 gate/footer/log 均收敛。 */
    @Test
    void onlineCreateIndexCleansUpAfterFinalMetadataLockTimeout() throws Exception {
        EngineConfig config = config().withOnlineDdlConfig(new OnlineDdlConfig(
                128L * 1024 * 1024, 1, 4096));
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var seed = database.openSession(options(true))) {
                for (int id = 1; id <= 100; id++) {
                    seed.execute("INSERT INTO index_docs (id, category) VALUES ("
                            + id + ", " + id + ")");
                }
            }
            long tableId;
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(921), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                tableId = lease.table().id().value();
            }

            try (var ddlSession = database.openSession(options(true));
                 var writer = database.openSession(options(false));
                 var executor = Executors.newSingleThreadExecutor()) {
                var build = executor.submit(() -> ddlSession.execute(
                        "CREATE INDEX idx_timeout ON index_docs (category)"));
                await(() -> database.storage().onlineDdlTableGate().phase(tableId)
                        == OnlineDdlTablePhase.CAPTURING, Duration.ofSeconds(5));

                writer.execute("INSERT INTO index_docs (id, category) VALUES (1000, 9)");
                var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> build.get(8, TimeUnit.SECONDS));
                assertInstanceOf(MetadataLockTimeoutException.class, failure.getCause());
                assertEquals(OnlineDdlTablePhase.ABSENT,
                        database.storage().onlineDdlTableGate().phase(tableId));

                writer.execute("COMMIT");
                try (var lease = database.dictionary().openTable(
                        MdlOwnerId.of(922), QualifiedTableName.of("app", "index_docs"),
                        cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                    assertEquals(1, lease.table().indexes().size());
                }
                assertEquals(BigInteger.valueOf(1000), ((SqlValue.IntegerValue)
                        assertInstanceOf(QueryResult.class, ddlSession.execute(
                                "SELECT id FROM index_docs WHERE id=1000"))
                                .rows().getFirst().values().getFirst()).value());
            }
            if (Files.exists(config.onlineDdlDirectory())) {
                try (var files = Files.list(config.onlineDdlDirectory())) {
                    assertTrue(files.noneMatch(path -> path.getFileName().toString().endsWith(".log")));
                }
            }
        }
    }

    /**
     * Online DDL 阻塞在 final-X upgrade 时，admin cancel 先持久 marker 再精确移除 pending request；
     * 已授予的 DDL SU 和用户 SW 都由原 owner 继续持有，直到各自 RAII/事务路径释放。
     */
    @Test
    void cancelsOnlineCreateIndexWhileFinalMetadataLockIsPending() throws Exception {
        EngineConfig config = config().withOnlineDdlConfig(new OnlineDdlConfig(
                128L * 1024 * 1024, 1, 4096));
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var seed = database.openSession(options(true))) {
                for (int id = 1; id <= 80; id++) {
                    seed.execute("INSERT INTO index_docs (id, category) VALUES ("
                            + id + ", " + id + ")");
                }
            }
            long tableId;
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(923), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                tableId = lease.table().id().value();
            }

            try (var ddlSession = database.openSession(options(true));
                 var writer = database.openSession(options(false));
                 var executor = Executors.newSingleThreadExecutor()) {
                var build = executor.submit(() -> ddlSession.execute(
                        "CREATE INDEX idx_cancel ON index_docs (category)"));
                await(() -> database.storage().onlineDdlTableGate().phase(tableId)
                                == OnlineDdlTablePhase.CAPTURING,
                        Duration.ofSeconds(5));
                writer.execute("INSERT INTO index_docs (id, category) VALUES (1001, 9)");
                await(() -> database.onlineDdlControl().list().stream().anyMatch(snapshot ->
                                snapshot.runtimePhase() == OnlineDdlRuntimePhase.WAITING_FINAL_MDL),
                        Duration.ofSeconds(5));
                OnlineDdlOperationSnapshot pending = database.onlineDdlControl().list().stream()
                        .filter(snapshot -> snapshot.runtimePhase()
                                == OnlineDdlRuntimePhase.WAITING_FINAL_MDL)
                        .findFirst().orElseThrow();

                OnlineDdlCancelResult cancel = database.onlineDdlControl().requestCancel(
                        pending.identity().ddlId(), OnlineDdlCancelRequest.admin(
                                DdlCancellationReason.USER_REQUEST, 1), Duration.ofSeconds(2));

                assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE, cancel.outcome());
                var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> build.get(5, TimeUnit.SECONDS));
                assertInstanceOf(OnlineDdlCancellationException.class, failure.getCause());
                assertTrue(database.storage().onlineDdlTableGate().phase(tableId)
                        == OnlineDdlTablePhase.ABSENT);
                assertTrue(database.onlineDdlControl().find(pending.identity().ddlId())
                        .orElseThrow().terminalResult() == OnlineDdlTerminalResult.ROLLED_BACK);
                assertTrue(database.onlineDdlControl().find(pending.identity().ddlId())
                        .orElseThrow().cancellationReason().isPresent());

                // cancel 没有强拆用户 SW；原事务仍可正常提交并保留业务行。
                writer.execute("COMMIT");
                assertEquals(BigInteger.valueOf(1001), ((SqlValue.IntegerValue)
                        assertInstanceOf(QueryResult.class, ddlSession.execute(
                                "SELECT id FROM index_docs WHERE id=1001"))
                                .rows().getFirst().values().getFirst()).value());
            }
        }
    }

    /**
     * 通用shadow ALTER阻塞在final-X时，控制面必须按capture id唤醒table gate；取消完成后旧表仍是
     * 唯一可见版本，持有SW的用户事务不被强拆且可以正常提交。
     */
    @Test
    void cancelsGeneralShadowAlterWhileFinalMetadataLockIsPending() throws Exception {
        EngineConfig config = config().withOnlineDdlConfig(new OnlineDdlConfig(
                128L * 1024 * 1024, 1, 4096));
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var seed = database.openSession(options(true))) {
                for (int id = 1; id <= 80; id++) {
                    seed.execute("INSERT INTO index_docs (id, category) VALUES ("
                            + id + ", " + id + ")");
                }
            }
            long tableId;
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(924), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                tableId = lease.table().id().value();
            }

            try (var ddlSession = database.openSession(options(true));
                 var writer = database.openSession(options(false));
                 var executor = Executors.newSingleThreadExecutor()) {
                var alter = executor.submit(() -> ddlSession.execute(
                        "ALTER TABLE index_docs ADD COLUMN status VARCHAR(16) DEFAULT 'new'"));
                await(() -> database.storage().onlineDdlTableGate().phase(tableId)
                                == OnlineDdlTablePhase.CAPTURING,
                        Duration.ofSeconds(5));
                writer.execute("INSERT INTO index_docs (id, category) VALUES (1002, 9)");
                await(() -> database.onlineDdlControl().list().stream().anyMatch(snapshot ->
                                snapshot.identity().operation() == DdlLogOperation.REBUILD_TABLE
                                        && snapshot.runtimePhase()
                                        == OnlineDdlRuntimePhase.WAITING_FINAL_MDL),
                        Duration.ofSeconds(5));
                OnlineDdlOperationSnapshot pending = database.onlineDdlControl().list().stream()
                        .filter(snapshot -> snapshot.identity().operation()
                                == DdlLogOperation.REBUILD_TABLE
                                && snapshot.runtimePhase()
                                == OnlineDdlRuntimePhase.WAITING_FINAL_MDL)
                        .findFirst().orElseThrow();

                OnlineDdlCancelResult cancel = database.onlineDdlControl().requestCancel(
                        pending.identity().ddlId(), OnlineDdlCancelRequest.admin(
                                DdlCancellationReason.USER_REQUEST, 1), Duration.ofSeconds(2));

                assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE, cancel.outcome());
                var failure = assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> alter.get(5, TimeUnit.SECONDS));
                assertInstanceOf(OnlineDdlCancellationException.class, failure.getCause());
                assertEquals(OnlineDdlTablePhase.ABSENT,
                        database.storage().onlineDdlTableGate().phase(tableId));
                assertEquals(OnlineDdlTerminalResult.ROLLED_BACK,
                        database.onlineDdlControl().find(pending.identity().ddlId())
                                .orElseThrow().terminalResult());

                writer.execute("COMMIT");
                QueryResult row = assertInstanceOf(QueryResult.class,
                        ddlSession.execute("SELECT category FROM index_docs WHERE id=1002"));
                assertEquals(1, row.rows().size());
                try (var lease = database.dictionary().openTable(
                        MdlOwnerId.of(925), QualifiedTableName.of("app", "index_docs"),
                        cn.zhangyis.db.dd.service.TableAccessIntent.READ,
                        Duration.ofSeconds(2))) {
                    assertEquals(List.of("id", "category"), lease.table().columns().stream()
                            .map(column -> column.name().canonicalName()).toList());
                }
            }
        }
    }

    /**
     * Shadow capture期间普通事务的INSERT/UPDATE/DELETE、savepoint回滚以及XA PREPARE都必须先force各自
     * candidate；final两遍reconcile只采用source current truth，因此已回滚candidate不复活，已提交普通/XA行
     * 则携带新row format进入target。
     */
    @Test
    void generalShadowAlterReconcilesConcurrentCommitSavepointRollbackAndXa() throws Exception {
        EngineConfig config = config().withOnlineDdlConfig(new OnlineDdlConfig(
                128L * 1024 * 1024, 1, 4096));
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var seed = database.openSession(options(true))) {
                for (int id = 1; id <= 160; id++) {
                    seed.execute("INSERT INTO index_docs (id, category) VALUES ("
                            + id + ", " + id + ")");
                }
            }
            long tableId;
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(926), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                tableId = lease.table().id().value();
            }

            try (var ddlSession = database.openSession(options(true));
                 var writer = database.openSession(options(false));
                 var xa = database.openSession(options(true));
                 var executor = Executors.newSingleThreadExecutor()) {
                var alter = executor.submit(() -> ddlSession.execute(
                        "ALTER TABLE index_docs ADD COLUMN status VARCHAR(16) DEFAULT 'new'"));
                await(() -> database.storage().onlineDdlTableGate().phase(tableId)
                                == OnlineDdlTablePhase.CAPTURING,
                        Duration.ofSeconds(5));

                writer.execute("INSERT INTO index_docs (id, category) VALUES (1001, 11)");
                writer.execute("UPDATE index_docs SET category=999 WHERE id=1");
                writer.execute("DELETE FROM index_docs WHERE id=2");
                writer.execute("SAVEPOINT transient_row");
                writer.execute("INSERT INTO index_docs (id, category) VALUES (1002, 12)");
                writer.execute("ROLLBACK TO SAVEPOINT transient_row");

                xa.execute("XA START 'online-shadow-xa', 'index-docs', 31");
                xa.execute("INSERT INTO index_docs (id, category) VALUES (1003, 13)");
                xa.execute("XA END 'online-shadow-xa', 'index-docs', 31");
                xa.execute("XA PREPARE 'online-shadow-xa', 'index-docs', 31");

                writer.execute("COMMIT");
                xa.execute("XA COMMIT 'online-shadow-xa', 'index-docs', 31");
                assertInstanceOf(CommandResult.class, alter.get(10, TimeUnit.SECONDS));

                QueryResult committed = assertInstanceOf(QueryResult.class,
                        ddlSession.execute("SELECT status, category FROM index_docs WHERE id=1001"));
                assertEquals("new", assertInstanceOf(SqlValue.StringValue.class,
                        committed.rows().getFirst().values().getFirst()).value());
                assertEquals(BigInteger.valueOf(11), assertInstanceOf(SqlValue.IntegerValue.class,
                        committed.rows().getFirst().values().getLast()).value());

                QueryResult updated = assertInstanceOf(QueryResult.class,
                        ddlSession.execute("SELECT category FROM index_docs WHERE id=1"));
                assertEquals(BigInteger.valueOf(999), assertInstanceOf(SqlValue.IntegerValue.class,
                        updated.rows().getFirst().values().getFirst()).value());
                assertTrue(assertInstanceOf(QueryResult.class,
                        ddlSession.execute("SELECT id FROM index_docs WHERE id=2")).rows().isEmpty());
                assertTrue(assertInstanceOf(QueryResult.class,
                        ddlSession.execute("SELECT id FROM index_docs WHERE id=1002")).rows().isEmpty());
                QueryResult xaCommitted = assertInstanceOf(QueryResult.class,
                        ddlSession.execute("SELECT status FROM index_docs WHERE id=1003"));
                assertEquals("new", assertInstanceOf(SqlValue.StringValue.class,
                        xaCommitted.rows().getFirst().values().getFirst()).value());
            }
        }
    }

    /** final sealed 验证必须拒绝已提交非 NULL 重复键并完整清理；删除冲突后，多个 NULL 仍可建 UNIQUE。 */
    @Test
    void onlineUniqueIndexDecidesCommittedConflictsAtFinalValidation() {
        EngineConfig config = config().withOnlineDdlConfig(new OnlineDdlConfig(
                128L * 1024 * 1024, 1, 4096));
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            database.ddl().createSchema(MdlOwnerId.of(930), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(2));
            TableDefinition table = database.ddl().createTable(MdlOwnerId.of(930),
                    new CreateTableCommand(QualifiedTableName.of("app", "unique_docs"), PageNo.of(128),
                            List.of(new CreateColumnSpec(ObjectName.of("id"),
                                            ColumnTypeDefinition.bigint(false, false)),
                                    new CreateColumnSpec(ObjectName.of("category"),
                                            ColumnTypeDefinition.integer(false, true))),
                            List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                                    List.of(new CreateIndexKeyPartSpec(
                                            ObjectName.of("id"), IndexOrder.ASC, 0))))),
                    Duration.ofSeconds(5));

            try (var session = database.openSession(options(true))) {
                session.execute("INSERT INTO unique_docs (id, category) VALUES (1, 7)");
                session.execute("INSERT INTO unique_docs (id, category) VALUES (2, 7)");
                session.execute("INSERT INTO unique_docs (id, category) VALUES (3, NULL)");
                session.execute("INSERT INTO unique_docs (id, category) VALUES (4, NULL)");

                assertThrows(SecondaryIndexBuildDuplicateKeyException.class,
                        () -> session.execute(
                                "CREATE UNIQUE INDEX uk_category ON unique_docs (category)"));
                assertEquals(OnlineDdlTablePhase.ABSENT,
                        database.storage().onlineDdlTableGate().phase(table.id().value()));
                try (var lease = database.dictionary().openTable(
                        MdlOwnerId.of(931), QualifiedTableName.of("app", "unique_docs"),
                        cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                    assertEquals(1, lease.table().indexes().size());
                }

                session.execute("DELETE FROM unique_docs WHERE id=2");
                assertInstanceOf(CommandResult.class, session.execute(
                        "CREATE UNIQUE INDEX uk_category ON unique_docs (category)"));
            }
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(932), QualifiedTableName.of("app", "unique_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                assertEquals(ObjectName.of("uk_category"), lease.table().indexes().getLast().name());
            }
        }
    }

    /** base scan 可能看见未提交重复键；该事务回滚后 final current truth 无冲突，Online UNIQUE 必须成功。 */
    @Test
    void onlineUniqueIndexIgnoresConcurrentConflictThatRollsBack() throws Exception {
        EngineConfig config = config().withOnlineDdlConfig(new OnlineDdlConfig(
                128L * 1024 * 1024, 1, 4096));
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            long tableId;
            try (var loader = database.openSession(options(true))) {
                for (int id = 1; id <= 100; id++) {
                    loader.execute("INSERT INTO index_docs (id, category) VALUES ("
                            + id + ", " + id + ")");
                }
            }
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(933), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                tableId = lease.table().id().value();
            }

            try (var ddlSession = database.openSession(options(true));
                 var writer = database.openSession(options(false));
                 var executor = Executors.newSingleThreadExecutor()) {
                var build = executor.submit(() -> ddlSession.execute(
                        "CREATE UNIQUE INDEX uk_category_live ON index_docs (category)"));
                await(() -> database.storage().onlineDdlTableGate().phase(tableId)
                        == OnlineDdlTablePhase.CAPTURING, Duration.ofSeconds(5));

                writer.execute("INSERT INTO index_docs (id, category) VALUES (1000, 1)");
                writer.execute("ROLLBACK");

                assertInstanceOf(CommandResult.class, build.get(10, TimeUnit.SECONDS));
            }
            try (var reader = database.openSession(options(true))) {
                assertTrue(assertInstanceOf(QueryResult.class,
                        reader.execute("SELECT id FROM index_docs WHERE id=1000")).rows().isEmpty());
            }
        }
    }

    /**
     * 两种 SQL DROP INDEX 语法必须共享原子删除链；autocommit=0 在 DROP 前提交旧事务并在结束后恢复空事务，
     * 删除只改变访问路径，不丢失聚簇行，且重启后 DD 不得复活已回收索引。
     */
    @Test
    void sqlDropsSecondaryIndexesWithImplicitCommitAndKeepsRowsAcrossRestart() {
        EngineConfig config = config();
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var session = database.openSession(options(false))) {
                session.execute("INSERT INTO index_docs (id, category) VALUES (1, 7)");
                session.execute("INSERT INTO index_docs (id, category) VALUES (2, 8)");
                session.execute("CREATE INDEX idx_category ON index_docs (category)");

                assertInstanceOf(CommandResult.class,
                        session.execute("DROP INDEX idx_category ON index_docs"));
                assertTrue(session.snapshot().transactionActive(),
                        "autocommit=0 的 DROP INDEX 后必须恢复新的 implicit transaction");
                QueryResult first = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT category FROM index_docs WHERE id=1"));
                assertEquals(1, first.rows().size(),
                        "DROP 只移除访问路径，DDL 前 implicit commit 的聚簇行必须保留");

                int onlineOperationsBeforeAlter = database.onlineDdlControl().list().size();
                session.execute("ALTER TABLE index_docs ADD INDEX idx_category (category DESC)");
                assertInstanceOf(CommandResult.class,
                        session.execute("ALTER TABLE index_docs DROP INDEX idx_category"));
                List<OnlineDdlOperationSnapshot> alterOperations = database.onlineDdlControl().list()
                        .stream()
                        .skip(onlineOperationsBeforeAlter)
                        .toList();
                assertEquals(List.of(DdlLogOperation.CREATE_INDEX, DdlLogOperation.DROP_INDEX),
                        alterOperations.stream()
                                .map(snapshot -> snapshot.identity().operation())
                                .toList(),
                        "single-index ALTER actions must route through the online protocols");
                assertTrue(alterOperations.stream().allMatch(snapshot ->
                        snapshot.cancelCapable()
                                && snapshot.terminalResult() == OnlineDdlTerminalResult.COMPLETED));
            }
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(903), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                assertEquals(List.of("primary"), lease.table().indexes().stream()
                        .map(index -> index.name().canonicalName()).toList());
            }
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var reader = reopened.openSession(options(true))) {
                QueryResult result = assertInstanceOf(QueryResult.class,
                        reader.execute("SELECT category FROM index_docs WHERE id=2"));
                assertEquals(1, result.rows().size());
            }
            try (var lease = reopened.dictionary().openTable(
                    MdlOwnerId.of(904), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(2))) {
                assertEquals(1, lease.table().indexes().size());
            }
        }
    }

    /**
     * 多 action ALTER 必须只建立一个 shadow space，按声明顺序定位新列并复制 live rows/重建索引；
     * 随后的 metadata-only COMMENT+RENAME 保持新 row format，重启后 DD/SDI/物理 binding 一致。
     */
    @Test
    void sqlGeneralAlterRebuildsRowsOnceAndPersistsInstantMetadataRename() {
        EngineConfig config = config();
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var session = database.openSession(options(true))) {
                session.execute("INSERT INTO index_docs (id, category) VALUES (1, 7)");
                session.execute("INSERT INTO index_docs (id, category) VALUES (2, 8)");

                assertInstanceOf(CommandResult.class, session.execute("""
                        ALTER TABLE index_docs
                          DEFAULT CHARACTER SET 1 COLLATE 2,
                          ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'new' AFTER id,
                          ADD INDEX idx_status (status),
                          COMMENT='rebuilt'
                        """));
                QueryResult rebuilt = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT status, category FROM index_docs WHERE id=1"));
                assertEquals("new", assertInstanceOf(
                        SqlValue.StringValue.class,
                        rebuilt.rows().getFirst().values().getFirst()).value());
                assertEquals(java.math.BigInteger.valueOf(7), assertInstanceOf(
                        SqlValue.IntegerValue.class,
                        rebuilt.rows().getFirst().values().getLast()).value());

                int onlineOperationsBeforeMetadataAlter =
                        database.onlineDdlControl().list().size();
                session.execute(
                        "ALTER TABLE index_docs COMMENT='renamed', RENAME TO renamed_docs");
                List<OnlineDdlOperationSnapshot> metadataAlterOperations =
                        database.onlineDdlControl().list().stream()
                                .skip(onlineOperationsBeforeMetadataAlter)
                                .toList();
                assertEquals(1, metadataAlterOperations.size());
                assertEquals(DdlLogOperation.ALTER_TABLE_INPLACE,
                        metadataAlterOperations.getFirst().identity().operation());
                assertTrue(metadataAlterOperations.getFirst().cancelCapable());
                assertEquals(OnlineDdlTerminalResult.COMPLETED,
                        metadataAlterOperations.getFirst().terminalResult());
                QueryResult renamed = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT status FROM renamed_docs WHERE id=2"));
                assertEquals("new", assertInstanceOf(
                        SqlValue.StringValue.class,
                        renamed.rows().getFirst().values().getFirst()).value());
            }
            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(905), QualifiedTableName.of("app", "renamed_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ,
                    Duration.ofSeconds(2))) {
                assertEquals("renamed", lease.table().options().comment());
                assertEquals(List.of("id", "status", "category"),
                        lease.table().columns().stream()
                                .map(column -> column.name().canonicalName()).toList());
                assertEquals(List.of("primary", "idx_status"),
                        lease.table().indexes().stream()
                                .map(index -> index.name().canonicalName()).toList());
            }
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var reader = reopened.openSession(options(true))) {
                QueryResult result = assertInstanceOf(QueryResult.class,
                        reader.execute("SELECT category FROM renamed_docs WHERE status='new'"));
                assertEquals(2, result.rows().size());
            }
        }
    }

    /**
     * 通用 INPLACE_INDEX 必须让同组 DROP 与两个 ADD 共用一个 manifest、descriptor generation 和 DD 版本；
     * 旧索引只在 retirement fence 安全后回收，重启不得复活旧 binding 或丢失任一新索引。
     */
    @Test
    void sqlGeneralInplaceAlterPublishesMultipleIndexActionsAtomically() {
        EngineConfig config = config();
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createIndexBuildTable(database);
            try (var session = database.openSession(options(true))) {
                session.execute("INSERT INTO index_docs (id, category) VALUES (1, 7)");
                session.execute("INSERT INTO index_docs (id, category) VALUES (2, 8)");
                session.execute("CREATE INDEX idx_old ON index_docs (category DESC)");
                int operationCount = database.onlineDdlControl().list().size();

                assertInstanceOf(CommandResult.class, session.execute("""
                        ALTER TABLE index_docs
                          DROP INDEX idx_old,
                          ADD INDEX idx_category (category),
                          ADD UNIQUE INDEX uk_id (id)
                        """));

                List<OnlineDdlOperationSnapshot> operations =
                        database.onlineDdlControl().list().stream()
                                .skip(operationCount)
                                .toList();
                assertEquals(1, operations.size());
                assertEquals(DdlLogOperation.ALTER_TABLE_INPLACE,
                        operations.getFirst().identity().operation());
                assertEquals(OnlineDdlTerminalResult.COMPLETED,
                        operations.getFirst().terminalResult());
                try (var lease = database.dictionary().openTable(
                        MdlOwnerId.of(906), QualifiedTableName.of("app", "index_docs"),
                        cn.zhangyis.db.dd.service.TableAccessIntent.READ,
                        Duration.ofSeconds(2))) {
                    assertEquals(List.of("PRIMARY", "idx_category", "uk_id"),
                            lease.table().indexes().stream()
                                    .map(index -> index.name().displayName()).toList());
                }
                QueryResult result = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT id FROM index_docs WHERE category=8"));
                assertEquals(1, result.rows().size());
            }
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var lease = reopened.dictionary().openTable(
                    MdlOwnerId.of(907), QualifiedTableName.of("app", "index_docs"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ,
                    Duration.ofSeconds(2))) {
                assertEquals(List.of("PRIMARY", "idx_category", "uk_id"),
                        lease.table().indexes().stream()
                                .map(index -> index.name().displayName()).toList());
            }
            try (var session = reopened.openSession(options(true))) {
                QueryResult result = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT category FROM index_docs WHERE id=1"));
                assertEquals(1, result.rows().size());
            }
        }
    }

    /**
     * 阻塞 ALTER 遇到旧空间 external TEXT 时必须 hydrate 完整值并在 shadow LOB segment 重分配；
     * 新聚簇记录不能继续引用已被交换后删除的旧 tablespace。
     */
    @Test
    void sqlBlockingAlterReallocatesExternalLobIntoShadowSpace() {
        EngineConfig config = config();
        String body = "跨空间LOB".repeat(180);
        try (DatabaseEngine database = new DatabaseEngine(config)) {
            database.open();
            createTable(database);
            try (var session = database.openSession(options(true))) {
                session.execute(
                        "INSERT INTO docs (id, email, body) VALUES "
                                + "(81, 'lob-alter@example.test', '"
                                + body + "')");

                session.execute("""
                        ALTER TABLE docs
                          ADD COLUMN status INT NOT NULL DEFAULT 4 AFTER id
                        """);

                QueryResult result = assertInstanceOf(
                        QueryResult.class,
                        session.execute(
                                "SELECT body, status FROM docs WHERE id=81"));
                assertEquals(body, assertInstanceOf(
                        SqlValue.StringValue.class,
                        result.rows().getFirst().values().getFirst()).value());
                assertEquals(BigInteger.valueOf(4), assertInstanceOf(
                        SqlValue.IntegerValue.class,
                        result.rows().getFirst().values().getLast()).value());
            }
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config)) {
            reopened.open();
            try (var session = reopened.openSession(options(true))) {
                QueryResult result = assertInstanceOf(
                        QueryResult.class,
                        session.execute(
                                "SELECT body FROM docs WHERE email='lob-alter@example.test'"));
                assertEquals(body, assertInstanceOf(
                        SqlValue.StringValue.class,
                        result.rows().getFirst().values().getFirst()).value());
            }
        }
    }

    /**
     * SQL DISCARD/IMPORT 只能使用实例受控 transfer 目录。管理员把 discarded 文件复制并 force 到
     * incoming 后，IMPORT 校验 page0 identity 并恢复 ACTIVE。
     */
    @Test
    void sqlDiscardAndImportUseControlledTransferDirectories() throws Exception {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            createIndexBuildTable(database);
            try (var session = database.openSession(options(true))) {
                session.execute("ALTER TABLE index_docs DISCARD TABLESPACE");
                Path discardedDirectory =
                        directory.resolve("tablespace-transfer").resolve("discarded");
                Path discarded;
                try (var files = Files.list(discardedDirectory)) {
                    discarded = files.findFirst().orElseThrow();
                }
                Path incomingDirectory =
                        directory.resolve("tablespace-transfer").resolve("incoming");
                Files.createDirectories(incomingDirectory);
                Files.copy(discarded, incomingDirectory.resolve(discarded.getFileName()));

                session.execute("ALTER TABLE index_docs IMPORT TABLESPACE");
                try (var lease = database.dictionary().openTable(
                        MdlOwnerId.of(906), QualifiedTableName.of("app", "index_docs"),
                        cn.zhangyis.db.dd.service.TableAccessIntent.READ,
                        Duration.ofSeconds(2))) {
                    assertEquals(TableState.ACTIVE, lease.table().state());
                }
            }
        }
    }

    /** autocommit 点 UPDATE/DELETE 从公开 SQL 维护 secondary，并完成 external LOB replacement。 */
    @Test
    void autocommitUpdateAndDeleteMaintainSecondaryAndReplacementLob() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            createTable(database);
            String before = "旧值".repeat(240);
            String after = "新值".repeat(260);
            try (var session = database.openSession(options(true))) {
                session.execute("INSERT INTO docs (id, email, body) VALUES (11, 'before@example.test', '"
                        + before + "')");
                UpdateResult updated = assertInstanceOf(UpdateResult.class, session.execute(
                        "UPDATE docs SET email='after@example.test', body='" + after + "' WHERE id=11"));
                assertEquals(1, updated.affectedRows());
                assertTrue(queryByEmail(session, "BEFORE@example.test").rows().isEmpty());
                QueryResult current = queryByEmail(session, "AFTER@example.test");
                assertEquals(after, assertInstanceOf(SqlValue.StringValue.class,
                        current.rows().getFirst().values().getFirst()).value());

                UpdateResult deleted = assertInstanceOf(UpdateResult.class,
                        session.execute("DELETE FROM docs WHERE id=11"));
                assertEquals(1, deleted.affectedRows());
                assertTrue(queryByEmail(session, "AFTER@example.test").rows().isEmpty());
            }
        }
    }

    /** full rollback 恢复 UPDATE/DELETE 前的 secondary identity 与旧 external LOB。 */
    @Test
    void rollbackRestoresSecondaryAndExternalLobAcrossUpdateAndDelete() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            createTable(database);
            String before = "原始".repeat(240);
            String replacement = "替换".repeat(260);
            try (var seed = database.openSession(options(true))) {
                seed.execute("INSERT INTO docs (id, email, body) VALUES (12, 'original@example.test', '"
                        + before + "')");
            }
            try (var session = database.openSession(options(false))) {
                assertEquals(1, assertInstanceOf(UpdateResult.class, session.execute(
                        "UPDATE docs SET email='replacement@example.test', body='" + replacement
                                + "' WHERE id=12")).affectedRows());
                assertEquals(replacement, assertInstanceOf(SqlValue.StringValue.class,
                        queryByEmail(session, "REPLACEMENT@example.test")
                                .rows().getFirst().values().getFirst()).value());
                session.execute("ROLLBACK");
                assertTrue(queryByEmail(session, "REPLACEMENT@example.test").rows().isEmpty());
                assertEquals(before, assertInstanceOf(SqlValue.StringValue.class,
                        queryByEmail(session, "ORIGINAL@example.test")
                                .rows().getFirst().values().getFirst()).value());

                assertEquals(1, assertInstanceOf(UpdateResult.class,
                        session.execute("DELETE FROM docs WHERE id=12")).affectedRows());
                assertTrue(queryByEmail(session, "ORIGINAL@example.test").rows().isEmpty());
                session.execute("ROLLBACK");
                assertEquals(before, assertInstanceOf(SqlValue.StringValue.class,
                        queryByEmail(session, "ORIGINAL@example.test")
                                .rows().getFirst().values().getFirst()).value());
            }
        }
    }

    private static QueryResult queryByEmail(cn.zhangyis.db.session.SqlSession session, String email) {
        return assertInstanceOf(QueryResult.class,
                session.execute("SELECT body, id FROM docs WHERE email='" + email + "'"));
    }

    private static void createTable(DatabaseEngine database) {
        database.ddl().createSchema(MdlOwnerId.of(900), ObjectName.of("app"), 1, 1, Duration.ofSeconds(2));
        database.ddl().createTable(MdlOwnerId.of(900), new CreateTableCommand(
                QualifiedTableName.of("app", "docs"), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false)),
                        new CreateColumnSpec(ObjectName.of("email"), new ColumnTypeDefinition(
                                DictionaryTypeId.VARCHAR, false, false, 160, 0, 1, 2, List.of())),
                        new CreateColumnSpec(ObjectName.of("body"), new ColumnTypeDefinition(DictionaryTypeId.TEXT,
                                false, false, 65_535, 0, 1, 1, List.of()))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                                List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))),
                        new CreateIndexSpec(ObjectName.of("uq_email"), true, false,
                                List.of(new CreateIndexKeyPartSpec(ObjectName.of("email"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(5));
    }

    private static void createIndexBuildTable(DatabaseEngine database) {
        database.ddl().createSchema(MdlOwnerId.of(901), ObjectName.of("app"), 1, 1,
                Duration.ofSeconds(2));
        database.ddl().createTable(MdlOwnerId.of(901), new CreateTableCommand(
                QualifiedTableName.of("app", "index_docs"), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"),
                                ColumnTypeDefinition.bigint(false, false)),
                        new CreateColumnSpec(ObjectName.of("category"),
                                ColumnTypeDefinition.integer(false, false))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                        List.of(new CreateIndexKeyPartSpec(
                                ObjectName.of("id"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(5));
    }

    private SessionOptions options(boolean autocommit) {
        return new SessionOptions(Optional.of("app"), autocommit, SqlIsolationLevel.REPEATABLE_READ,
                SqlDurabilityMode.FLUSH_ON_COMMIT, ZoneId.of("UTC"), Duration.ofSeconds(3),
                Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofSeconds(2));
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    /** 有界轮询并发 phase，超时用断言失败保留当前测试线程。 */
    private static void await(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("condition did not become true before " + timeout);
            }
            Thread.yield();
        }
    }
}
