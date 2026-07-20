package cn.zhangyis.db.engine;

import cn.zhangyis.db.dd.ddl.*;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.session.SessionOptions;
import cn.zhangyis.db.session.SessionState;
import cn.zhangyis.db.sql.executor.QueryResult;
import cn.zhangyis.db.sql.executor.UpdateResult;
import cn.zhangyis.db.sql.executor.CommandResult;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.executor.storage.SqlDurabilityMode;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;
import cn.zhangyis.db.storage.engine.EngineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

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

                session.execute("ALTER TABLE index_docs ADD INDEX idx_category (category DESC)");
                assertInstanceOf(CommandResult.class,
                        session.execute("ALTER TABLE index_docs DROP INDEX idx_category"));
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
}
