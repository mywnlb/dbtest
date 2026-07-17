package cn.zhangyis.db.engine;

import cn.zhangyis.db.dd.ddl.*;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.session.SessionOptions;
import cn.zhangyis.db.session.SessionState;
import cn.zhangyis.db.sql.executor.QueryResult;
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
