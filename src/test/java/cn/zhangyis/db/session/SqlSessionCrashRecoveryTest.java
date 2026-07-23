package cn.zhangyis.db.session;

import cn.zhangyis.db.dd.ddl.*;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.sql.executor.QueryResult;
import cn.zhangyis.db.sql.type.SqlValue;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 两套 DatabaseEngine 进程对象之间的公开 Session 持久结果验收。rollback A/B 的精确 commit 后 crash hook 继续由
 * RollbackServiceTest 覆盖；这里验证这些物理协议经组合根重开后不会泄漏 external LOB 或丢失已提交值。
 */
class SqlSessionCrashRecoveryTest {
    @TempDir Path directory;

    /** 已提交 INSERT+external LOB 在关闭/重开后可由 recovery 打开并完整 hydrate。 */
    @Test
    void committedExternalLobSurvivesEngineRestart() {
        String payload = "恢复".repeat(300);
        try (DatabaseEngine database = new DatabaseEngine(SqlSessionTestSupport.config(directory))) {
            database.open();
            SqlSessionTestSupport.createSchema(database);
            createTable(database);
            try (SqlSession session = database.openSession(SqlSessionTestSupport.options(true,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(2)))) {
                session.execute("INSERT INTO recovery_t (id, body) VALUES (1, '" + payload + "')");
            }
        }
        try (DatabaseEngine reopened = new DatabaseEngine(SqlSessionTestSupport.config(directory))) {
            reopened.open();
            try (SqlSession session = reopened.openSession(SqlSessionTestSupport.options(true,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(2)))) {
                QueryResult result = query(session, 1);
                assertEquals(List.of(new SqlValue.IntegerValue(BigInteger.ONE),
                        new SqlValue.StringValue(payload)), result.rows().getFirst().values());
            }
        }
    }

    /** explicit rollback 的 row 删除与 LOB ownership 回收在重开后仍收敛为空，不因 redo replay 复活。 */
    @Test
    void rolledBackExternalLobDoesNotReappearAfterRestart() {
        try (DatabaseEngine database = new DatabaseEngine(SqlSessionTestSupport.config(directory))) {
            database.open();
            SqlSessionTestSupport.createSchema(database);
            createTable(database);
            try (SqlSession session = database.openSession(SqlSessionTestSupport.options(false,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(2)))) {
                session.execute("INSERT INTO recovery_t (id, body) VALUES (2, '" + "撤销".repeat(300) + "')");
                session.execute("ROLLBACK");
            }
        }
        try (DatabaseEngine reopened = new DatabaseEngine(SqlSessionTestSupport.config(directory))) {
            reopened.open();
            try (SqlSession session = reopened.openSession(SqlSessionTestSupport.options(true,
                    SqlIsolationLevel.READ_COMMITTED, Duration.ofSeconds(2)))) {
                assertTrue(query(session, 2).rows().isEmpty());
            }
        }
    }

    private static void createTable(DatabaseEngine database) {
        database.ddl().createTable(MdlOwnerId.of(30_000), new CreateTableCommand(
                QualifiedTableName.of("app", "recovery_t"), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false)),
                        new CreateColumnSpec(ObjectName.of("body"), new ColumnTypeDefinition(DictionaryTypeId.TEXT,
                                false, false, 65_535, 0, 1, 1, List.of()))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                        List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))))),
                Duration.ofSeconds(5));
    }

    private static QueryResult query(SqlSession session, long id) {
        return assertInstanceOf(QueryResult.class,
                session.execute("SELECT * FROM recovery_t WHERE id=" + id));
    }
}
