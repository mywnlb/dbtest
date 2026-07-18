package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.exception.MetadataLockTimeoutException;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.sql.executor.QueryResult;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/** 两个公开 Session 的 RR/RC 可见性、row lock timeout 与 transaction-duration MDL 验收。 */
class SqlSessionMvccConcurrencyTest {
    @TempDir Path directory;

    /** 未提交 INSERT 不可见；RC 在 commit 后可见，已建立 RR view 仍不可见，新事务才可见。 */
    @Test
    void enforcesReadCommittedAndRepeatableReadVisibility() {
        try (DatabaseEngine database = openWithTables()) {
            try (SqlSession writer = database.openSession(SqlSessionTestSupport.options(false,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(1)));
                 SqlSession rc = database.openSession(SqlSessionTestSupport.options(true,
                         SqlIsolationLevel.READ_COMMITTED, Duration.ofSeconds(1)));
                 SqlSession rr = database.openSession(SqlSessionTestSupport.options(false,
                         SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(1)))) {
                writer.execute("INSERT INTO data_t (id) VALUES (1)");
                assertTrue(query(rc, 1).rows().isEmpty());
                writer.execute("COMMIT");
                assertEquals(1, query(rc, 1).rows().size());

                writer.execute("INSERT INTO data_t (id) VALUES (2)");
                assertTrue(query(rr, 2).rows().isEmpty(), "first RR read establishes the old view");
                writer.execute("COMMIT");
                assertTrue(query(rr, 2).rows().isEmpty(), "same RR transaction must reuse its view");
                rr.execute("COMMIT");
                assertEquals(1, query(rr, 2).rows().size(), "new implicit transaction gets a new RR view");

                writer.execute("INSERT INTO data_t (id) VALUES (3)");
                writer.execute("ROLLBACK");
                assertTrue(query(rc, 3).rows().isEmpty());
            }
        }
    }

    /** 冲突 INSERT 有界超时且失败 Session 清理后可复用；持锁事务 rollback 后同 key 可成功写入。 */
    @Test
    void rowLockWaitTimesOutAndCleansStatementTransaction() {
        try (DatabaseEngine database = openWithTables()) {
            try (SqlSession owner = database.openSession(SqlSessionTestSupport.options(false,
                    SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(1)));
                 SqlSession contender = database.openSession(SqlSessionTestSupport.options(true,
                         SqlIsolationLevel.REPEATABLE_READ, Duration.ofMillis(100)))) {
                owner.execute("INSERT INTO data_t (id) VALUES (4)");
                assertThrows(DatabaseRuntimeException.class,
                        () -> contender.execute("INSERT INTO data_t (id) VALUES (4)"));
                assertFalse(contender.snapshot().transactionActive(),
                        "failed autocommit statement must full-rollback its temporary transaction");
                owner.execute("ROLLBACK");
                contender.execute("INSERT INTO data_t (id) VALUES (4)");
                assertEquals(1, query(contender, 4).rows().size());
            }
        }
    }

    /** SELECT 的 table SR 保持到 transaction end，DDL X 等待 rollback 后才可执行。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void metadataExclusiveWaitsForTransactionScope() throws Exception {
        try (DatabaseEngine database = openWithTables();
             SqlSession holder = database.openSession(SqlSessionTestSupport.options(false,
                     SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(1)))) {
            holder.execute("SELECT * FROM mdl_t WHERE id=1");
            CountDownLatch started = new CountDownLatch(1);
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var drop = executor.submit(() -> {
                    started.countDown();
                    database.ddl().dropTable(MdlOwnerId.of(20_000), QualifiedTableName.of("app", "mdl_t"),
                            Duration.ofSeconds(2));
                    return true;
                });
                assertTrue(started.await(1, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> drop.get(100, TimeUnit.MILLISECONDS));
                holder.execute("ROLLBACK");
                assertTrue(drop.get(1, TimeUnit.SECONDS));
            }
        }
    }

    /** DDL 调用方使用与 SessionId 相同的普通数字时也不能被识别成同一 MDL owner。 */
    @Test
    void ddlOwnerCannotAliasSessionOwner() {
        try (DatabaseEngine database = openWithTables();
             SqlSession holder = database.openSession(SqlSessionTestSupport.options(false,
                     SqlIsolationLevel.REPEATABLE_READ, Duration.ofSeconds(1)))) {
            assertEquals(1L, holder.id().value(), "fixture 的首个 Session 用于复现历史 owner=1 碰撞");
            holder.execute("SELECT * FROM mdl_t WHERE id=1");

            assertThrows(MetadataLockTimeoutException.class,
                    () -> database.ddl().dropTable(MdlOwnerId.of(1),
                            QualifiedTableName.of("app", "mdl_t"), Duration.ofMillis(80)));
            try (var lease = database.dictionary().openTable(MdlOwnerId.of(30_001),
                    QualifiedTableName.of("app", "mdl_t"),
                    cn.zhangyis.db.dd.service.TableAccessIntent.READ, Duration.ofSeconds(1))) {
                assertEquals(cn.zhangyis.db.dd.domain.TableState.ACTIVE, lease.table().state());
            }
        }
    }

    private DatabaseEngine openWithTables() {
        DatabaseEngine database = new DatabaseEngine(SqlSessionTestSupport.config(directory));
        database.open();
        SqlSessionTestSupport.createSchema(database);
        SqlSessionTestSupport.createSimpleTable(database, "data_t", 20_001);
        SqlSessionTestSupport.createSimpleTable(database, "mdl_t", 20_002);
        return database;
    }

    private static QueryResult query(SqlSession session, long id) {
        return assertInstanceOf(QueryResult.class,
                session.execute("SELECT * FROM data_t WHERE id=" + id));
    }
}
