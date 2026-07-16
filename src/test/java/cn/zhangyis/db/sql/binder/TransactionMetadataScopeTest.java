package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.mdl.MdlDuration;
import cn.zhangyis.db.dd.mdl.MdlKey;
import cn.zhangyis.db.dd.mdl.MdlMode;
import cn.zhangyis.db.dd.mdl.MdlRequest;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** 事务级 DD pin/MDL 所有权和 statement publish/abort 的真实并发测试。 */
class TransactionMetadataScopeTest {
    @TempDir Path directory;

    /** bind 失败只撤销 staged lease；成功发布后 READ 复用且 READ→WRITE 原子替换旧 lease。 */
    @Test
    void publishesAbortsReusesAndUpgradesLeases() {
        try (BinderTestFixture fixture = new BinderTestFixture(directory);
             TransactionMetadataScope transaction = new TransactionMetadataScope(
                     fixture.dictionary, MdlOwnerId.of(100))) {
            try (StatementBindingScope failed = transaction.beginStatement(Duration.ofSeconds(1))) {
                assertNotNull(failed.openTable(QualifiedTableName.of("app", "orders"), TableAccessIntent.READ));
            }
            assertTrue(fixture.locks.snapshot().granted().isEmpty());

            try (StatementBindingScope read = transaction.beginStatement(Duration.ofSeconds(1))) {
                read.openTable(QualifiedTableName.of("app", "orders"), TableAccessIntent.READ);
                read.publish();
            }
            assertEquals(2, fixture.locks.snapshot().granted().size());

            try (StatementBindingScope reused = transaction.beginStatement(Duration.ofSeconds(1))) {
                reused.openTable(QualifiedTableName.of("APP", "ORDERS"), TableAccessIntent.READ);
                reused.publish();
            }
            assertEquals(2, fixture.locks.snapshot().granted().size());

            try (StatementBindingScope write = transaction.beginStatement(Duration.ofSeconds(1))) {
                write.openTable(QualifiedTableName.of("app", "orders"), TableAccessIntent.WRITE);
                write.publish();
            }
            assertEquals(2, fixture.locks.snapshot().granted().size());
            assertTrue(fixture.locks.snapshot().granted().stream()
                    .filter(grant -> grant.key().namespace() == cn.zhangyis.db.dd.mdl.MdlNamespace.TABLE)
                    .allMatch(grant -> grant.mode() == MdlMode.SHARED_WRITE));
        }
    }

    /** DDL X 必须等到 transaction scope 反序释放 table pin/MDL 后才能越过。 */
    @Test
    void holdsMetadataUntilTransactionEndAndCloseIsIdempotent() throws Exception {
        try (BinderTestFixture fixture = new BinderTestFixture(directory)) {
            TransactionMetadataScope transaction = new TransactionMetadataScope(fixture.dictionary,
                    MdlOwnerId.of(101));
            try (StatementBindingScope statement = transaction.beginStatement(Duration.ofSeconds(1))) {
                statement.openTable(QualifiedTableName.of("app", "orders"), TableAccessIntent.WRITE);
                statement.publish();
            }
            CountDownLatch started = new CountDownLatch(1);
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var ddl = executor.submit(() -> {
                    started.countDown();
                    try (var ignored = fixture.locks.acquire(new MdlRequest(MdlOwnerId.of(102),
                            MdlKey.table(QualifiedTableName.of("app", "orders").canonicalKey()),
                            MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), Duration.ofSeconds(2))) {
                        return true;
                    }
                });
                assertTrue(started.await(1, TimeUnit.SECONDS));
                awaitWaiter(fixture);
                transaction.close();
                transaction.close();
                assertTrue(ddl.get(1, TimeUnit.SECONDS));
            }
            assertTrue(fixture.cache.awaitUnpinned(cn.zhangyis.db.dd.domain.TableId.of(2),
                    Duration.ofSeconds(1)));
        }
    }

    private static void awaitWaiter(BinderTestFixture fixture) {
        long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (fixture.locks.snapshot().waiting().isEmpty() && System.nanoTime() < deadline) Thread.onSpinWait();
        assertEquals(1, fixture.locks.snapshot().waiting().size());
    }
}
