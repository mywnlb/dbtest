package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.storage.api.ReadViewRetentionTimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** SHADOW finalization只等待fence前ReadView，且任何等待都有timeout/中断释放路径。 */
class TransactionReadViewRetentionBarrierTest {

    /** fence后创建的新快照不依赖旧schema；关闭唯一旧快照后等待应完成，即使新快照仍存活。 */
    @Test
    void waitsOnlyForReadViewsAtOrBeforeCapturedGeneration() throws Exception {
        TransactionSystem system = new TransactionSystem();
        TransactionManager transactions = new TransactionManager(system);
        ReadViewManager views = transactions.readViewManager();
        Transaction oldTransaction = transactions.begin(new TransactionOptions(
                IsolationLevel.REPEATABLE_READ, true, true));
        views.openReadView(oldTransaction);
        long fence = system.captureReadViewGeneration();
        Transaction newTransaction = transactions.begin(new TransactionOptions(
                IsolationLevel.REPEATABLE_READ, true, true));
        views.openReadView(newTransaction);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch started = new CountDownLatch(1);
            Future<?> waiter = executor.submit(() -> {
                started.countDown();
                system.awaitReadViewsClosedThrough(fence, Duration.ofSeconds(2));
            });
            started.await();
            views.release(oldTransaction);
            waiter.get();
        } finally {
            views.release(newTransaction);
        }
    }

    /** 超时必须抛领域异常且释放协调锁，随后关闭快照和再次等待均可继续。 */
    @Test
    void timesOutWithoutLeakingTransactionSystemLock() {
        TransactionSystem system = new TransactionSystem();
        TransactionManager transactions = new TransactionManager(system);
        Transaction transaction = transactions.begin(new TransactionOptions(
                IsolationLevel.REPEATABLE_READ, true, true));
        transactions.readViewManager().openReadView(transaction);
        long fence = system.captureReadViewGeneration();

        assertThrows(ReadViewRetentionTimeoutException.class,
                () -> system.awaitReadViewsClosedThrough(fence, Duration.ofMillis(20)));
        transactions.readViewManager().release(transaction);
        assertDoesNotThrow(() -> system.awaitReadViewsClosedThrough(
                fence, Duration.ofSeconds(1)));
    }
}
