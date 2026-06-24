package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 后台 purge driver：周期/on-demand 驱动 runBatch、stop 幂等、runBatch 失败进 FAILED。用 fake target 注入确定性。
 */
class PurgeDriverWorkerTest {

    @Test
    void drivesRunBatchPeriodically() {
        FakeTarget target = new FakeTarget();
        PurgeDriverWorker worker = new PurgeDriverWorker(target, 16, Duration.ofMillis(20));
        worker.start();
        try {
            assertTrue(awaitUntil(() -> target.calls.get() >= 1, Duration.ofSeconds(2)),
                    "periodic tick should drive runBatch");
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    @Test
    void requestPurgeTriggersImmediateRun() {
        FakeTarget target = new FakeTarget();
        PurgeDriverWorker worker = new PurgeDriverWorker(target, 16, Duration.ofSeconds(30));
        worker.start();
        try {
            worker.requestPurge();
            assertTrue(awaitUntil(() -> target.calls.get() >= 1, Duration.ofSeconds(2)),
                    "requestPurge should run before the long periodic interval");
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    @Test
    void stopHaltsWorkerAndIsIdempotent() {
        PurgeDriverWorker worker = new PurgeDriverWorker(new FakeTarget(), 16, Duration.ofMillis(20));
        worker.start();
        assertTrue(worker.stop(Duration.ofSeconds(2)));
        assertEquals(PurgeDriverWorkerState.STOPPED, worker.state());
        assertTrue(worker.stop(Duration.ofSeconds(2)), "second stop idempotent");
    }

    @Test
    void runBatchFailureMovesToFailedState() {
        FakeTarget target = new FakeTarget();
        target.failWith = new DatabaseRuntimeException("induced purge failure");
        PurgeDriverWorker worker = new PurgeDriverWorker(target, 16, Duration.ofMillis(20));
        worker.start();
        try {
            assertTrue(awaitUntil(() -> worker.state() == PurgeDriverWorkerState.FAILED, Duration.ofSeconds(2)),
                    "runBatch failure must move worker to FAILED");
            assertTrue(worker.failure().isPresent());
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    private static boolean awaitUntil(BooleanSupplier cond, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return cond.getAsBoolean();
    }

    /** 测试用 purge 端口：计数 runBatch，可注入失败。 */
    private static final class FakeTarget implements PurgeTarget {
        final AtomicInteger calls = new AtomicInteger();
        volatile DatabaseRuntimeException failWith;

        @Override
        public PurgeSummary runBatch(int maxLogs) {
            calls.incrementAndGet();
            if (failWith != null) {
                throw failWith;
            }
            return new PurgeSummary(0, 0, 0);
        }
    }
}
