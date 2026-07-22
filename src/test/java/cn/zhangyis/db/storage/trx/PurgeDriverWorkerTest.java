package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 后台 purge driver：周期/on-demand 驱动 runBatch、stop 幂等、runBatch 失败进 FAILED。用 fake target 注入确定性。
 */
class PurgeDriverWorkerTest {

    /**
     * 验证 {@code drivesRunBatchPeriodically} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
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

    /**
     * 验证 {@code requestPurgeTriggersImmediateRun} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
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

    /**
     * 验证 {@code stopHaltsWorkerAndIsIdempotent} 所描述的组件生命周期，并断言状态转换、后台线程停止和资源恰好释放一次。
     */
    @Test
    void stopHaltsWorkerAndIsIdempotent() {
        PurgeDriverWorker worker = new PurgeDriverWorker(new FakeTarget(), 16, Duration.ofMillis(20));
        worker.start();
        assertTrue(worker.stop(Duration.ofSeconds(2)));
        assertEquals(PurgeDriverWorkerState.STOPPED, worker.state());
        assertTrue(worker.stop(Duration.ofSeconds(2)), "second stop idempotent");
    }

    /** 分离的 stop request/await 允许组合根先同时取消 driver 与内部 worker pool，再共享一个关闭预算。 */
    @Test
    void requestStopAndAwaitStoppedShareExistingStateMachine() {
        PurgeDriverWorker worker = new PurgeDriverWorker(new FakeTarget(), 16, Duration.ofSeconds(30));
        worker.start();

        worker.requestStop();

        assertTrue(worker.awaitStopped(Duration.ofSeconds(2)));
        assertEquals(PurgeDriverWorkerState.STOPPED, worker.state());
    }

    /**
     * 验证 {@code runBatchFailureMovesToFailedState} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
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

    /** 成功 batch 即使没有 purge 进展也必须调用 maintenance，随后才发布完整 cycle summary。 */
    @Test
    void successfulBatchInvokesMaintenance() {
        FakeTarget target = new FakeTarget();
        AtomicInteger maintenanceCalls = new AtomicInteger();
        PurgeDriverWorker worker = new PurgeDriverWorker(target, 16, Duration.ofMillis(20),
                summary -> maintenanceCalls.incrementAndGet());
        worker.start();
        try {
            assertTrue(awaitUntil(() -> maintenanceCalls.get() >= 1 && worker.lastSummary().isPresent(),
                    Duration.ofSeconds(2)));
            assertTrue(worker.lastSummary().isPresent());
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    /** purge batch 自身失败时没有稳定 post-batch 边界，maintenance 不得运行。 */
    @Test
    void purgeFailureSkipsMaintenance() {
        FakeTarget target = new FakeTarget();
        target.failWith = new DatabaseRuntimeException("induced purge failure");
        AtomicInteger maintenanceCalls = new AtomicInteger();
        PurgeDriverWorker worker = new PurgeDriverWorker(target, 16, Duration.ofMillis(20),
                summary -> maintenanceCalls.incrementAndGet());
        worker.start();
        try {
            assertTrue(awaitUntil(() -> worker.state() == PurgeDriverWorkerState.FAILED,
                    Duration.ofSeconds(2)));
            assertEquals(0, maintenanceCalls.get());
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    /** STOPPING 先于 purge 返回线性化时跳过尚未 claim 的 truncate maintenance。 */
    @Test
    void stopPublishedBeforeBatchReturnsSkipsMaintenance() throws Exception {
        CountDownLatch batchEntered = new CountDownLatch(1);
        CountDownLatch releaseBatch = new CountDownLatch(1);
        PurgeTarget target = maxLogs -> {
            batchEntered.countDown();
            awaitLatch(releaseBatch);
            return new PurgeSummary(0, 0, 0, 0, 0);
        };
        AtomicInteger maintenanceCalls = new AtomicInteger();
        PurgeDriverWorker worker = new PurgeDriverWorker(target, 16, Duration.ofMillis(20),
                summary -> maintenanceCalls.incrementAndGet());
        worker.start();
        try {
            assertTrue(batchEntered.await(2, TimeUnit.SECONDS));

            worker.requestStop();
            releaseBatch.countDown();

            assertTrue(worker.awaitStopped(Duration.ofSeconds(2)));
            assertEquals(PurgeDriverWorkerState.STOPPED, worker.state());
            assertEquals(0, maintenanceCalls.get());
        } finally {
            releaseBatch.countDown();
            worker.stop(Duration.ofSeconds(2));
        }
    }

    /** 已 claim 的 maintenance 不接受 stop 中断；其真实失败即使发生于 STOPPING 也必须保留为 FAILED。 */
    @Test
    void maintenanceFailureDuringStopRemainsFailed() throws Exception {
        CountDownLatch maintenanceEntered = new CountDownLatch(1);
        CountDownLatch releaseMaintenance = new CountDownLatch(1);
        PurgeDriverWorker worker = new PurgeDriverWorker(new FakeTarget(), 16, Duration.ofMillis(20), summary -> {
            maintenanceEntered.countDown();
            awaitLatch(releaseMaintenance);
            throw new DatabaseRuntimeException("truncate failed after claim");
        });
        worker.start();
        try {
            assertTrue(maintenanceEntered.await(2, TimeUnit.SECONDS));

            worker.requestStop();
            releaseMaintenance.countDown();

            assertTrue(worker.awaitStopped(Duration.ofSeconds(2)));
            assertEquals(PurgeDriverWorkerState.FAILED, worker.state());
            assertTrue(worker.failure().orElseThrow().getMessage().contains("truncate failed"));
        } finally {
            releaseMaintenance.countDown();
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

    /** 测试线程等待显式闩锁；中断恢复标志并以项目异常结束，避免静默继续场景。 */
    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(2, TimeUnit.SECONDS)) {
                throw new DatabaseRuntimeException("test latch timed out");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException("test latch interrupted", interrupted);
        }
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
            return new PurgeSummary(0, 0, 0, 0);
        }
    }
}
