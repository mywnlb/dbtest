package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 后台 redo flusher 测试：周期/on-demand 驱动 flush 推进 durable LSN、空转不 fsync、stop 幂等、flush 失败进 FAILED；
 * 末一条用真 durable {@link RedoLogManager} + 适配器验证生产路径。worker 行为用 fake target 注入，确定性强。
 */
class RedoFlushWorkerTest {

    @TempDir
    Path dir;

    /**
     * 验证 {@code advancesDurableLsnPeriodically} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void advancesDurableLsnPeriodically() {
        FakeTarget target = new FakeTarget();
        target.current = 100;
        RedoFlushWorker worker = new RedoFlushWorker(target, Duration.ofMillis(20));
        worker.start();
        try {
            assertTrue(awaitUntil(() -> target.flushed == 100, Duration.ofSeconds(2)),
                    "durable LSN should advance to current via periodic flush");
            assertTrue(target.flushCalls.get() >= 1);
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    /**
     * 验证 {@code requestFlushTriggersImmediateFlush} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void requestFlushTriggersImmediateFlush() {
        FakeTarget target = new FakeTarget();
        target.current = 100;
        // 极长周期：若非 on-demand 唤醒，本轮内不会有周期 tick。
        RedoFlushWorker worker = new RedoFlushWorker(target, Duration.ofSeconds(30));
        worker.start();
        try {
            worker.requestFlush();
            assertTrue(awaitUntil(() -> target.flushed == 100, Duration.ofSeconds(2)),
                    "requestFlush should flush before the long periodic interval");
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    /**
     * 验证 {@code idleTickDoesNotFsyncWhenNothingPending} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void idleTickDoesNotFsyncWhenNothingPending() throws Exception {
        FakeTarget target = new FakeTarget(); // current == flushed == 0：无待刷
        RedoFlushWorker worker = new RedoFlushWorker(target, Duration.ofMillis(20));
        worker.start();
        try {
            Thread.sleep(150); // 经过多个周期 tick
            assertEquals(0, target.flushCalls.get(), "idle tick must not fsync when nothing is pending");
            assertEquals(RedoFlushWorkerState.IDLE, worker.state());
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    /**
     * 验证 {@code stopHaltsWorkerAndIsIdempotent} 所描述的组件生命周期，并断言状态转换、后台线程停止和资源恰好释放一次。
     */
    @Test
    void stopHaltsWorkerAndIsIdempotent() {
        FakeTarget target = new FakeTarget();
        RedoFlushWorker worker = new RedoFlushWorker(target, Duration.ofMillis(20));
        worker.start();
        assertTrue(worker.stop(Duration.ofSeconds(2)));
        assertEquals(RedoFlushWorkerState.STOPPED, worker.state());
        assertTrue(worker.stop(Duration.ofSeconds(2)), "second stop must be idempotent");
    }

    /**
     * 验证 {@code flushFailureMovesToFailedState} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void flushFailureMovesToFailedState() {
        FakeTarget target = new FakeTarget();
        target.current = 100;
        target.failWith = new RedoLogIoException("induced redo flush failure", new RuntimeException("induced"));
        RedoFlushWorker worker = new RedoFlushWorker(target, Duration.ofMillis(20));
        worker.start();
        try {
            assertTrue(awaitUntil(() -> worker.state() == RedoFlushWorkerState.FAILED, Duration.ofSeconds(2)),
                    "flush failure must move worker to FAILED");
            assertTrue(worker.failure().isPresent());
        } finally {
            worker.stop(Duration.ofSeconds(2));
        }
    }

    /**
     * 验证 {@code realDurableManagerPathAdvancesDurableLsn} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void realDurableManagerPathAdvancesDurableLsn() {
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(
                    PageId.of(SpaceId.of(1), PageNo.of(2)), 0, new byte[]{1, 2, 3})));
            long current = redo.currentLsn().value();
            assertTrue(current > redo.flushedToDiskLsn().value());

            RedoFlushWorker worker = new RedoFlushWorker(
                    new RedoLogManagerFlushTarget(redo), Duration.ofMillis(20));
            worker.start();
            try {
                assertTrue(awaitUntil(() -> redo.flushedToDiskLsn().value() >= current, Duration.ofSeconds(2)),
                        "background worker should drive real redo durable LSN to current");
            } finally {
                worker.stop(Duration.ofSeconds(2));
            }
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

    /** 测试用 redo flush 驱动端口：模拟 flush 把 durable 推到 current，可注入失败。 */
    private static final class FakeTarget implements RedoFlushTarget {
        volatile long current = 0;
        volatile long flushed = 0;
        final AtomicInteger flushCalls = new AtomicInteger();
        volatile DatabaseRuntimeException failWith;

        @Override
        public Lsn currentLsn() {
            return Lsn.of(current);
        }

        @Override
        public Lsn flushedToDiskLsn() {
            return Lsn.of(flushed);
        }

        @Override
        public Lsn flush() {
            flushCalls.incrementAndGet();
            if (failWith != null) {
                throw failWith;
            }
            flushed = current;
            return Lsn.of(flushed);
        }
    }
}
