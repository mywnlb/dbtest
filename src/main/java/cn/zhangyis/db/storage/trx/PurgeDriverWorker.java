package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单线程后台 purge driver（0.4）：周期或 on-demand 驱动 {@link PurgeTarget#runBatch}，回收已提交 undo 段、物理移除
 * 过 boundary 的 delete-marked 聚簇记录，使 purge 不依赖前台手动调用。purge boundary 由 {@code PurgeCoordinator}
 * 内部按 {@code TransactionSystem.purgeLowWaterNo} 自治判定，本 worker 只管节奏与每批上限。
 *
 * <p>形态沿用 page cleaner / redo flusher worker：单 daemon 线程 + {@code ReentrantLock} + {@code Condition} +
 * 状态枚举，无 {@code synchronized}。{@code runBatch} 无工作时是廉价 no-op（空队列 + boundary 停批），故不设空转跳过。
 * runBatch 抛领域异常即 FAILED 停机，不重试自旋。
 *
 * <p><b>并发归属</b>：{@code state}/{@code purgeRequested}/{@code inFlight}/{@code lastSummary}/{@code failure} 由
 * {@code lock} 保护；{@link PurgeTarget#runBatch} 在锁外执行（其内部 MTR/页 latch 不嵌套 worker 锁）。
 */
public final class PurgeDriverWorker implements AutoCloseable {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** purge 驱动端口。 */
    private final PurgeTarget target;
    /** 每批最多处理的 committed undo log 数。 */
    private final int maxPerBatch;
    /** 空闲等待间隔；即使无 signal 也周期醒来跑一批并检查停止。 */
    private final long idleWaitNanos;

    /** 保护状态、purgeRequested、inFlight、lastSummary、failure。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 有 on-demand 请求或停止请求时唤醒 worker。 */
    private final Condition workAvailable = lock.newCondition();
    /** 状态变为空闲/停止/失败时唤醒等待者。 */
    private final Condition idleChanged = lock.newCondition();
    /** worker 线程终止信号。 */
    private CountDownLatch terminated = new CountDownLatch(1);

    private PurgeDriverWorkerState state = PurgeDriverWorkerState.NEW;
    private boolean purgeRequested;
    private boolean inFlight;
    private Thread thread;
    private PurgeSummary lastSummary;
    private DatabaseRuntimeException failure;

    public PurgeDriverWorker(PurgeTarget target, int maxPerBatch, Duration idleWait) {
        if (target == null || idleWait == null) {
            throw new DatabaseValidationException("purge driver target/idle wait must not be null");
        }
        if (maxPerBatch <= 0) {
            throw new DatabaseValidationException("purge driver maxPerBatch must be positive: " + maxPerBatch);
        }
        if (idleWait.isNegative() || idleWait.isZero()) {
            throw new DatabaseValidationException("purge driver idle wait must be positive: " + idleWait);
        }
        this.target = target;
        this.maxPerBatch = maxPerBatch;
        this.idleWaitNanos = timeoutNanos(idleWait);
    }

    /** 启动后台 worker。只能从 NEW 启动一次。 */
    public void start() {
        lock.lock();
        try {
            if (state != PurgeDriverWorkerState.NEW) {
                throw new DatabaseValidationException("purge driver can only start from NEW state: " + state);
            }
            state = PurgeDriverWorkerState.IDLE;
            thread = new Thread(this::runLoop, "minimysql-purge-driver");
            thread.setDaemon(true);
            thread.start();
        } finally {
            lock.unlock();
        }
    }

    /** 请求一次 on-demand purge（合并式）；worker 已停止/失败时静默丢弃。 */
    public void requestPurge() {
        lock.lock();
        try {
            if (state == PurgeDriverWorkerState.NEW) {
                throw new DatabaseValidationException("purge driver must be started before accepting requests");
            }
            if (state == PurgeDriverWorkerState.STOPPING || state == PurgeDriverWorkerState.STOPPED
                    || state == PurgeDriverWorkerState.FAILED) {
                return;
            }
            purgeRequested = true;
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 请求停止并在 timeout 内等待线程退出。不丢弃已开始的批次。
     *
     * @param timeout 最大等待时间。
     * @return true 表示已停止。
     */
    public boolean stop(Duration timeout) {
        long nanos = validateWaitTimeout(timeout, "purge driver stop timeout");
        CountDownLatch latch;
        lock.lock();
        try {
            if (state == PurgeDriverWorkerState.NEW) {
                state = PurgeDriverWorkerState.STOPPED;
                terminated.countDown();
                idleChanged.signalAll();
                return true;
            }
            if (state == PurgeDriverWorkerState.STOPPED) {
                return true;
            }
            if (state != PurgeDriverWorkerState.FAILED) {
                state = PurgeDriverWorkerState.STOPPING;
            }
            purgeRequested = false;
            workAvailable.signalAll();
            latch = terminated;
        } finally {
            lock.unlock();
        }
        try {
            return latch.await(nanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** 当前 worker 状态。 */
    public PurgeDriverWorkerState state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    /** 最近一批 purge 统计（诊断用）。 */
    public Optional<PurgeSummary> lastSummary() {
        lock.lock();
        try {
            return Optional.ofNullable(lastSummary);
        } finally {
            lock.unlock();
        }
    }

    /** worker 失败根因。 */
    public Optional<DatabaseRuntimeException> failure() {
        lock.lock();
        try {
            return Optional.ofNullable(failure);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        stop(Duration.ofSeconds(5));
    }

    private void runLoop() {
        try {
            while (true) {
                if (!takeWorkOrStop()) {
                    return;
                }
                try {
                    PurgeSummary summary = target.runBatch(maxPerBatch);
                    markCycleComplete(summary);
                } catch (DatabaseRuntimeException e) {
                    markFailed(e);
                    return;
                }
            }
        } finally {
            lock.lock();
            try {
                if (state != PurgeDriverWorkerState.FAILED) {
                    state = PurgeDriverWorkerState.STOPPED;
                }
                inFlight = false;
                idleChanged.signalAll();
                terminated.countDown();
            } finally {
                lock.unlock();
            }
        }
    }

    /** 等待"on-demand 请求 / 周期 tick / 停止"。返回 true=跑一批；false=停止退出。 */
    private boolean takeWorkOrStop() {
        lock.lock();
        try {
            while (!purgeRequested && state != PurgeDriverWorkerState.STOPPING) {
                state = PurgeDriverWorkerState.IDLE;
                idleChanged.signalAll();
                try {
                    long remaining = workAvailable.awaitNanos(idleWaitNanos);
                    if (remaining <= 0 && !purgeRequested && state != PurgeDriverWorkerState.STOPPING) {
                        inFlight = true;
                        state = PurgeDriverWorkerState.RUNNING;
                        return true;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    state = PurgeDriverWorkerState.STOPPING;
                    break;
                }
            }
            if (state == PurgeDriverWorkerState.STOPPING) {
                return false;
            }
            purgeRequested = false;
            inFlight = true;
            state = PurgeDriverWorkerState.RUNNING;
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void markCycleComplete(PurgeSummary summary) {
        lock.lock();
        try {
            lastSummary = summary;
            inFlight = false;
            if (state != PurgeDriverWorkerState.STOPPING) {
                state = purgeRequested ? PurgeDriverWorkerState.RUNNING : PurgeDriverWorkerState.IDLE;
            }
            idleChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void markFailed(DatabaseRuntimeException error) {
        lock.lock();
        try {
            failure = error;
            purgeRequested = false;
            inFlight = false;
            state = PurgeDriverWorkerState.FAILED;
            idleChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private static long validateWaitTimeout(Duration timeout, String fieldName) {
        if (timeout == null) {
            throw new DatabaseValidationException(fieldName + " must not be null");
        }
        if (timeout.isNegative()) {
            throw new DatabaseValidationException(fieldName + " must not be negative: " + timeout);
        }
        return timeoutNanos(timeout);
    }

    private static long timeoutNanos(Duration timeout) {
        long seconds = timeout.getSeconds();
        int nanos = timeout.getNano();
        if (seconds > Long.MAX_VALUE / NANOS_PER_SECOND) {
            return Long.MAX_VALUE;
        }
        long base = seconds * NANOS_PER_SECOND;
        if (Long.MAX_VALUE - base < nanos) {
            return Long.MAX_VALUE;
        }
        return base + nanos;
    }
}
