package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单线程后台 redo flusher：周期或 on-demand 驱动 {@link RedoFlushTarget#flush()}，使 {@code flushedToDiskLsn}
 * 自动前进，避免淘汰/flush 的 WAL gate 因无人驱动 flush 而长时间命中 {@code SKIPPED_REDO_NOT_DURABLE}。
 *
 * <p>形状沿用 page cleaner worker：单 daemon 线程 + {@code ReentrantLock} + {@code Condition} + 状态枚举，
 * 无 {@code synchronized}。空转跳过（无待刷不 fsync）；flush 失败即终止（FAILED），不在失败 fsync 上重试自旋。
 *
 * <p><b>并发归属</b>：{@code state}/{@code flushRequested}/{@code inFlight}/{@code lastFlushedLsn}/{@code failure}
 * 由 {@code lock} 保护；{@link RedoFlushTarget#flush()} 在锁外执行（IO 不持 worker 锁）。worker 锁与 redo 内部锁
 * 只有 worker→redo 单向获取（redo 不感知 worker），无环。
 */
public final class RedoFlushWorker implements AutoCloseable {

    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏Redo/WAL的不变量。
     */
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** redo durable 驱动端口。 */
    private final RedoFlushTarget target;
    /** 空闲等待间隔；即使无 signal 也会周期醒来检查待刷与停止条件。 */
    private final long idleWaitNanos;

    /** 保护状态、flushRequested、inFlight、lastFlushedLsn、failure。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 有新 flush 请求或停止请求时唤醒 worker。 */
    private final Condition workAvailable = lock.newCondition();
    /** 状态变为空闲/停止/失败时唤醒等待者。 */
    private final Condition idleChanged = lock.newCondition();
    /** worker 线程终止信号，stop(timeout) 用它等待线程退出。 */
    private CountDownLatch terminated = new CountDownLatch(1);

    /** 当前 worker 状态，由 lock 保护。 */
    private RedoFlushWorkerState state = RedoFlushWorkerState.NEW;
    /** 是否有合并式 on-demand flush 请求待处理，由 lock 保护。 */
    private boolean flushRequested;
    /** 是否有一轮 flush 正在锁外执行，由 lock 保护。 */
    private boolean inFlight;
    /** 后台线程对象，只在 start 创建。 */
    private Thread thread;
    /** 最近一次成功 flush 后的 durable LSN（诊断用）。 */
    private Lsn lastFlushedLsn;
    /** worker 失败根因。 */
    private DatabaseRuntimeException failure;

    /**
     * 创建 {@code RedoFlushWorker}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param target redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param idleWait 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoFlushWorker(RedoFlushTarget target, Duration idleWait) {
        if (target == null || idleWait == null) {
            throw new DatabaseValidationException("redo flush worker target/idle wait must not be null");
        }
        if (idleWait.isNegative() || idleWait.isZero()) {
            throw new DatabaseValidationException("redo flush worker idle wait must be positive: " + idleWait);
        }
        this.target = target;
        this.idleWaitNanos = timeoutNanos(idleWait);
    }

    /** 启动后台 worker。只能从 NEW 启动一次。
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void start() {
        lock.lock();
        try {
            if (state != RedoFlushWorkerState.NEW) {
                throw new DatabaseValidationException("redo flush worker can only start from NEW state: " + state);
            }
            state = RedoFlushWorkerState.IDLE;
            thread = new Thread(this::runLoop, "minimysql-redo-flusher");
            thread.setDaemon(true);
            thread.start();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 请求一次 on-demand flush（合并式）。只置位并唤醒 worker，不在调用线程做 IO；worker 已停止/失败时静默丢弃，
     * 不抛异常——nudge 不应因 worker 生命周期而让调用方（如淘汰路径）失败。
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void requestFlush() {
        lock.lock();
        try {
            if (state == RedoFlushWorkerState.NEW) {
                throw new DatabaseValidationException("redo flush worker must be started before accepting requests");
            }
            if (state == RedoFlushWorkerState.STOPPING || state == RedoFlushWorkerState.STOPPED
                    || state == RedoFlushWorkerState.FAILED) {
                return;
            }
            flushRequested = true;
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 请求 worker 停止并在 timeout 内等待线程退出。不丢弃已经开始执行的 flush。
     *
     * @param timeout 最大等待时间。
     * @return true 表示已停止；false 表示超时或中断。
     */
    public boolean stop(Duration timeout) {
        long nanos = validateWaitTimeout(timeout, "redo flush worker stop timeout");
        CountDownLatch latch;
        lock.lock();
        try {
            if (state == RedoFlushWorkerState.NEW) {
                state = RedoFlushWorkerState.STOPPED;
                terminated.countDown();
                idleChanged.signalAll();
                return true;
            }
            if (state == RedoFlushWorkerState.STOPPED) {
                return true;
            }
            if (state != RedoFlushWorkerState.FAILED) {
                state = RedoFlushWorkerState.STOPPING;
            }
            flushRequested = false;
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

    /** 当前 worker 状态。
     *
     * @return {@code state} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public RedoFlushWorkerState state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    /** 最近一次成功 flush 后的 durable LSN（诊断用）。
     *
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<Lsn> lastFlushedLsn() {
        lock.lock();
        try {
            return Optional.ofNullable(lastFlushedLsn);
        } finally {
            lock.unlock();
        }
    }

    /** worker 失败根因。
     *
     * @return 最近一次受控操作记录的失败；尚无失败时为空 {@code Optional}，参数容器与返回值均不使用 Java {@code null}
     */
    public Optional<DatabaseRuntimeException> failure() {
        lock.lock();
        try {
            return Optional.ofNullable(failure);
        } finally {
            lock.unlock();
        }
    }

    /** close 默认等待 5 秒；shutdown 路径可显式 stop(timeout) 用更短边界。 */
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
                    flushIfPending();
                    markCycleComplete();
                } catch (DatabaseRuntimeException e) {
                    markFailed(e);
                    return;
                }
            }
        } finally {
            lock.lock();
            try {
                if (state != RedoFlushWorkerState.FAILED) {
                    state = RedoFlushWorkerState.STOPPED;
                }
                inFlight = false;
                idleChanged.signalAll();
                terminated.countDown();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * 等待"有 on-demand 请求 / 周期 tick 命中待刷 / 停止"。返回 true=去做一轮 flush；false=停止退出。
     * 周期 tick 只有在确有待刷（{@link #hasPendingLocked()}）时才唤醒去工作，否则继续空闲——避免无待刷时反复
     * 翻 RUNNING 状态、也避免空转 fsync。
     */
    private boolean takeWorkOrStop() {
        lock.lock();
        try {
            while (!flushRequested && state != RedoFlushWorkerState.STOPPING) {
                state = RedoFlushWorkerState.IDLE;
                idleChanged.signalAll();
                try {
                    long remaining = workAvailable.awaitNanos(idleWaitNanos);
                    if (remaining <= 0 && !flushRequested && state != RedoFlushWorkerState.STOPPING
                            && hasPendingLocked()) {
                        inFlight = true;
                        state = RedoFlushWorkerState.RUNNING;
                        return true;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    state = RedoFlushWorkerState.STOPPING;
                    break;
                }
            }
            if (state == RedoFlushWorkerState.STOPPING) {
                return false;
            }
            flushRequested = false;
            inFlight = true;
            state = RedoFlushWorkerState.RUNNING;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 是否有待刷 redo（current > flushed）。调用须持 lock；target 查询是快速读，worker→redo 单向无环。 */
    private boolean hasPendingLocked() {
        return target.currentLsn().value() > target.flushedToDiskLsn().value();
    }

    /**
     * 推进Redo/WAL刷盘或检查点边界；写数据前遵守 WAL，失败时不得清除尚未安全持久化的状态。
     */
    private void flushIfPending() {
        // 空转保护：无待刷 redo 时不触发 fsync（on-demand 请求落到无待刷时同样跳过）。flush 在锁外执行。
        if (target.currentLsn().value() <= target.flushedToDiskLsn().value()) {
            return;
        }
        Lsn flushed = target.flush();
        lock.lock();
        try {
            lastFlushedLsn = flushed;
        } finally {
            lock.unlock();
        }
    }

    private void markCycleComplete() {
        lock.lock();
        try {
            inFlight = false;
            if (state != RedoFlushWorkerState.STOPPING) {
                state = flushRequested ? RedoFlushWorkerState.RUNNING : RedoFlushWorkerState.IDLE;
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
            flushRequested = false;
            inFlight = false;
            state = RedoFlushWorkerState.FAILED;
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
