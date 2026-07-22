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

    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏事务、MVCC 与锁的不变量。
     */
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

    /**
     * 本对象的权威状态机字段 {@code state}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
     */
    private PurgeDriverWorkerState state = PurgeDriverWorkerState.NEW;
    /**
     * 记录 {@code purgeRequested} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private boolean purgeRequested;
    /**
     * 记录 {@code inFlight} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private boolean inFlight;
    /**
     * 本对象拥有的后台工作线程；启动、停止与引用清理必须由生命周期锁协调，关闭后不得残留存活线程。
     */
    private Thread thread;
    /**
     * 最近一次后台周期或恢复步骤的可观测结果 {@code lastSummary}；由执行线程发布，诊断读取不得清除原始失败或覆盖更新顺序。
     */
    private PurgeSummary lastSummary;
    /**
     * 最近一次后台周期或恢复步骤的可观测结果 {@code failure}；由执行线程发布，诊断读取不得清除原始失败或覆盖更新顺序。
     */
    private DatabaseRuntimeException failure;

    /**
     * 创建 {@code PurgeDriverWorker}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param target 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param maxPerBatch 参与 {@code 构造} 的上界或规格值 {@code maxPerBatch}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param idleWait 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public PurgeDriverWorker(PurgeTarget target, int maxPerBatch, Duration idleWait) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (target == null || idleWait == null) {
            throw new DatabaseValidationException("purge driver target/idle wait must not be null");
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        if (maxPerBatch <= 0) {
            throw new DatabaseValidationException("purge driver maxPerBatch must be positive: " + maxPerBatch);
        }
        if (idleWait.isNegative() || idleWait.isZero()) {
            throw new DatabaseValidationException("purge driver idle wait must be positive: " + idleWait);
        }
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.target = target;
        this.maxPerBatch = maxPerBatch;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.idleWaitNanos = timeoutNanos(idleWait);
    }

    /** 启动后台 worker。只能从 NEW 启动一次。
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** 请求一次 on-demand purge（合并式）；worker 已停止/失败时静默丢弃。
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
        requestStop();
        return awaitStopped(timeout);
    }

    /**
     * 非阻塞请求 driver 停止。组合根先调用本方法，再同时停止 coordinator 内部 worker pool，避免 driver 在
     * {@link PurgeTarget#runBatch(int)} 中等待而内部 worker 尚未收到取消。
     */
    public void requestStop() {
        lock.lock();
        try {
            if (state == PurgeDriverWorkerState.NEW) {
                state = PurgeDriverWorkerState.STOPPED;
                terminated.countDown();
                idleChanged.signalAll();
                return;
            }
            if (state == PurgeDriverWorkerState.STOPPED || state == PurgeDriverWorkerState.FAILED) {
                return;
            }
            state = PurgeDriverWorkerState.STOPPING;
            purgeRequested = false;
            workAvailable.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 有界等待已请求停止的 driver 线程退出；本方法不重新发停止信号，便于组合根共享一个绝对 deadline。
     *
     * @param timeout 当前关闭 deadline 的剩余非负预算
     * @return 线程已终止时为 true，超时或调用线程中断时为 false
     */
    public boolean awaitStopped(Duration timeout) {
        long nanos = validateWaitTimeout(timeout, "purge driver stop timeout");
        CountDownLatch latch;
        lock.lock();
        try {
            if (state == PurgeDriverWorkerState.NEW) {
                throw new DatabaseValidationException("purge driver stop must be requested before await");
            }
            if (state == PurgeDriverWorkerState.STOPPED) {
                return true;
            }
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
    public PurgeDriverWorkerState state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    /** 最近一批 purge 统计（诊断用）。
     *
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<PurgeSummary> lastSummary() {
        lock.lock();
        try {
            return Optional.ofNullable(lastSummary);
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

    /**
     * 释放本方法拥有的事务、MVCC 与锁资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
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
                    markFailedUnlessStopping(e);
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

    /** shutdown 已先发布 STOPPING 时，target 的取消异常属于正常退出，不得覆盖为业务 FAILED。 */
    private void markFailedUnlessStopping(DatabaseRuntimeException error) {
        lock.lock();
        try {
            if (state == PurgeDriverWorkerState.STOPPING) {
                inFlight = false;
                idleChanged.signalAll();
                return;
            }
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
