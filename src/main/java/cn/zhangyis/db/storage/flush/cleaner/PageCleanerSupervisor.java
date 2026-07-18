package cn.zhangyis.db.storage.flush.cleaner;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.flush.FlushCycleResult;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Page cleaner worker 的轻量 supervisor。
 *
 * <p>第一阶段先收敛 worker 生命周期与 metrics 入口：supervisor 创建、启动、停止当前 worker，并把当前 worker 的
 * completed cycle 快照汇总成跨 worker metrics。后续失败重启在同一对象中扩展，worker 本身仍只负责执行 flush cycle。
 */
public final class PageCleanerSupervisor implements AutoCloseable {

    /** 创建 NEW worker 的工厂；supervisor 每次重启都通过它获得全新 worker，避免复用 FAILED 状态对象。 */
    private final PageCleanerWorkerFactory factory;
    /** 单个 supervisor 生命周期允许的最大重启次数；超过后进入 FAILED，防止后台线程失败后无限自旋。 */
    private final int maxRestarts;
    /** worker 失败到下一次启动之间的固定退避；第一阶段不做指数退避或动态调参。 */
    private final Duration restartBackoff;
    /** monitor loop 周期；只用于观察 worker snapshot，不参与 flush 调度。 */
    private final Duration monitorInterval;
    /** 保护当前 worker 引用、supervisor 状态和跨 worker metrics。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** supervisor 状态变化通知；测试/诊断等待用，不保护 worker 内部状态。 */
    private final Condition stateChanged = lock.newCondition();

    /** 当前被托管的 worker；只在 lock 下替换，执行 flush 的线程仍归 worker 自己管理。 */
    private PageCleanerWorkerHandle worker;
    /** supervisor 视角状态；正常情况下跟随当前 worker，超限失败/停止时由 supervisor 发布。 */
    private PageCleanerState state = PageCleanerState.NEW;
    /** 已执行的重启次数；跨 worker 生命周期累计。 */
    private int restartCount;
    /** 已确认的成功 cycle 总数；monitor 按 worker completedCycles 差值累计，避免重复计数。 */
    private long successfulCycles;
    /** 已确认的失败 cycle 总数；每次观察到 worker FAILED 计一次。 */
    private long failedCycles;
    /** 当前 worker 已被 supervisor 计入 metrics 的 completedCycles 水位。 */
    private long observedWorkerCycles;
    /** 最近失败消息；无失败时为空字符串，避免 snapshot 暴露 null。 */
    private String lastErrorMessage = "";
    /** 最近一次 worker 启动时间，epoch millis；未启动时为 0。 */
    private long lastStartedAtMillis;
    /** 最近一次 supervisor 停止时间，epoch millis；未停止时为 0。 */
    private long lastStoppedAtMillis;
    /** stop/FAILED 超限发布后置 true，monitor loop 观察到后退出。 */
    private boolean stopping;
    /** supervisor 自己的轻量监控线程，只读取 worker snapshot 并执行重启策略，不执行 flush IO。 */
    private Thread monitorThread;
    /** monitor 线程终止信号，stop(timeout) 用它避免后台监控线程泄漏。 */
    private CountDownLatch monitorTerminated = new CountDownLatch(1);

    /**
     * 创建 {@code PageCleanerSupervisor}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param factory 由组合根提供的 {@code PageCleanerWorkerFactory} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param maxRestarts 参与 {@code 构造} 的上界或规格值 {@code maxRestarts}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param restartBackoff 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @param monitorInterval 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public PageCleanerSupervisor(PageCleanerWorkerFactory factory, int maxRestarts,
                                 Duration restartBackoff, Duration monitorInterval) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (factory == null || restartBackoff == null || monitorInterval == null) {
            throw new DatabaseValidationException("page cleaner supervisor dependencies must not be null");
        }
        if (maxRestarts < 0) {
            throw new DatabaseValidationException("page cleaner max restarts must not be negative: " + maxRestarts);
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        if (restartBackoff.isNegative() || monitorInterval.isNegative() || monitorInterval.isZero()) {
            throw new DatabaseValidationException("page cleaner supervisor durations are invalid");
        }
        this.factory = factory;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.maxRestarts = maxRestarts;
        this.restartBackoff = restartBackoff;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.monitorInterval = monitorInterval;
    }

    /** 启动 supervisor 及其当前 worker。只能从 NEW 状态调用一次。
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void start() {
        lock.lock();
        try {
            if (state != PageCleanerState.NEW) {
                throw new DatabaseValidationException("page cleaner supervisor can only start from NEW: " + state);
            }
            worker = factory.create();
            worker.start();
            state = worker.state();
            lastStartedAtMillis = System.currentTimeMillis();
            monitorThread = new Thread(this::monitorLoop, "minimysql-page-cleaner-supervisor");
            monitorThread.setDaemon(true);
            monitorThread.start();
        } finally {
            lock.unlock();
        }
    }

    /** 转发显式 flush 请求到当前 worker。
     *
     * @param maxPages 参与 {@code requestFlush} 的上界或规格值 {@code maxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @throws PageCleanerStoppedException 后台刷脏工作线程已经停止或无法继续服务时抛出；监督者应停止派发并关闭或重启对应 worker
     */
    public void requestFlush(int maxPages) {
        PageCleanerState current = state();
        if (current == PageCleanerState.FAILED || current == PageCleanerState.STOPPING
                || current == PageCleanerState.STOPPED) {
            throw new PageCleanerStoppedException("page cleaner supervisor is not accepting requests in state: "
                    + current);
        }
        currentWorker().requestFlush(maxPages);
        signalStateChanged();
    }

    /** 等待当前 worker 空闲。
     *
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @return 在超时或取消前观察到 {@code awaitIdle} 的目标状态时为 {@code true}；等待期限届满且状态仍未满足时为 {@code false}
     */
    public boolean awaitIdle(Duration timeout) {
        return currentWorker().awaitIdle(timeout);
    }

    /** 等待 supervisor 进入指定状态；仅用于测试和诊断，不改变后台线程状态。
     *
     * @param expected 选择 {@code awaitState} 分支的 {@code PageCleanerState} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @return 在超时或取消前观察到 {@code awaitState} 的目标状态时为 {@code true}；等待期限届满且状态仍未满足时为 {@code false}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public boolean awaitState(PageCleanerState expected, Duration timeout) {
        if (expected == null || timeout == null) {
            throw new DatabaseValidationException("page cleaner await state arguments must not be null");
        }
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("page cleaner await state timeout is too large", overflow);
        }
        lock.lock();
        try {
            while (state != expected) {
                if (nanos <= 0) {
                    return false;
                }
                try {
                    nanos = stateChanged.awaitNanos(nanos);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 当前 supervisor metrics 快照。
     *
     * @return {@code metricsSnapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public PageCleanerMetricsSnapshot metricsSnapshot() {
        lock.lock();
        try {
            PageCleanerWorkerSnapshot workerSnapshot = worker == null
                    ? new PageCleanerWorkerSnapshot(state, false, 0, 0, false, lastErrorMessage)
                    : worker.snapshot();
            recordSuccessfulCyclesLocked(workerSnapshot);
            // FAILED 可能表示 supervisor 正在替换已经 STOPPED 的旧 worker；STOPPING/STOPPED 则是 supervisor
            // 自己发布的终态。这三种状态都比 worker 快照权威，不能被旧 worker 的 STOPPED 或尚未停下的 IDLE
            // 反向覆盖，否则等待方会把旧状态误认成 replacement 已经发布，甚至在 close 期间重新放行请求。
            if (state != PageCleanerState.FAILED
                    && state != PageCleanerState.STOPPING
                    && state != PageCleanerState.STOPPED) {
                state = workerSnapshot.state();
            }
            boolean lastCyclePresent = successfulCycles > 0 || workerSnapshot.lastCyclePresent();
            return new PageCleanerMetricsSnapshot(state, restartCount, successfulCycles,
                    failedCycles, lastCyclePresent, lastErrorMessage,
                    lastStartedAtMillis, lastStoppedAtMillis);
        } finally {
            lock.unlock();
        }
    }

    /** 最近一轮后台 flush/checkpoint cycle。 */
    public Optional<FlushCycleResult> lastCycle() {
        return currentWorker().lastCycle();
    }

    /** 当前 supervisor 状态。 */
    public PageCleanerState state() {
        return metricsSnapshot().state();
    }

    /** 停止当前 worker。
     *
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code stop} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     */
    public boolean stop(Duration timeout) {
        PageCleanerWorkerHandle current;
        lock.lock();
        try {
            if (state == PageCleanerState.NEW) {
                state = PageCleanerState.STOPPED;
                lastStoppedAtMillis = System.currentTimeMillis();
                return true;
            }
            stopping = true;
            state = PageCleanerState.STOPPING;
            stateChanged.signalAll();
            current = worker;
        } finally {
            lock.unlock();
        }
        boolean stopped = current == null || current.stop(timeout);
        awaitMonitor(timeout);
        lock.lock();
        try {
            state = stopped ? PageCleanerState.STOPPED : current.state();
            if (stopped) {
                lastStoppedAtMillis = System.currentTimeMillis();
            }
            return stopped;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放本方法拥有的脏页刷盘与 checkpoint资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    public void close() {
        stop(Duration.ofSeconds(5));
    }

    private PageCleanerWorkerHandle currentWorker() {
        lock.lock();
        try {
            if (worker == null) {
                throw new DatabaseRuntimeException("page cleaner supervisor has no active worker");
            }
            return worker;
        } finally {
            lock.unlock();
        }
    }

    private void monitorLoop() {
        try {
            while (true) {
                PageCleanerWorkerHandle current;
                lock.lock();
                try {
                    if (stopping) {
                        return;
                    }
                    current = worker;
                } finally {
                    lock.unlock();
                }
                PageCleanerWorkerSnapshot snapshot = current.snapshot();
                if (snapshot.state() == PageCleanerState.FAILED) {
                    handleFailedWorker(snapshot);
                } else {
                    recordSuccessfulCycles(snapshot);
                    sleepMonitorInterval();
                }
            }
        } finally {
            monitorTerminated.countDown();
        }
    }

    /**
     * 收敛失败 worker 并在预算允许时安装 replacement。
     *
     * <p>数据流：monitor 提供失败快照；本方法先在 supervisor 锁内累计 metrics，并发布 FAILED 作为重启过渡态，
     * 再在锁外停止旧 worker、执行有界 backoff 和启动 replacement，最后回到锁内一次发布新 worker 与其状态。
     * FAILED 必须先于锁外 stop 发布，否则旧 IDLE 会让 {@link #awaitState(PageCleanerState, Duration)} 提前成功；
     * replacement 发布前也不接受 flush 请求，避免请求落到已停止的旧 worker。
     */
    private void handleFailedWorker(PageCleanerWorkerSnapshot snapshot) {
        lock.lock();
        try {
            if (stopping) {
                return;
            }
            recordSuccessfulCyclesLocked(snapshot);
            failedCycles++;
            lastErrorMessage = snapshot.failureMessage();
            if (restartCount >= maxRestarts) {
                state = PageCleanerState.FAILED;
                stateChanged.signalAll();
                stopping = true;
                return;
            }
            restartCount++;
            observedWorkerCycles = 0;
            state = PageCleanerState.FAILED;
            stateChanged.signalAll();
        } finally {
            lock.unlock();
        }
        worker.stop(restartBackoff.isZero() ? Duration.ZERO : restartBackoff);
        sleepRestartBackoff();
        PageCleanerWorkerHandle replacement = factory.create();
        replacement.start();
        lock.lock();
        try {
            worker = replacement;
            state = replacement.state();
            lastStartedAtMillis = System.currentTimeMillis();
            stateChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在 supervisor lock 内吸收 worker 的累计完成周期，只把相对上次观测新增的正差值计入监督指标。
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    private void recordSuccessfulCycles(PageCleanerWorkerSnapshot snapshot) {
        lock.lock();
        try {
            recordSuccessfulCyclesLocked(snapshot);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 按脏页刷盘与 checkpoint并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    private void recordSuccessfulCyclesLocked(PageCleanerWorkerSnapshot snapshot) {
        long delta = snapshot.completedCycles() - observedWorkerCycles;
        if (delta > 0) {
            successfulCycles += delta;
            observedWorkerCycles = snapshot.completedCycles();
        }
    }

    private void signalStateChanged() {
        lock.lock();
        try {
            stateChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void sleepMonitorInterval() {
        lock.lock();
        try {
            stateChanged.await(monitorInterval.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            stopping = true;
        } finally {
            lock.unlock();
        }
    }

    private void sleepRestartBackoff() {
        if (restartBackoff.isZero()) {
            return;
        }
        try {
            TimeUnit.NANOSECONDS.sleep(restartBackoff.toNanos());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            lock.lock();
            try {
                stopping = true;
            } finally {
                lock.unlock();
            }
        }
    }

    private boolean awaitMonitor(Duration timeout) {
        try {
            return monitorTerminated.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
