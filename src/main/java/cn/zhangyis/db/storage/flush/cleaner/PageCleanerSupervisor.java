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

    public PageCleanerSupervisor(PageCleanerWorkerFactory factory, int maxRestarts,
                                 Duration restartBackoff, Duration monitorInterval) {
        if (factory == null || restartBackoff == null || monitorInterval == null) {
            throw new DatabaseValidationException("page cleaner supervisor dependencies must not be null");
        }
        if (maxRestarts < 0) {
            throw new DatabaseValidationException("page cleaner max restarts must not be negative: " + maxRestarts);
        }
        if (restartBackoff.isNegative() || monitorInterval.isNegative() || monitorInterval.isZero()) {
            throw new DatabaseValidationException("page cleaner supervisor durations are invalid");
        }
        this.factory = factory;
        this.maxRestarts = maxRestarts;
        this.restartBackoff = restartBackoff;
        this.monitorInterval = monitorInterval;
    }

    /** 启动 supervisor 及其当前 worker。只能从 NEW 状态调用一次。 */
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

    /** 转发显式 flush 请求到当前 worker。 */
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

    /** 等待当前 worker 空闲。 */
    public boolean awaitIdle(Duration timeout) {
        return currentWorker().awaitIdle(timeout);
    }

    /** 等待 supervisor 进入指定状态；仅用于测试和诊断，不改变后台线程状态。 */
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

    /** 当前 supervisor metrics 快照。 */
    public PageCleanerMetricsSnapshot metricsSnapshot() {
        lock.lock();
        try {
            PageCleanerWorkerSnapshot workerSnapshot = worker == null
                    ? new PageCleanerWorkerSnapshot(state, false, 0, 0, false, lastErrorMessage)
                    : worker.snapshot();
            recordSuccessfulCyclesLocked(workerSnapshot);
            state = workerSnapshot.state();
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

    /** 停止当前 worker。 */
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

    private void recordSuccessfulCycles(PageCleanerWorkerSnapshot snapshot) {
        lock.lock();
        try {
            recordSuccessfulCyclesLocked(snapshot);
        } finally {
            lock.unlock();
        }
    }

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
