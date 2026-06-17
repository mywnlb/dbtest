package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单线程 page cleaner worker。它只接收 flush request 并调用 {@link FlushService#flushForCapacity(int)}，
 * 不直接访问 Buffer Pool frame 或 PageStore，因此不会在后台线程中引入新的页锁顺序。
 */
public final class PageCleanerWorker implements AutoCloseable {

    /** 每秒纳秒数，用于 Duration 转换并避免溢出。 */
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** flush service 门面，执行实际 capacity flush cycle。 */
    private final FlushService flushService;
    /** 请求队列容量，防止后台 worker 停滞时无限堆积请求。 */
    private final int queueCapacity;
    /** 空闲等待间隔；即使没有 signal，也会周期性醒来检查停止条件。 */
    private final long idleWaitNanos;
    /** 保护状态、队列、lastCycle 和 failure。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 有新请求或停止请求时唤醒 worker。 */
    private final Condition workAvailable = lock.newCondition();
    /** 状态变为空闲/停止/失败时唤醒等待者。 */
    private final Condition idleChanged = lock.newCondition();
    /** 有界 flush 请求队列，元素为本轮 maxPages。 */
    private final Deque<Integer> requests = new ArrayDeque<>();
    /** worker 线程终止信号，stop(timeout) 用它等待线程退出。 */
    private CountDownLatch terminated = new CountDownLatch(1);

    /** 当前 worker 状态，由 lock 保护。 */
    private PageCleanerState state = PageCleanerState.NEW;
    /** 是否有一轮 flush cycle 正在锁外执行，由 lock 保护。 */
    private boolean inFlight;
    /** 后台线程对象，只在 start 创建。 */
    private Thread thread;
    /** 最近一轮 flush cycle 结果，用于诊断和测试。 */
    private FlushCycleResult lastCycle;
    /** worker 失败根因。 */
    private DatabaseRuntimeException failure;

    public PageCleanerWorker(FlushService flushService, int queueCapacity, Duration idleWait) {
        if (flushService == null || idleWait == null) {
            throw new DatabaseValidationException("page cleaner service/idle wait must not be null");
        }
        if (queueCapacity < 1) {
            throw new DatabaseValidationException("page cleaner queue capacity must be >= 1: " + queueCapacity);
        }
        if (idleWait.isNegative() || idleWait.isZero()) {
            throw new DatabaseValidationException("page cleaner idle wait must be positive: " + idleWait);
        }
        this.flushService = flushService;
        this.queueCapacity = queueCapacity;
        this.idleWaitNanos = timeoutNanos(idleWait);
    }

    /** 启动后台 worker。只能从 NEW 状态启动一次。 */
    public void start() {
        lock.lock();
        try {
            if (state != PageCleanerState.NEW) {
                throw new DatabaseValidationException("page cleaner can only start from NEW state: " + state);
            }
            state = PageCleanerState.IDLE;
            thread = new Thread(this::runLoop, "minimysql-page-cleaner");
            thread.setDaemon(true);
            thread.start();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交一次 capacity flush 请求。该方法只入队并唤醒 worker，不在调用线程执行 IO。
     *
     * @param maxPages 本轮最多刷页数。
     */
    public void requestFlush(int maxPages) {
        if (maxPages < 0) {
            throw new DatabaseValidationException("page cleaner request max pages must not be negative: " + maxPages);
        }
        lock.lock();
        try {
            if (state == PageCleanerState.NEW) {
                throw new DatabaseValidationException("page cleaner must be started before accepting requests");
            }
            if (state == PageCleanerState.STOPPING || state == PageCleanerState.STOPPED
                    || state == PageCleanerState.FAILED) {
                throw new PageCleanerStoppedException("page cleaner is not accepting requests in state: " + state);
            }
            if (requests.size() >= queueCapacity) {
                throw new FlushWriteException("page cleaner request queue is full: capacity=" + queueCapacity);
            }
            requests.addLast(maxPages);
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待 worker 进入空闲或终止状态。
     *
     * @param timeout 最大等待时间。
     * @return true 表示已空闲/已停止；false 表示超时或中断。
     */
    public boolean awaitIdle(Duration timeout) {
        long nanos = validateWaitTimeout(timeout, "page cleaner await idle timeout");
        lock.lock();
        try {
            while (!isIdleOrStopped()) {
                if (nanos <= 0) {
                    return false;
                }
                try {
                    nanos = idleChanged.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 请求 worker 停止，并在 timeout 内等待线程退出。该方法不会丢弃已经开始执行的 flush cycle。
     *
     * @param timeout 最大等待时间。
     * @return true 表示 worker 已停止；false 表示等待超时或中断。
     */
    public boolean stop(Duration timeout) {
        long nanos = validateWaitTimeout(timeout, "page cleaner stop timeout");
        CountDownLatch latch;
        lock.lock();
        try {
            if (state == PageCleanerState.NEW) {
                state = PageCleanerState.STOPPED;
                terminated.countDown();
                idleChanged.signalAll();
                return true;
            }
            if (state == PageCleanerState.STOPPED) {
                return true;
            }
            if (state != PageCleanerState.FAILED) {
                state = PageCleanerState.STOPPING;
            }
            requests.clear();
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
    public PageCleanerState state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    /** 最近一轮成功执行的 flush cycle。 */
    public Optional<FlushCycleResult> lastCycle() {
        lock.lock();
        try {
            return Optional.ofNullable(lastCycle);
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

    /** close 默认等待 5 秒，测试和生产 shutdown 可显式调用 stop(timeout) 使用更短边界。 */
    @Override
    public void close() {
        stop(Duration.ofSeconds(5));
    }

    private void runLoop() {
        try {
            while (true) {
                Integer request = takeRequestOrStop();
                if (request == null) {
                    return;
                }
                try {
                    FlushCycleResult cycle = flushService.flushForCapacity(request);
                    markCycleComplete(cycle);
                } catch (DatabaseRuntimeException e) {
                    markFailed(e);
                    return;
                }
            }
        } finally {
            lock.lock();
            try {
                if (state != PageCleanerState.FAILED) {
                    state = PageCleanerState.STOPPED;
                }
                inFlight = false;
                idleChanged.signalAll();
                terminated.countDown();
            } finally {
                lock.unlock();
            }
        }
    }

    private Integer takeRequestOrStop() {
        lock.lock();
        try {
            while (requests.isEmpty() && state != PageCleanerState.STOPPING) {
                state = PageCleanerState.IDLE;
                idleChanged.signalAll();
                try {
                    workAvailable.awaitNanos(idleWaitNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    state = PageCleanerState.STOPPING;
                    break;
                }
            }
            if (state == PageCleanerState.STOPPING && requests.isEmpty()) {
                return null;
            }
            Integer request = requests.pollFirst();
            if (request == null) {
                return null;
            }
            inFlight = true;
            state = PageCleanerState.RUNNING;
            return request;
        } finally {
            lock.unlock();
        }
    }

    private void markCycleComplete(FlushCycleResult cycle) {
        lock.lock();
        try {
            lastCycle = cycle;
            inFlight = false;
            if (state != PageCleanerState.STOPPING) {
                state = requests.isEmpty() ? PageCleanerState.IDLE : PageCleanerState.RUNNING;
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
            requests.clear();
            inFlight = false;
            state = PageCleanerState.FAILED;
            idleChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean isIdleOrStopped() {
        return requests.isEmpty()
                && !inFlight
                && (state == PageCleanerState.IDLE
                || state == PageCleanerState.STOPPED
                || state == PageCleanerState.FAILED);
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
