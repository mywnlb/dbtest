package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Linear read-ahead 服务（§8.1/§8.2）：实现 {@link ReadAheadHook}，前台 {@code getPage} 经 {@link #recordAccess} 上报访问；
 * 内部 {@link LinearReadAheadTracker} 检测同一 extent 内的顺序访问，达阈值时把「下一 extent 预取请求」入有界队列；
 * 单后台 worker 出队、对每页调 {@code BufferPool.prefetch}（最佳努力、不 fix、不提升）。
 *
 * <p><b>非阻塞前台</b>：{@link #recordAccess} 只喂检测器并可能入队（队满即丢弃 read-ahead，绝不阻塞 demand read），
 * 不在前台线程做盘 IO，且**绝不抛异常**。停止后 {@code recordAccess} 静默忽略。
 *
 * <p><b>并发边界</b>：单 {@code lock} 保护 tracker / 队列 / 状态；worker 在锁外调 {@code prefetch}（prefetch 自带 poolLock）。
 * 无 {@code synchronized}，所有等待带条件/超时；停止 join 有超时。
 */
public final class ReadAheadService implements ReadAheadHook, AutoCloseable {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** 预取目标池；worker 对其调 {@code prefetch}。 */
    private final BufferPool bufferPool;
    /** 顺序访问检测器（单顺序流，仅在 {@link #recordAccess} 内、持 lock 串行访问）。 */
    private final LinearReadAheadTracker tracker;
    /** 有界预取请求队列容量，防止后台停滞时无限堆积。 */
    private final int queueCapacity;

    private final ReentrantLock lock = new ReentrantLock();
    /** 有新请求或停止时唤醒 worker。 */
    private final Condition workAvailable = lock.newCondition();
    /** 状态变空闲/停止时唤醒 awaitIdle 等待者。 */
    private final Condition idleChanged = lock.newCondition();
    /** 有界预取请求队列。 */
    private final Deque<ReadAheadRequest> requests = new ArrayDeque<>();
    /** worker 终止信号，stop(timeout) 等待它。 */
    private final CountDownLatch terminated = new CountDownLatch(1);

    private ReadAheadState state = ReadAheadState.NEW;
    /** 是否有一批预取正在锁外执行。 */
    private boolean inFlight;
    private Thread thread;

    /**
     * @param bufferPool    预取目标池。
     * @param threshold     linear read-ahead 触发阈值（同一 extent 连续访问页数，1..64）。
     * @param queueCapacity 预取请求队列容量（≥1）。
     */
    public ReadAheadService(BufferPool bufferPool, int threshold, int queueCapacity) {
        if (bufferPool == null) {
            throw new DatabaseValidationException("read-ahead buffer pool must not be null");
        }
        if (queueCapacity < 1) {
            throw new DatabaseValidationException("read-ahead queue capacity must be >= 1: " + queueCapacity);
        }
        this.bufferPool = bufferPool;
        this.tracker = new LinearReadAheadTracker(threshold);
        this.queueCapacity = queueCapacity;
    }

    /** 启动后台 worker。只能从 NEW 启动一次。 */
    public void start() {
        lock.lock();
        try {
            if (state != ReadAheadState.NEW) {
                throw new DatabaseValidationException("read-ahead can only start from NEW state: " + state);
            }
            state = ReadAheadState.IDLE;
            thread = new Thread(this::runLoop, "minimysql-read-ahead");
            thread.setDaemon(true);
            thread.start();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 上报一次页访问（{@link ReadAheadHook}）。喂检测器，若产出预取请求则入队（队满或已停止则丢弃）。
     * 该方法廉价、非阻塞、不抛异常——它在 demand read 热路径上被调用。
     */
    @Override
    public void recordAccess(PageId pageId) {
        if (pageId == null) {
            return; // read-ahead 是最佳努力：异常输入静默忽略，绝不破坏 demand read。
        }
        lock.lock();
        try {
            if (state != ReadAheadState.IDLE && state != ReadAheadState.RUNNING) {
                return; // 未启动 / 停止中 / 已停止：不再调度。
            }
            Optional<ReadAheadRequest> request = tracker.record(pageId);
            if (request.isPresent() && requests.size() < queueCapacity) {
                requests.addLast(request.get());
                workAvailable.signal();
            }
            // 队满：丢弃 read-ahead 请求（不挤占前台、不无限堆积）。
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待 worker 空闲（队列空且无在飞预取）或已停止。供测试确定性地观察预取完成。
     *
     * @param timeout 最大等待时间。
     * @return true 表示已空闲/停止；false 表示超时或中断。
     */
    public boolean awaitIdle(Duration timeout) {
        long nanos = validateWaitTimeout(timeout);
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
     * 请求停止并在 timeout 内等待 worker 退出；丢弃未处理的预取请求（read-ahead 可丢弃）。
     *
     * @param timeout 最大等待时间。
     * @return true 表示已停止；false 表示超时或中断。
     */
    public boolean stop(Duration timeout) {
        long nanos = validateWaitTimeout(timeout);
        lock.lock();
        try {
            if (state == ReadAheadState.NEW) {
                state = ReadAheadState.STOPPED;
                terminated.countDown();
                idleChanged.signalAll();
                return true;
            }
            if (state == ReadAheadState.STOPPED) {
                return true;
            }
            state = ReadAheadState.STOPPING;
            requests.clear();
            workAvailable.signalAll();
        } finally {
            lock.unlock();
        }
        try {
            return terminated.await(nanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** 当前 worker 状态。 */
    public ReadAheadState state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    /** close 默认等待 5 秒；生产 shutdown / 测试可显式 stop(timeout)。 */
    @Override
    public void close() {
        stop(Duration.ofSeconds(5));
    }

    private void runLoop() {
        try {
            while (true) {
                ReadAheadRequest request = takeRequestOrStop();
                if (request == null) {
                    return;
                }
                prefetchExtent(request);
                markBatchComplete();
            }
        } finally {
            lock.lock();
            try {
                state = ReadAheadState.STOPPED;
                inFlight = false;
                idleChanged.signalAll();
                terminated.countDown();
            } finally {
                lock.unlock();
            }
        }
    }

    private ReadAheadRequest takeRequestOrStop() {
        lock.lock();
        try {
            while (requests.isEmpty() && state != ReadAheadState.STOPPING) {
                state = ReadAheadState.IDLE;
                idleChanged.signalAll();
                try {
                    workAvailable.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    state = ReadAheadState.STOPPING;
                    break;
                }
            }
            if (state == ReadAheadState.STOPPING) {
                return null;
            }
            ReadAheadRequest request = requests.pollFirst();
            if (request == null) {
                return null;
            }
            inFlight = true;
            state = ReadAheadState.RUNNING;
            return request;
        } finally {
            lock.unlock();
        }
    }

    /** 锁外逐页预取一个 extent。prefetch 最佳努力（跳过已驻留 / 无空闲帧丢弃 / 吞 IO 失败），故不会让 worker 失败。 */
    private void prefetchExtent(ReadAheadRequest request) {
        long first = request.firstPageNo();
        for (int i = 0; i < request.pageCount(); i++) {
            bufferPool.prefetch(PageId.of(request.spaceId(), PageNo.of(first + i)));
        }
    }

    private void markBatchComplete() {
        lock.lock();
        try {
            inFlight = false;
            if (state != ReadAheadState.STOPPING) {
                state = requests.isEmpty() ? ReadAheadState.IDLE : ReadAheadState.RUNNING;
            }
            idleChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean isIdleOrStopped() {
        return requests.isEmpty() && !inFlight
                && (state == ReadAheadState.IDLE || state == ReadAheadState.STOPPED);
    }

    private static long validateWaitTimeout(Duration timeout) {
        if (timeout == null) {
            throw new DatabaseValidationException("read-ahead wait timeout must not be null");
        }
        if (timeout.isNegative()) {
            throw new DatabaseValidationException("read-ahead wait timeout must not be negative: " + timeout);
        }
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
