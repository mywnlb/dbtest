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
 * Read-ahead 服务（linear §8.1/§8.2 + random §8.3）：实现 {@link ReadAheadHook}，前台 {@code getPage} 经
 * {@link #recordAccess} 上报访问；内部两个检测器各自判定是否预取，达条件时把预取请求入有界队列；单后台 worker 出队、
 * 对每页调 {@code BufferPool.prefetch}（最佳努力、不 fix、不提升）。
 *
 * <ul>
 *   <li><b>linear</b>（{@link LinearReadAheadTracker}）：检测同一 extent 内顺序访问达阈值，预取<em>下一</em> extent。</li>
 *   <li><b>random</b>（{@link RandomReadAheadDetector}，默认禁用）：检测被访问页所在 extent 驻留页数达阈值，补取<em>该</em>
 *       extent 缺失页。启用时每次访问查一次 {@code bufferPool.residentCountInRange}（O(extent) 开销）；threshold 0=禁用，
 *       普通路径无额外开销，对齐 MySQL 默认 {@code innodb_random_read_ahead=OFF}。</li>
 * </ul>
 *
 * <p><b>非阻塞前台</b>：{@link #recordAccess} 只喂检测器并可能入队（队满即丢弃 read-ahead，绝不阻塞 demand read），
 * 不在前台线程做盘 IO，且**绝不抛异常**（random 检测的任何异常被吞掉、丢弃本次预取）。停止后 {@code recordAccess} 静默忽略。
 *
 * <p><b>并发边界</b>：单 {@code lock} 保护 tracker / detector / 队列 / 状态；{@code recordAccess} 持该 lock 时调
 * {@code residentCountInRange}（短持 pageHashLock），与 worker {@code prefetch}（进 BufferPool 内部锁、不持本 lock）、{@code getPage}
 * （先放 BufferPool 内部锁再 recordAccess）一致为 service.lock→BufferPool 单向，无反向、无环。无 {@code synchronized}，所有等待带
 * 条件/超时；停止 join 有超时。
 */
public final class ReadAheadService implements ReadAheadHook, AutoCloseable {

    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏Buffer Pool的不变量。
     */
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** 预取目标池；worker 对其调 {@code prefetch}。 */
    private final BufferPool bufferPool;
    /** 顺序访问检测器（单顺序流，仅在 {@link #recordAccess} 内、持 lock 串行访问）。 */
    private final LinearReadAheadTracker tracker;
    /** 随机访问检测器；null 表示 random read-ahead 禁用（threshold 0）。仅在 {@link #recordAccess} 内、持 lock 串行访问。 */
    private final RandomReadAheadDetector randomDetector;
    /** 有界预取请求队列容量，防止后台停滞时无限堆积。 */
    private final int queueCapacity;

    /**
     * 保护本对象共享状态的显式并发闩；获取后必须在 {@code finally} 或 Guard 关闭路径释放。
     */
    private final ReentrantLock lock = new ReentrantLock();
    /** 有新请求或停止时唤醒 worker。 */
    private final Condition workAvailable = lock.newCondition();
    /** 状态变空闲/停止时唤醒 awaitIdle 等待者。 */
    private final Condition idleChanged = lock.newCondition();
    /** 有界预取请求队列。 */
    private final Deque<ReadAheadRequest> requests = new ArrayDeque<>();
    /** worker 终止信号，stop(timeout) 等待它。 */
    private final CountDownLatch terminated = new CountDownLatch(1);

    /**
     * 本对象的权威状态机字段 {@code state}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
     */
    private ReadAheadState state = ReadAheadState.NEW;
    /** 是否有一批预取正在锁外执行。 */
    private boolean inFlight;
    /**
     * 本对象拥有的后台工作线程；启动、停止与引用清理必须由生命周期锁协调，关闭后不得残留存活线程。
     */
    private Thread thread;

    /**
     * 仅 linear read-ahead（random 禁用）。等价于 {@code ReadAheadService(bufferPool, threshold, 0, queueCapacity)}。
     *
     * @param bufferPool    预取目标池。
     * @param threshold     linear read-ahead 触发阈值（同一 extent 连续访问页数，1..64）。
     * @param queueCapacity 预取请求队列容量（≥1）。
     */
    public ReadAheadService(BufferPool bufferPool, int threshold, int queueCapacity) {
        this(bufferPool, threshold, 0, queueCapacity);
    }

    /**
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param bufferPool      预取目标池。
     * @param linearThreshold linear read-ahead 触发阈值（同一 extent 连续访问页数，1..64）。
     * @param randomThreshold random read-ahead 触发阈值（同一 extent 驻留页数，1..64）；<b>0 表示禁用</b>（不构造检测器、
     *                        普通路径不查 residentCountInRange），对齐 MySQL 默认 OFF。
     * @param queueCapacity   预取请求队列容量（≥1）。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public ReadAheadService(BufferPool bufferPool, int linearThreshold, int randomThreshold, int queueCapacity) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (bufferPool == null) {
            throw new DatabaseValidationException("read-ahead buffer pool must not be null");
        }
        if (queueCapacity < 1) {
            throw new DatabaseValidationException("read-ahead queue capacity must be >= 1: " + queueCapacity);
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        if (randomThreshold < 0) {
            throw new DatabaseValidationException("random read-ahead threshold must be >= 0 (0 disables): "
                    + randomThreshold);
        }
        this.bufferPool = bufferPool;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.tracker = new LinearReadAheadTracker(linearThreshold);
        // threshold 0 → 不构造检测器（random 禁用），使普通 recordAccess 不查 residentCountInRange、零额外开销。
        this.randomDetector = randomThreshold == 0 ? null : new RandomReadAheadDetector(randomThreshold);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.queueCapacity = queueCapacity;
    }

    /** 启动后台 worker。只能从 NEW 启动一次。
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
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
            maybeScheduleRandom(pageId);
        } finally {
            lock.unlock();
        }
    }

    /**
     * random read-ahead 调度（§8.3，默认禁用时为 no-op）。调用方须持 {@code lock}。
     *
     * <p>数据流：查被访问页所在 extent 的驻留页数（{@code residentCountInRange} 短持 pageHashLock，service.lock→BufferPool 单向）
     * 喂 {@link RandomReadAheadDetector}；命中且队未满则入队补取整 extent（{@code prefetch} 自身跳过已驻留页）。
     *
     * <p><b>最佳努力</b>：resident count 或 detector 的任何 {@link RuntimeException} 都被吞掉、只丢弃本次 random 预取，
     * 绝不向上传播——random 是次要启发式，不能因它破坏 demand read 或已入队的 linear read-ahead。
     */
    private void maybeScheduleRandom(PageId pageId) {
        if (randomDetector == null) {
            return; // random 禁用：零额外开销，不查 residentCountInRange。
        }
        try {
            long extentFirst = (pageId.pageNo().value() / RandomReadAheadDetector.PAGES_PER_EXTENT)
                    * RandomReadAheadDetector.PAGES_PER_EXTENT;
            int resident = bufferPool.residentCountInRange(pageId.spaceId(), extentFirst,
                    RandomReadAheadDetector.PAGES_PER_EXTENT);
            Optional<ReadAheadRequest> randomRequest = randomDetector.record(pageId, resident);
            if (randomRequest.isPresent() && requests.size() < queueCapacity) {
                requests.addLast(randomRequest.get());
                workAvailable.signal();
            }
        } catch (RuntimeException ignored) {
            // 吞掉：random 检测失败不影响前台 demand read 与 linear read-ahead（本片不统计/不重试）。
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

    /** 当前 worker 状态。
     *
     * @return {@code state} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
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

    /** 锁外逐页预取一个 extent。prefetch 最佳努力（跳过已驻留 / 无空闲帧丢弃 / 吞 IO 失败），故不会让 worker 失败。
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
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
