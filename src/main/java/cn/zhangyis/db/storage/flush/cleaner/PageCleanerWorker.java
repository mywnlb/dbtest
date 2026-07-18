package cn.zhangyis.db.storage.flush.cleaner;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.flush.FlushCycleResult;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.flush.FlushWriteException;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单线程 page cleaner worker。它接收显式 flush request，也可以在空闲超时后执行周期性 capacity tick；
 * 两种路径都只调用 {@link FlushService#flushForCapacity(int)}，不直接访问 Buffer Pool frame 或 PageStore，
 * 因此不会在后台线程中引入新的页锁顺序。
 */
public final class PageCleanerWorker implements PageCleanerWorkerHandle {

    /** 关闭周期 tick 的内部哨兵；显式请求模式保持 F2 既有行为。 */
    private static final int PERIODIC_DISABLED = -1;

    /** 每秒纳秒数，用于 Duration 转换并避免溢出。 */
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    /** flush service 门面，执行实际 capacity flush cycle。 */
    private final FlushService flushService;
    /** 请求队列容量，防止后台 worker 停滞时无限堆积请求。 */
    private final int queueCapacity;
    /** 空闲等待间隔；即使没有 signal，也会周期性醒来检查停止条件。 */
    private final long idleWaitNanos;
    /** 空闲等待超时后自动执行的 maxPages；为 {@link #PERIODIC_DISABLED} 时不生成周期 tick。 */
    private final int periodicMaxPages;
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
    /** 当前 worker 生命周期内成功完成的 flush cycle 数；由 lock 保护，供 supervisor 按差值汇总。 */
    private long completedCycles;
    /** worker 失败根因。 */
    private DatabaseRuntimeException failure;

    /**
     * 创建 {@code PageCleanerWorker}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param flushService 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param queueCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param idleWait 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     */
    public PageCleanerWorker(FlushService flushService, int queueCapacity, Duration idleWait) {
        this(flushService, queueCapacity, idleWait, PERIODIC_DISABLED, false);
    }

    /**
     * 创建带周期 tick 的 page cleaner。周期 tick 只表达“检查 redo capacity 并推进 checkpoint”的后台节奏；
     * 真正刷哪些页仍由 {@link FlushService} 内部策略和 WAL gate 决定。{@code periodicMaxPages=0} 时允许只推进
     * checkpoint，不主动刷脏，便于 engine bootstrap 测试验证后台 checkpoint worker 语义。
     * @param flushService 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param queueCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param idleWait 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @param periodicMaxPages 参与 {@code 构造} 的上界或规格值 {@code periodicMaxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     */
    public PageCleanerWorker(FlushService flushService, int queueCapacity, Duration idleWait, int periodicMaxPages) {
        this(flushService, queueCapacity, idleWait, periodicMaxPages, true);
    }

    /**
     * 创建 {@code PageCleanerWorker}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param flushService 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param queueCapacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param idleWait 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @param periodicMaxPages 参与 {@code 构造} 的上界或规格值 {@code periodicMaxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @param validatePeriodicMaxPages 当前算法是否纳入终态增量、同步压力、磁盘来源、根节点或周期上界校验；用于选择对应的不变量检查分支
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private PageCleanerWorker(FlushService flushService, int queueCapacity, Duration idleWait,
                              int periodicMaxPages, boolean validatePeriodicMaxPages) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (flushService == null || idleWait == null) {
            throw new DatabaseValidationException("page cleaner service/idle wait must not be null");
        }
        if (queueCapacity < 1) {
            throw new DatabaseValidationException("page cleaner queue capacity must be >= 1: " + queueCapacity);
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        if (idleWait.isNegative() || idleWait.isZero()) {
            throw new DatabaseValidationException("page cleaner idle wait must be positive: " + idleWait);
        }
        if (validatePeriodicMaxPages && periodicMaxPages < 0) {
            throw new DatabaseValidationException("page cleaner periodic max pages must not be negative: "
                    + periodicMaxPages);
        }
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.flushService = flushService;
        this.queueCapacity = queueCapacity;
        this.idleWaitNanos = timeoutNanos(idleWait);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.periodicMaxPages = periodicMaxPages;
    }

    /** 启动后台 worker。只能从 NEW 状态启动一次。
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws PageCleanerStoppedException 后台刷脏工作线程已经停止或无法继续服务时抛出；监督者应停止派发并关闭或重启对应 worker
     * @throws FlushWriteException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
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

    /** 当前 worker 状态。
     *
     * @return {@code state} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public PageCleanerState state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    /** 最近一轮成功执行的 flush cycle。
     *
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<FlushCycleResult> lastCycle() {
        lock.lock();
        try {
            return Optional.ofNullable(lastCycle);
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
     * 生成 worker 当前状态快照。快照只暴露诊断信息，不携带可唤醒线程的锁或 Condition 引用。
     *
     * @return 当前 worker 生命周期内的只读诊断快照。
     */
    public PageCleanerWorkerSnapshot snapshot() {
        lock.lock();
        try {
            return new PageCleanerWorkerSnapshot(state, inFlight, requests.size(), completedCycles,
                    lastCycle != null, failure == null ? "" : failure.getMessage());
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
                    long remaining = workAvailable.awaitNanos(idleWaitNanos);
                    if (remaining <= 0 && requests.isEmpty() && state != PageCleanerState.STOPPING
                            && periodicTickEnabled()) {
                        inFlight = true;
                        state = PageCleanerState.RUNNING;
                        return periodicMaxPages;
                    }
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

    private boolean periodicTickEnabled() {
        return periodicMaxPages != PERIODIC_DISABLED;
    }

    private void markCycleComplete(FlushCycleResult cycle) {
        lock.lock();
        try {
            lastCycle = cycle;
            completedCycles++;
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
