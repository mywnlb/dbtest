package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Buffer Pool facade（§5.1）：把 {@link PageId} 经 {@link BufferPoolRouter} 路由到若干 {@link BufferPoolInstance} 分片，
 * 单页操作（get/new/prefetch/flush/snapshot）直接转发到归属分片；跨切面查询（dirty 候选 / oldest LSN / hasDirty /
 * residentCount / 截断）逐分片调用本地方法再在锁外合并。
 *
 * <p><b>分片（§5.2）</b>：默认 {@code instanceCount=1}，行为与原单实例池字节级等价（路由恒 0）。
 * 总容量按 {@code base=cap/N + 前 r 个各 +1} 切分到各分片（要求 {@code capacity≥instanceCount}）。每分片自有
 * pageHashLock/frameMutex 以及 free/LRU/flush list 专用短锁；分片间无 work stealing——某分片满即抛 {@link BufferPoolExhaustedException}，
 * 即便他分片有空闲帧（与 InnoDB 一致）。
 *
 * <p><b>并发不变量</b>：facade 的跨切面操作<b>一次只进入一个 instance</b>（单分片取快照→解锁→下一分片→锁外合并），
 * 绝不同时持两把 → 无分片间锁序、无死锁。read-ahead 钩子在 {@link #getPage} 路由+取页之后调用（钩子是 pool 级、非分片级）。
 *
 * <p>同时提供 {@code attachVictimFlusher}/{@code attachReadAheadHook}（set-once）：victim flusher 传播给每个分片
 * （flushVictim 经本 facade 路由回归属分片），read-ahead 钩子留在 facade。
 */
public final class LruBufferPool implements BufferPool {

    /** 默认 load 超时：命中 LOADING 页的等待者最长等待时长。 */
    private static final Duration DEFAULT_LOAD_TIMEOUT = Duration.ofSeconds(30);

    /** 分片数组（≥1）。下标即 {@link BufferPoolRouter} 的路由结果；构造期固定。 */
    private final BufferPoolInstance[] instances;
    /** PageId→分片下标路由器；与 {@link #instances} 长度一致。 */
    private final BufferPoolRouter router;
    /**
     * Buffer Pool 内部的表空间生命周期时钟。truncate/drop/discard invalidate 期间用它阻止绕过 MTR lease 的直接
     * page admission，并给 frame 打版本戳，避免旧代际 frame 在维护后重新可见。
     */
    private final SpaceLifecycleClock lifecycleClock;
    /** dirty view 变化等待锁；只承载 Condition，不保护 BufferFrame 元数据或 page hash。 */
    private final ReentrantLock dirtyStateWaitLock = new ReentrantLock();
    /** flush/drain 等待 dirty view 可能变化的通知点；谓词由调用方醒来后重新扫描。 */
    private final Condition dirtyStateChanged = dirtyStateWaitLock.newCondition();

    /**
     * 淘汰脏页的 WAL 安全刷盘端口（facade 级 set-once）。bootstrap 注入后传播给每个分片；CAS 防运行期被换。
     */
    private final AtomicReference<DirtyVictimFlusher> victimFlusher = new AtomicReference<>();

    /**
     * 可选 read-ahead 钩子（facade 级 set-once）；null 表示未启用预取。{@link #getPage} 命中/未命中后回调它上报访问。
     * 钩子必须廉价且不抛异常（demand read 热路径）。
     */
    private final AtomicReference<ReadAheadHook> readAheadHook = new AtomicReference<>();

    /** 单实例池，默认 midpoint LRU（Phase A）。等价 {@code instanceCount=1}。 */
    public LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity) {
        this(pageStore, pageSize, capacity, 1);
    }

    /**
     * 多实例池：把容量分到 {@code instanceCount} 个分片，各用独立默认 midpoint 策略 + 默认 load 超时。
     * 生产由 {@code StorageEngine} 经 {@code EngineConfig.bufferPoolInstanceCount()} 构造（默认 1）。
     */
    public LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity, int instanceCount) {
        this.lifecycleClock = new SpaceLifecycleClock();
        this.instances = buildInstances(pageStore, pageSize, capacity, instanceCount,
                DEFAULT_LOAD_TIMEOUT, lifecycleClock, this::signalDirtyStateChanged);
        this.router = new BufferPoolRouter(instanceCount);
    }

    /** 单实例池 + 注入替换策略（测试注入可控时钟）。 */
    LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy) {
        this(pageStore, pageSize, capacity, policy, DEFAULT_LOAD_TIMEOUT);
    }

    /** 单实例池 + 注入替换策略 + load 超时（测试注入短超时验证 LOADING 等待有界）。 */
    LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy,
                  Duration loadTimeout) {
        this.lifecycleClock = new SpaceLifecycleClock();
        this.instances = new BufferPoolInstance[]{
                new BufferPoolInstance(pageStore, pageSize, capacity, policy, loadTimeout, lifecycleClock,
                        this::signalDirtyStateChanged)};
        this.router = new BufferPoolRouter(1);
    }

    /** 容量切分：base=cap/N、前 r 个分片各 +1；要求 capacity≥instanceCount（每分片至少 1 帧）。 */
    private static BufferPoolInstance[] buildInstances(PageStore pageStore, PageSize pageSize, int capacity,
                                                        int instanceCount, Duration loadTimeout,
                                                        SpaceLifecycleClock lifecycleClock,
                                                        Runnable dirtyStateChangeListener) {
        if (instanceCount < 1) {
            throw new DatabaseValidationException("buffer pool instance count must be >= 1: " + instanceCount);
        }
        if (capacity < instanceCount) {
            throw new DatabaseValidationException("capacity must be >= instanceCount: capacity=" + capacity
                    + " instanceCount=" + instanceCount);
        }
        BufferPoolInstance[] arr = new BufferPoolInstance[instanceCount];
        int base = capacity / instanceCount;
        int remainder = capacity % instanceCount;
        for (int i = 0; i < instanceCount; i++) {
            int shardCapacity = base + (i < remainder ? 1 : 0);
            // 每分片独立 midpoint 策略对象（策略持 per-instance LRU 状态，不能跨分片共享）。
            arr[i] = new BufferPoolInstance(pageStore, pageSize, shardCapacity,
                    new MidpointLruReplacementPolicy(System::currentTimeMillis), loadTimeout, lifecycleClock,
                    dirtyStateChangeListener);
        }
        return arr;
    }

    /** 注入淘汰脏页的 WAL 安全刷盘端口（set-once）并传播给每个分片。 */
    public void attachVictimFlusher(DirtyVictimFlusher flusher) {
        if (flusher == null) {
            throw new DatabaseValidationException("victim flusher must not be null");
        }
        if (!victimFlusher.compareAndSet(null, flusher)) {
            throw new DatabaseValidationException("victim flusher already attached (set-once)");
        }
        for (BufferPoolInstance instance : instances) {
            instance.attachVictimFlusher(flusher);
        }
    }

    /** 注入 read-ahead 钩子（set-once）；留在 facade，由 {@link #getPage} 调用。 */
    public void attachReadAheadHook(ReadAheadHook hook) {
        if (hook == null) {
            throw new DatabaseValidationException("read-ahead hook must not be null");
        }
        if (!readAheadHook.compareAndSet(null, hook)) {
            throw new DatabaseValidationException("read-ahead hook already attached (set-once)");
        }
    }

    private BufferPoolInstance instanceFor(PageId pageId) {
        return instances[router.route(pageId)];
    }

    @Override
    public PageGuard getPage(PageId pageId, PageLatchMode mode) {
        PageGuard guard = instanceFor(pageId).getPage(pageId, mode);
        // demand read 后上报访问，驱动 read-ahead。钩子在分片锁之外调用、约定廉价/不抛异常，不影响返回的 guard。
        // newPage 不上报：read-ahead 只跟踪需求读模式。
        ReadAheadHook hook = readAheadHook.get();
        if (hook != null) {
            hook.recordAccess(pageId);
        }
        return guard;
    }

    @Override
    public PageGuard newPage(PageId pageId, PageLatchMode mode) {
        return instanceFor(pageId).newPage(pageId, mode);
    }

    @Override
    public void prefetch(PageId pageId) {
        instanceFor(pageId).prefetch(pageId);
    }

    @Override
    public List<DirtyPageCandidate> dirtyPageCandidates(Lsn targetLsn, int maxPages) {
        if (targetLsn == null) {
            throw new DatabaseValidationException("target LSN must not be null");
        }
        if (maxPages < 0) {
            throw new DatabaseValidationException("max pages must not be negative: " + maxPages);
        }
        // 各分片各取本地 ≤maxPages 候选（已按 oldest 升序裁剪），并集再全局按 oldest 升序排序取前 maxPages：
        // 全局 top-maxPages ⊆ 各分片 top-maxPages 的并集，故合并结果正确。
        List<DirtyPageCandidate> merged = new ArrayList<>();
        for (BufferPoolInstance instance : instances) {
            merged.addAll(instance.localDirtyPageCandidates(targetLsn, maxPages));
        }
        merged.sort(Comparator.comparingLong(candidate -> candidate.oldestModificationLsn().value()));
        if (merged.size() > maxPages) {
            merged = merged.subList(0, maxPages);
        }
        return List.copyOf(merged);
    }

    @Override
    public Optional<FlushPageSnapshot> snapshotForFlush(PageId pageId) {
        return instanceFor(pageId).snapshotForFlush(pageId);
    }

    /**
     * 等待任一分片的 dirty 状态变化通知。Condition 不保护 dirty 谓词，调用方必须在返回后重新查询 dirty view。
     */
    @Override
    public boolean awaitDirtyStateChange(Duration timeout) {
        if (timeout == null) {
            throw new DatabaseValidationException("dirty-state wait timeout must not be null");
        }
        if (timeout.isNegative()) {
            throw new DatabaseValidationException("dirty-state wait timeout must not be negative: " + timeout);
        }
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("dirty-state wait timeout is too large: " + timeout, overflow);
        }
        if (nanos == 0) {
            return !Thread.currentThread().isInterrupted();
        }
        dirtyStateWaitLock.lock();
        try {
            return dirtyStateChanged.awaitNanos(nanos) > 0;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            dirtyStateWaitLock.unlock();
        }
    }

    @Override
    public boolean completeFlush(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        return instanceFor(snapshot.pageId()).completeFlush(snapshot);
    }

    @Override
    public void failFlush(PageId pageId) {
        instanceFor(pageId).failFlush(pageId);
    }

    /** 唤醒等待 dirty view 变化的 flush/drain 调用方；不表示 dirty 谓词已经满足。 */
    private void signalDirtyStateChanged() {
        dirtyStateWaitLock.lock();
        try {
            dirtyStateChanged.signalAll();
        } finally {
            dirtyStateWaitLock.unlock();
        }
    }

    @Override
    public Lsn oldestDirtyLsnOr(Lsn cleanBoundary) {
        if (cleanBoundary == null) {
            throw new DatabaseValidationException("clean boundary must not be null");
        }
        Lsn min = null;
        for (BufferPoolInstance instance : instances) {
            Lsn local = instance.localOldestDirtyLsnOrNull();
            if (local != null && (min == null || local.value() < min.value())) {
                min = local;
            }
        }
        return min == null ? cleanBoundary : min;
    }

    @Override
    public boolean hasDirtyPages() {
        for (BufferPoolInstance instance : instances) {
            if (instance.hasDirtyPages()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 截断前排空目标表空间（§5.5）：空间的页按 hash 散在各分片，故必须对**所有**分片做。**两阶段**保证 all-or-nothing：
     * 先逐分片 {@link BufferPoolInstance#awaitDrainedAndCheckClean}（共享 deadline 等 fix=0 + 校验无脏，任一脏帧/超时即抛、
     * 尚未移除任何帧），全部通过后再逐分片 {@link BufferPoolInstance#removeTablespaceFrames}。调用方须已持该表空间独占
     * operation lease，阻止新 page fix 与并发 flush，使两阶段之间确认的 fix=0/clean 不变。
     */
    @Override
    public void invalidateTablespace(SpaceId spaceId, Duration timeout) {
        if (spaceId == null) {
            throw new DatabaseValidationException("invalidate tablespace space id must not be null");
        }
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("invalidate tablespace timeout must be positive");
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("invalidate tablespace timeout is too large", overflow);
        }
        long deadlineNanos = System.nanoTime() + timeoutNanos;
        lifecycleClock.beginInvalidation(spaceId);
        boolean advanced = false;
        try {
            // 阶段 1：维护窗口已打开，新 admission 被拒绝；全部分片 drain+check 通过前不推进版本。
            for (BufferPoolInstance instance : instances) {
                instance.awaitDrainedAndCheckClean(spaceId, deadlineNanos);
            }
            lifecycleClock.advanceInvalidation(spaceId);
            advanced = true;
            // 阶段 2：版本已推进但 admission 仍关闭，移除旧代际 frame 后才重新开放。
            for (BufferPoolInstance instance : instances) {
                instance.removeTablespaceFrames(spaceId);
            }
            lifecycleClock.finishInvalidation(spaceId);
        } catch (RuntimeException failure) {
            if (advanced) {
                lifecycleClock.finishInvalidation(spaceId);
            } else {
                lifecycleClock.abortInvalidation(spaceId);
            }
            throw failure;
        }
    }

    @Override
    public int residentCountInRange(SpaceId spaceId, long firstPageNo, int pageCount) {
        // 各分片本地区间计数求和：每个 PageId 只在其归属分片，故求和=区间内总驻留数。
        // 参数校验由分片内 PageHashTable.countInRange 完成（spaceId/firstPageNo/pageCount）。
        int total = 0;
        for (BufferPoolInstance instance : instances) {
            total += instance.countResidentInRange(spaceId, firstPageNo, pageCount);
        }
        return total;
    }

    @Override
    public int capacity() {
        int total = 0;
        for (BufferPoolInstance instance : instances) {
            total += instance.capacity();
        }
        return total;
    }

    @Override
    public int residentCount() {
        int total = 0;
        for (BufferPoolInstance instance : instances) {
            total += instance.residentCount();
        }
        return total;
    }

    @Override
    public List<PageId> residentPageIds() {
        List<PageId> all = new ArrayList<>();
        for (BufferPoolInstance instance : instances) {
            all.addAll(instance.residentPageIds());
        }
        return List.copyOf(all);
    }

    @Override
    public void close() {
        // BufferPool 不拥有 PageStore 生命周期，也不再提供 legacy flushAll 直写路径。
        // 调用方若需要 shutdown flush，必须先通过 FlushService/FlushCoordinator 完成 WAL-safe 写盘。
    }
}
