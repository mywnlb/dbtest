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

    /**
     * 可选发布前页面拦截器（set-once）。实例仅保存引用并在完成磁盘 IO 后、发布 page hash future 前调用；
     * 回调期间 facade/instance 的 metadata 锁均未持有。
     */
    private final AtomicReference<PageLoadInterceptor> pageLoadInterceptor = new AtomicReference<>();

    /** 单实例池，默认 midpoint LRU（Phase A）。等价 {@code instanceCount=1}。
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param capacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    public LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity) {
        this(pageStore, pageSize, capacity, 1);
    }

    /**
     * 多实例池：把容量分到 {@code instanceCount} 个分片，各用独立默认 midpoint 策略 + 默认 load 超时。
     * 生产由 {@code StorageEngine} 经 {@code EngineConfig.bufferPoolInstanceCount()} 构造（默认 1）。
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param capacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param instanceCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    public LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity, int instanceCount) {
        this.lifecycleClock = new SpaceLifecycleClock();
        this.instances = buildInstances(pageStore, pageSize, capacity, instanceCount,
                DEFAULT_LOAD_TIMEOUT, lifecycleClock, this::signalDirtyStateChanged);
        this.router = new BufferPoolRouter(instanceCount);
    }

    /** 单实例池 + 注入替换策略（测试注入可控时钟）。
     *
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param capacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param policy 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     */
    LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy) {
        this(pageStore, pageSize, capacity, policy, DEFAULT_LOAD_TIMEOUT);
    }

    /** 单实例池 + 注入替换策略 + load 超时（测试注入短超时验证 LOADING 等待有界）。
     *
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param capacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param policy 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param loadTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     */
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

    /** 注入淘汰脏页的 WAL 安全刷盘端口（set-once）并传播给每个分片。
     *
     * @param flusher 由组合根提供的 {@code DirtyVictimFlusher} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code attachVictimFlusher} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** 注入 read-ahead 钩子（set-once）；留在 facade，由 {@link #getPage} 调用。
     * @param hook 由组合根提供的 {@code ReadAheadHook} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code attachReadAheadHook} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void attachReadAheadHook(ReadAheadHook hook) {
        if (hook == null) {
            throw new DatabaseValidationException("read-ahead hook must not be null");
        }
        if (!readAheadHook.compareAndSet(null, hook)) {
            throw new DatabaseValidationException("read-ahead hook already attached (set-once)");
        }
    }

    /**
     * 注入发布前页面拦截器并传播到全部分片。该能力影响页可见性，必须在开放用户流量前 set-once 安装。
     *
     * @param interceptor 只通过 PendingPagePublication 修改页面的拦截器；不得为 {@code null}
     * @throws DatabaseValidationException 重复安装或参数为空时抛出
     */
    public void attachPageLoadInterceptor(PageLoadInterceptor interceptor) {
        if (interceptor == null) {
            throw new DatabaseValidationException("page load interceptor must not be null");
        }
        if (!pageLoadInterceptor.compareAndSet(null, interceptor)) {
            throw new DatabaseValidationException("page load interceptor already attached (set-once)");
        }
        for (BufferPoolInstance instance : instances) {
            instance.attachPageLoadInterceptor(interceptor);
        }
    }

    private BufferPoolInstance instanceFor(PageId pageId) {
        return instances[router.route(pageId)];
    }

    /** O(1) 路由到所属分片查询 page hash；LOADING 占位同样阻止 Change Buffer 新增 mutation。 */
    @Override
    public boolean isResident(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("resident page id must not be null");
        }
        return instanceFor(pageId).isResident(pageId);
    }

    /**
     * 返回 {@code getPage} 对应的Buffer Pool受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code getPage} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
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

    /**
     * 根据调用参数构造 {@code newPage} 对应的Buffer Pool领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code newPage} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    @Override
    public PageGuard newPage(PageId pageId, PageLatchMode mode) {
        return instanceFor(pageId).newPage(pageId, mode);
    }

    /**
     * 将预取请求路由到 PageId 所属分片；预取只填充可淘汰 frame，不向调用方返回或遗留 page fix。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     */
    @Override
    public void prefetch(PageId pageId) {
        instanceFor(pageId).prefetch(pageId);
    }

    /**
     * 汇总各分片 oldest modification LSN 不晚于目标 LSN 的脏页候选，全局排序后最多返回 maxPages 个不可变快照。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，过期映射在返回前拒绝。</li>
     *     <li>遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，慢 IO 或条件等待移到内部锁外。</li>
     *     <li>完成页载入、替换、dirty snapshot 或状态转换，并向等待者发布唯一完成或失败信号。</li>
     *     <li>返回受控 Guard/快照或释放 fix；失败回收占位且不错误清除并发产生的 dirty 状态。</li>
     * </ol>
     *
     * @param targetLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param maxPages 参与 {@code dirtyPageCandidates} 的上界或规格值 {@code maxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public List<DirtyPageCandidate> dirtyPageCandidates(Lsn targetLsn, int maxPages) {
        // 1、按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，在共享或持久副作用前拒绝非法状态。
        if (targetLsn == null) {
            throw new DatabaseValidationException("target LSN must not be null");
        }
        if (maxPages < 0) {
            throw new DatabaseValidationException("max pages must not be negative: " + maxPages);
        }
        // 各分片各取本地 ≤maxPages 候选（已按 oldest 升序裁剪），并集再全局按 oldest 升序排序取前 maxPages：
        // 全局 top-maxPages ⊆ 各分片 top-maxPages 的并集，故合并结果正确。
        // 2、继续完成范围、身份与候选校验；通过后，遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，保持处理顺序与资源边界。
        List<DirtyPageCandidate> merged = new ArrayList<>();
        for (BufferPoolInstance instance : instances) {
            merged.addAll(instance.localDirtyPageCandidates(targetLsn, maxPages));
        }
        // 3、在中间分支复核阶段性结果；满足条件后，完成页载入、替换、dirty snapshot 或状态转换，并维持领域不变量。
        merged.sort(Comparator.comparingLong(candidate -> candidate.oldestModificationLsn().value()));
        if (merged.size() > maxPages) {
            merged = merged.subList(0, maxPages);
        }
        // 4、返回受控 Guard/快照或释放 fix，以稳定返回或领域异常完成收口。
        return List.copyOf(merged);
    }

    /**
     * 按各分片 LRU 淘汰顺序汇总脏页候选，并在不固定 frame 的情况下最多返回 maxPages 个不可变候选。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，过期映射在返回前拒绝。</li>
     *     <li>遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，慢 IO 或条件等待移到内部锁外。</li>
     *     <li>完成页载入、替换、dirty snapshot 或状态转换，并向等待者发布唯一完成或失败信号。</li>
     *     <li>返回受控 Guard/快照或释放 fix；失败回收占位且不错误清除并发产生的 dirty 状态。</li>
     * </ol>
     *
     * @param maxPages 参与 {@code lruDirtyPageCandidates} 的上界或规格值 {@code maxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public List<DirtyPageCandidate> lruDirtyPageCandidates(int maxPages) {
        // 1、按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，在共享或持久副作用前拒绝非法状态。
        if (maxPages < 0) {
            throw new DatabaseValidationException("lru dirty max pages must not be negative: " + maxPages);
        }
        // 2、继续完成范围、身份与候选校验；通过后，遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，保持处理顺序与资源边界。
        List<DirtyPageCandidate> merged = new ArrayList<>();
        // 3、在中间分支复核阶段性结果；满足条件后，完成页载入、替换、dirty snapshot 或状态转换，并维持领域不变量。
        for (BufferPoolInstance instance : instances) {
            merged.addAll(instance.localLruDirtyPageCandidates(maxPages));
        }
        if (merged.size() > maxPages) {
            merged = merged.subList(0, maxPages);
        }
        // 4、返回受控 Guard/快照或释放 fix，以稳定返回或领域异常完成收口。
        return List.copyOf(merged);
    }

    /**
     * 计算 {@code freeFrameCount} 所表达的Buffer Pool数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @return {@code freeFrameCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    @Override
    public int freeFrameCount() {
        int total = 0;
        for (BufferPoolInstance instance : instances) {
            total += instance.freeFrameCount();
        }
        return total;
    }

    /**
     * 推进Buffer Pool刷盘或检查点边界；写数据前遵守 WAL，失败时不得清除尚未安全持久化的状态。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     */
    @Override
    public Optional<FlushPageSnapshot> snapshotForFlush(PageId pageId) {
        return instanceFor(pageId).snapshotForFlush(pageId);
    }

    /**
     * 等待任一分片的 dirty 状态变化通知。Condition 不保护 dirty 谓词，调用方必须在返回后重新查询 dirty view。
     *
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @return 在超时或取消前观察到 {@code awaitDirtyStateChange} 的目标状态时为 {@code true}；等待期限届满且状态仍未满足时为 {@code false}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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

    /**
     * 推进Buffer Pool刷盘或检查点边界；写数据前遵守 WAL，失败时不得清除尚未安全持久化的状态。
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code completeFlush} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public boolean completeFlush(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        return instanceFor(snapshot.pageId()).completeFlush(snapshot);
    }

    /**
     * 推进Buffer Pool刷盘或检查点边界；写数据前遵守 WAL，失败时不得清除尚未安全持久化的状态。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     */
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

    /**
     * 读取所有分片的最早脏页 LSN 并返回全局最小值；没有脏页时返回调用方提供的 cleanBoundary。
     *
     * @param cleanBoundary redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @return {@code oldestDirtyLsnOr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /**
     * 判断 {@code hasDirtyPages} 所表达的Buffer Pool条件；方法只读取稳定状态，并用返回值报告是否满足条件。
     *
     * @return {@code hasDirtyPages} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
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
     * <p>数据流：</p>
     * <ol>
     *     <li>按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，过期映射在返回前拒绝。</li>
     *     <li>遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，慢 IO 或条件等待移到内部锁外。</li>
     *     <li>完成页载入、替换、dirty snapshot 或状态转换，并向等待者发布唯一完成或失败信号。</li>
     *     <li>返回受控 Guard/快照或释放 fix；失败回收占位且不错误清除并发产生的 dirty 状态。</li>
     * </ol>
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void invalidateTablespace(SpaceId spaceId, Duration timeout) {
        // 1、按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，在共享或持久副作用前拒绝非法状态。
        if (spaceId == null) {
            throw new DatabaseValidationException("invalidate tablespace space id must not be null");
        }
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("invalidate tablespace timeout must be positive");
        }
        // 2、继续完成范围、身份与候选校验；通过后，遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，保持处理顺序与资源边界。
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("invalidate tablespace timeout is too large", overflow);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，完成页载入、替换、dirty snapshot 或状态转换，并维持领域不变量。
        long deadlineNanos = System.nanoTime() + timeoutNanos;
        lifecycleClock.beginInvalidation(spaceId);
        boolean advanced = false;
        // 4、返回受控 Guard/快照或释放 fix，以稳定返回或领域异常完成收口。
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

    /**
     * 计算 {@code residentCountInRange} 所表达的Buffer Pool数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param firstPageNo 参与 {@code residentCountInRange} 的原始数值身份 {@code firstPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param pageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code residentCountInRange} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
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

    /**
     * 计算 {@code capacity} 所表达的Buffer Pool数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @return {@code capacity} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    @Override
    public int capacity() {
        int total = 0;
        for (BufferPoolInstance instance : instances) {
            total += instance.capacity();
        }
        return total;
    }

    /**
     * 计算 {@code residentCount} 所表达的Buffer Pool数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @return {@code residentCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    @Override
    public int residentCount() {
        int total = 0;
        for (BufferPoolInstance instance : instances) {
            total += instance.residentCount();
        }
        return total;
    }

    /**
     * 合并各分片当前驻留页的稳定标识快照；返回列表不携带 frame、page latch 或 fix 所有权。
     *
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    @Override
    public List<PageId> residentPageIds() {
        List<PageId> all = new ArrayList<>();
        for (BufferPoolInstance instance : instances) {
            all.addAll(instance.residentPageIds());
        }
        return List.copyOf(all);
    }

    /**
     * 释放本方法拥有的Buffer Pool资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    public void close() {
        // BufferPool 不拥有 PageStore 生命周期，也不再提供 legacy flushAll 直写路径。
        // 调用方若需要 shutdown flush，必须先通过 FlushService/FlushCoordinator 完成 WAL-safe 写盘。
    }
}
