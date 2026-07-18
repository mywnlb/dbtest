package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

/**
 * 单个 buffer pool 分片（§5.2 BufferPoolInstance）：并发隔离单元，自有 pageHashLock/frameMutex + {@link PageHashTable} +
 * free list + {@link ReplacementPolicy} LRU + {@link FrameStateMachine}。它承接原单实例池的全部 per-pool 逻辑——
 * fix/new/prefetch/淘汰/写回/标脏/snapshot 刷盘/截断排空——只服务于路由到本分片的页（{@code hash(PageId)%N==thisIndex}）。
 *
 * <p><b>分片范围（13.1d）</b>：page hash 映射由 {@code pageHashLock} 保护，单帧元数据由
 * {@link BufferFrame#frameMutex} 保护；free list、LRU 与 flush list 由各自 list 锁保护。miss 读盘、future wait、
 * 脏 victim 刷盘和 legacy page write 都必须在不持任何 Buffer Pool 内部锁时进入。
 *
 * <p><b>跨切面查询由 facade 聚合</b>：dirty 候选/oldest LSN/hasDirty/residentCount/截断等需跨全部分片的操作，由
 * {@code LruBufferPool} facade 逐分片调用本类的 {@code local*} 方法并在锁外合并；facade 一次只持一把 instance 锁，
 * 不同时持两把 → 无分片间锁序、无死锁。
 *
 * <p>同时实现 {@link FrameReleaser}：本分片创建的 {@link PageGuard} 以 {@code this} 为 releaser，{@code close()} 直接回归
 * 本分片 {@link #release}（在 frameMutex 下 OR 脏并 unfix），无需路由。
 */
final class BufferPoolInstance implements FrameReleaser {

    /**
     * 本对象持有的 {@code pageStore} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PageStore pageStore;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;
    /**
     * 记录 {@code capacity} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
     */
    private final int capacity;
    /**
     * 本对象持有的 {@code policy} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final ReplacementPolicy policy;
    /** 表空间生命周期时钟；由 facade 共享注入，用于 frame 版本戳与维护窗口 admission gate。 */
    private final SpaceLifecycleClock lifecycleClock;

    /** 帧状态机：集中执行 BufferFrame.state 的合法转换（§5.7）。调用方必须已持目标 frame 的 frameMutex。 */
    private final FrameStateMachine stateMachine = new FrameStateMachine();

    /**
     * 淘汰脏 victim 时委托的 WAL 安全刷盘端口；null 表示独立/测试分片，淘汰脏帧退回 legacy snapshot flush。
     * 由 facade 在 bootstrap 期统一注入（set-once）；flushVictim 经 facade 路由回本分片，故注入同一端口即可。
     */
    private volatile DirtyVictimFlusher victimFlusher;

    /** 13.1d 子锁集合；page hash、frame、free/LRU/flush list 均有独立短锁边界。 */
    private final BufferPoolInstanceLatchSet latches = new BufferPoolInstanceLatchSet();
    /** 本分片 page hash：PageId→帧（含 LOADING 占位）。由 pageHashLock 在外保护。 */
    private final PageHashTable pageHash = new PageHashTable();
    /** 空闲 frame 队列。由 freeListLock 保护；frame 出队后才可被 miss/newPage 线程重新绑定。 */
    private final Deque<BufferFrame> freeList = new ArrayDeque<>();
    /** 本分片真实 flush list。由 flushListLock 保护，只保存 dirty 页定位与 LSN 边界，不保存 frame 引用。 */
    private final DirtyPageList dirtyPageList = new DirtyPageList();

    /** 命中 LOADING 页的等待者最长等待时长（纳秒），避免 IO owner 卡死时无限期阻塞。构造期固定。 */
    private final long loadTimeoutNanos;
    /** dirty view 变化通知回调；由 facade 注入，用于 FlushService drain 等待谓词重查。 */
    private final Runnable dirtyStateChangeListener;

    /**
     * 创建 {@code BufferPoolInstance}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param capacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param policy 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param loadTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @param lifecycleClock 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     */
    BufferPoolInstance(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy,
                        Duration loadTimeout, SpaceLifecycleClock lifecycleClock) {
        this(pageStore, pageSize, capacity, policy, loadTimeout, lifecycleClock, () -> { });
    }

    /**
     * 创建 {@code BufferPoolInstance}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pageStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param capacity 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param policy 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param loadTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @param lifecycleClock 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param dirtyStateChangeListener 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    BufferPoolInstance(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy,
                       Duration loadTimeout, SpaceLifecycleClock lifecycleClock, Runnable dirtyStateChangeListener) {
        if (pageStore == null) {
            throw new DatabaseValidationException("page store must not be null");
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
        if (capacity < 1) {
            throw new DatabaseValidationException("capacity must be >= 1: " + capacity);
        }
        if (policy == null) {
            throw new DatabaseValidationException("replacement policy must not be null");
        }
        if (loadTimeout == null || loadTimeout.isZero() || loadTimeout.isNegative()) {
            throw new DatabaseValidationException("load timeout must be positive: " + loadTimeout);
        }
        if (lifecycleClock == null) {
            throw new DatabaseValidationException("space lifecycle clock must not be null");
        }
        if (dirtyStateChangeListener == null) {
            throw new DatabaseValidationException("dirty-state change listener must not be null");
        }
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.capacity = capacity;
        this.policy = policy;
        this.lifecycleClock = lifecycleClock;
        this.dirtyStateChangeListener = dirtyStateChangeListener;
        try {
            this.loadTimeoutNanos = loadTimeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("load timeout is too large: " + loadTimeout, overflow);
        }
        for (int i = 0; i < capacity; i++) {
            freeList.add(new BufferFrame(pageSize));
        }
    }

    /** 注入淘汰脏页的 WAL 安全刷盘端口（set-once）。由 facade 在 FlushCoordinator 构造后、首 page access 前调用一次。
     *
     * @param flusher 由组合根提供的 {@code DirtyVictimFlusher} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code attachVictimFlusher} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void attachVictimFlusher(DirtyVictimFlusher flusher) {
        if (flusher == null) {
            throw new DatabaseValidationException("victim flusher must not be null");
        }
        if (this.victimFlusher != null) {
            throw new DatabaseValidationException("victim flusher already attached (set-once)");
        }
        this.victimFlusher = flusher;
    }

    /** 取页（命中或读穿），固定并取 page latch，返回 guard。**不上报 read-ahead 钩子**——钩子由 facade.getPage 路由后调。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code getPage} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    PageGuard getPage(PageId pageId, PageLatchMode mode) {
        return acquire(pageId, mode, true);
    }

    /** 页创建（不读盘的零帧）。要求 X latch；驻留则重初始化。详见 {@link BufferPool#newPage}。
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code newPage} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    PageGuard newPage(PageId pageId, PageLatchMode mode) {
        return acquire(pageId, mode, false);
    }

    /**
     * getPage/newPage 公共骨架：page hash 锁内查映射，frameMutex 内固定/检查状态；miss 时短持 free/LRU 子锁取得
     * victim。任何 PageStore IO、PageLoadFuture 等待、dirty victim flush 都在锁外执行。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param readFromDisk 当前算法是否纳入终态增量、同步压力、磁盘来源、根节点或周期上界校验；用于选择对应的不变量检查分支
     * @return {@code acquire} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws BufferPoolStalePageException 页固定、闩锁、淘汰或 frame 代际校验失败时抛出；调用方应释放已持 Guard 后重试或终止操作
     * @throws BufferPoolExhaustedException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    private PageGuard acquire(PageId pageId, PageLatchMode mode, boolean readFromDisk) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("page latch mode must not be null");
        }
        if (!readFromDisk && mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("newPage requires EXCLUSIVE latch but got " + mode);
        }
        BufferFrame chosen;
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        boolean resetAfterLatch = false;
        Set<PageId> cleanSkip = null;
        while (true) {
            TablespaceVersion admittedVersion = lifecycleClock.admitForeground(pageId.spaceId());
            PageId victimToClean = null;
            DirtyVictimFlusher victimCleaner = null;
            BufferFrame loadingVictim = null;
            TablespaceVersion loadingVersion = null;
            PageLoadFuture awaitFuture = null;
            latches.lockPageHash();
            try {
                if (!lifecycleClock.isCurrentAndOpen(pageId.spaceId(), admittedVersion)) {
                    throw new BufferPoolStalePageException("tablespace became stale before page admission: " + pageId);
                }
                BufferFrame resident = pageHash.get(pageId);
                if (resident != null) {
                    latches.lockFrame(resident);
                    try {
                        if (!admittedVersion.equals(resident.spaceVersion)) {
                            isolateStaleFrame(resident);
                            continue;
                        }
                        if (resident.state == BufferFrameState.LOADING) {
                            // 命中正在载入的页：取 load future 出锁有界等待，绝不缓存帧引用，醒来回环重查 pageHash。
                            awaitFuture = resident.loadFuture;
                        } else {
                            if (!readFromDisk) {
                                // 页创建命中驻留页：重初始化（复用帧），清零延后到取得 X latch 之后做。
                                resident.dirty = true;
                                stateMachine.transition(resident, BufferFrameState.DIRTY);
                                resetAfterLatch = true;
                            }
                            resident.spaceVersion = admittedVersion;
                            resident.fixCount++;
                            latches.lockLruList();
                            try {
                                policy.onAccess(resident);
                            } finally {
                                latches.unlockLruList();
                            }
                            chosen = resident;
                            break;
                        }
                    } finally {
                        latches.unlockFrame(resident);
                    }
                } else {
                    BufferFrame victim = null;
                    try {
                        victim = obtainVictim(cleanSkip);
                        if (victim.dirty) {
                            victimToClean = victim.pageId;
                            victimCleaner = victimFlusher;
                        } else {
                            if (victim.pageId != null) {
                                pageHash.remove(victim.pageId);
                                removeFromLru(victim);
                            }
                            if (readFromDisk) {
                                // 装 LOADING 占位：owner 自持 fixCount=1，建 load future、注册 pageHash；出锁读盘。
                                victim.pageId = pageId;
                                victim.generation++;
                                victim.spaceVersion = admittedVersion;
                                clearDirty(victim);
                                stateMachine.transition(victim, BufferFrameState.LOADING);
                                victim.fixCount = 1;
                                victim.loadFuture = new PageLoadFuture();
                                pageHash.put(pageId, victim);
                                loadingVictim = victim;
                                loadingVersion = admittedVersion;
                            } else {
                                // newPage miss：无盘 IO，清零并直接发布 CLEAN（内容清零延后到取 X latch 之后）。
                                Arrays.fill(victim.data, (byte) 0);
                                victim.pageId = pageId;
                                victim.generation++;
                                victim.spaceVersion = admittedVersion;
                                clearDirty(victim);
                                stateMachine.transition(victim, BufferFrameState.CLEAN);
                                victim.fixCount = 1;
                                pageHash.put(pageId, victim);
                                insertIntoLru(victim);
                                chosen = victim;
                                break;
                            }
                        }
                    } finally {
                        if (victim != null) {
                            latches.unlockFrame(victim);
                        }
                    }
                }
            } finally {
                latches.unlockPageHash();
            }
            if (awaitFuture != null) {
                latches.assertMetadataUnlocked("page load future wait");
                awaitFuture.await(loadTimeoutNanos, pageId);
                continue;
            }
            if (loadingVictim != null) {
                chosen = readAndPublish(pageId, loadingVictim, loadingVersion);
                break;
            }
            if (victimToClean != null) {
                latches.assertMetadataUnlocked("dirty victim flush");
                if (victimCleaner == null) {
                    throw new BufferPoolExhaustedException("dirty victim requires WAL-safe DirtyVictimFlusher: "
                            + victimToClean);
                }
                boolean cleaned = victimCleaner.flushVictim(victimToClean);
                if (!cleaned) {
                    if (cleanSkip == null) {
                        cleanSkip = new HashSet<>();
                    }
                    cleanSkip.add(victimToClean);
                }
            }
        }
        // 模式→闩：X 取 writeLock；S/SX 均取 readLock（故 SX 与 S 并发、与 X 互斥）；SX 再取 pageIntentLatch
        // 保证同页同一时刻只一个写意向者（SX 排斥 SX）。先 readLock 后 intent，intent 只在完全授予时持有。
        Lock latch = (mode == PageLatchMode.EXCLUSIVE)
                ? chosen.pageLatch.writeLock()
                : chosen.pageLatch.readLock();
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        latch.lock();
        Lock intentLatch = null;
        if (mode == PageLatchMode.SHARED_EXCLUSIVE) {
            intentLatch = chosen.pageIntentLatch;
            intentLatch.lock();
        }
        if (resetAfterLatch) {
            Arrays.fill(chosen.data, (byte) 0);
        }
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        return new PageGuard(this, chosen, mode, latch, intentLatch);
    }

    /**
     * 出 Buffer Pool 内部锁读盘并发布 CLEAN（设计 §7.1/§7.3）。owner 已装好 LOADING 占位
     *（fixCount=1、loadFuture）。
     * 读盘成功 → 重取 hash/frame/list 锁发布 CLEAN+onInsert+清 loadFuture，出锁 complete；
     * 读盘失败 → 重取锁移除占位+复位 FREE 回 free list+清 loadFuture，出锁以异常 complete 唤醒等待者，向上抛根因。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，过期映射在返回前拒绝。</li>
     *     <li>遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，慢 IO 或条件等待移到内部锁外。</li>
     *     <li>完成页载入、替换、dirty snapshot 或状态转换，并向等待者发布唯一完成或失败信号。</li>
     *     <li>返回受控 Guard/快照或释放 fix；失败回收占位且不错误清除并发产生的 dirty 状态。</li>
     * </ol>
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param loadVersion 表空间文件或 segment 的稳定身份与生命周期快照；不得为 {@code null}，必须与已打开文件和当前 generation 一致
     * @return {@code readAndPublish} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    private BufferFrame readAndPublish(PageId pageId, BufferFrame frame, TablespaceVersion loadVersion) {
        // 1、按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，在共享或持久副作用前拒绝非法状态。
        PageLoadFuture future;
        latches.lockFrame(frame);
        // 2、继续完成范围、身份与候选校验；通过后，遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，保持处理顺序与资源边界。
        try {
            future = frame.loadFuture;
        } finally {
            latches.unlockFrame(frame);
        }
        try {
            latches.assertMetadataUnlocked("page read");
            pageStore.readPage(pageId, ByteBuffer.wrap(frame.data));
        } catch (RuntimeException loadError) {
            latches.lockPageHash();
            try {
                latches.lockFrame(frame);
                try {
                    pageHash.remove(pageId);
                    resetFrameToFree(frame);
                } finally {
                    latches.unlockFrame(frame);
                }
            } finally {
                latches.unlockPageHash();
            }
            future.failExceptionally(loadError);
            throw loadError;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，完成页载入、替换、dirty snapshot 或状态转换，并维持领域不变量。
        latches.lockPageHash();
        try {
            latches.lockFrame(frame);
            try {
                if (!lifecycleClock.isCurrentAndOpen(pageId.spaceId(), loadVersion)) {
                    BufferPoolStalePageException stale = new BufferPoolStalePageException(
                            "page load became stale before publish: " + pageId);
                    pageHash.remove(pageId);
                    resetFrameToFree(frame);
                    future.failExceptionally(stale);
                    throw stale;
                }
                stateMachine.transition(frame, BufferFrameState.CLEAN);
                insertIntoLru(frame);
                frame.loadFuture = null;
            } finally {
                latches.unlockFrame(frame);
            }
        } finally {
            latches.unlockPageHash();
        }
        future.complete();
        // 4、返回受控 Guard/快照或释放 fix，以稳定返回或领域异常完成收口。
        return frame;
    }

    /**
     * 取受害者帧：优先空闲帧；否则复制 LRU victim 顺序后释放 LRU 锁，再逐帧持 frameMutex 复核，避免在持
     * {@code lruListLock} 时等待单帧锁。若无干净未固定帧则返回首个未在本轮 {@code cleanSkip} 中的脏未固定帧，
     * 交调用方出锁刷干净后复用。返回时目标 frameMutex 仍由当前线程持有，调用方负责释放。
     */
    private BufferFrame obtainVictim(Set<PageId> cleanSkip) {
        BufferFrame free = pollFreeFrame();
        if (free != null) {
            latches.lockFrame(free);
            return free;
        }
        BufferFrame firstDirty = null;
        for (BufferFrame frame : victimOrderSnapshot()) {
            latches.lockFrame(frame);
            if (frame.pageId == null || frame.state == BufferFrameState.FREE
                    || frame.state == BufferFrameState.LOADING || frame.fixCount != 0) {
                latches.unlockFrame(frame);
                continue;
            }
            if (frame.state == BufferFrameState.FLUSHING) {
                latches.unlockFrame(frame);
                continue;
            }
            if (!frame.dirty) {
                if (firstDirty != null) {
                    latches.unlockFrame(firstDirty);
                }
                return frame;
            }
            if (firstDirty == null && (cleanSkip == null || !cleanSkip.contains(frame.pageId))) {
                firstDirty = frame;
            } else {
                latches.unlockFrame(frame);
            }
        }
        if (firstDirty != null) {
            return firstDirty;
        }
        throw new BufferPoolExhaustedException("buffer pool instance exhausted: all " + capacity
                + " frames are fixed or pending durable flush");
    }

    /**
     * 更新 {@code markWritePending} 指定的Buffer Pool局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void markWritePending(BufferFrame frame) {
        if (frame == null) {
            throw new DatabaseValidationException("pending frame must not be null");
        }
        latches.lockFrame(frame);
        try {
            if (frame.state == BufferFrameState.CLEAN) {
                stateMachine.transition(frame, BufferFrameState.DIRTY_PENDING);
            }
        } finally {
            latches.unlockFrame(frame);
        }
    }

    /**
     * 释放本方法拥有的Buffer Pool资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param wrote 既有物理或缓存步骤是否已经发生；该事实用于避免重复修改、重复补偿或错误发布中间状态
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void release(BufferFrame frame, boolean wrote) {
        latches.lockFrame(frame);
        try {
            if (frame.fixCount <= 0) {
                throw new DatabaseValidationException("buffer frame release without active fix: " + frame.pageId);
            }
            if (wrote) {
                markDirty(frame);
            } else if (frame.state == BufferFrameState.DIRTY_PENDING) {
                stateMachine.transition(frame, BufferFrameState.CLEAN);
            }
            frame.fixCount--;
            latches.signalFrameReleased();
            dirtyStateChangeListener.run();
        } finally {
            latches.unlockFrame(frame);
        }
    }

    /** Read-ahead 预取（§8.1）：未驻留且有空闲帧则载入 old 子链——不 fix、不提升。详见 {@link BufferPool#prefetch}。
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void prefetch(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        TablespaceVersion admittedVersion = lifecycleClock.admitPrefetchOrNull(pageId.spaceId());
        if (admittedVersion == null) {
            return;
        }
        BufferFrame loading;
        latches.lockPageHash();
        try {
            BufferFrame resident = pageHash.get(pageId);
            if (resident != null) {
                latches.lockFrame(resident);
                try {
                    if (!admittedVersion.equals(resident.spaceVersion)) {
                        isolateStaleFrame(resident);
                    } else {
                        return; // 已驻留或正在载入。
                    }
                } finally {
                    latches.unlockFrame(resident);
                }
            }
            if (!lifecycleClock.isCurrentAndOpen(pageId.spaceId(), admittedVersion)) {
                return;
            }
            BufferFrame free = pollFreeFrame();
            if (free == null) {
                return; // 无空闲帧：read-ahead 直接丢弃，绝不淘汰脏页或挤占前台需求读。
            }
            latches.lockFrame(free);
            try {
                free.pageId = pageId;
                free.generation++;
                free.spaceVersion = admittedVersion;
                clearDirty(free);
                stateMachine.transition(free, BufferFrameState.LOADING);
                free.fixCount = 1;
                free.loadFuture = new PageLoadFuture();
                pageHash.put(pageId, free);
                loading = free;
            } finally {
                latches.unlockFrame(free);
            }
        } finally {
            latches.unlockPageHash();
        }
        BufferFrame published;
        try {
            published = readAndPublish(pageId, loading, admittedVersion);
        } catch (RuntimeException loadError) {
            return; // read-ahead 尽力而为：载入失败丢弃，占位已回收。
        }
        latches.lockFrame(published);
        try {
            published.fixCount--; // 立即 unfix 使预取页成为 old 冷页；不 onAccess（未被真实访问、不提升）。
            latches.signalFrameReleased();
        } finally {
            latches.unlockFrame(published);
        }
    }

    /**
     * 本分片的 flush list 候选（oldest≤targetLsn，按 oldest 升序，≤maxPages）。候选表达 dirty view，不承诺当前可
     * snapshot；fixed 页也必须暴露给 drain，真正能否写盘由 {@link #snapshotForFlush(PageId)} 在短锁内复核。
     * @param targetLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param maxPages 参与 {@code localDirtyPageCandidates} 的上界或规格值 {@code maxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    List<DirtyPageCandidate> localDirtyPageCandidates(Lsn targetLsn, int maxPages) {
        List<DirtyPageCandidate> candidates = new ArrayList<>();
        for (DirtyPageCandidate candidate : dirtyCandidateSnapshot(targetLsn, capacity)) {
            BufferFrame frame;
            latches.lockPageHash();
            try {
                frame = pageHash.get(candidate.pageId());
                if (frame == null) {
                    continue;
                }
                latches.lockFrame(frame);
            } finally {
                latches.unlockPageHash();
            }
            try {
                if (frame.dirty && frame.state == BufferFrameState.DIRTY
                        && frame.oldestModificationLsn != null
                        && frame.oldestModificationLsn.value() <= targetLsn.value()) {
                    candidates.add(new DirtyPageCandidate(frame.pageId,
                            frame.oldestModificationLsn, frame.newestModificationLsn));
                }
            } finally {
                latches.unlockFrame(frame);
            }
        }
        return candidates.stream()
                .sorted(Comparator.comparingLong(candidate -> candidate.oldestModificationLsn().value()))
                .limit(maxPages)
                .collect(Collectors.toList());
    }

    /** 复制 LRU dirty 尾部候选；只返回当前未 fixed 且仍为 DIRTY 的 frame。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，过期映射在返回前拒绝。</li>
     *     <li>遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，慢 IO 或条件等待移到内部锁外。</li>
     *     <li>完成页载入、替换、dirty snapshot 或状态转换，并向等待者发布唯一完成或失败信号。</li>
     *     <li>返回受控 Guard/快照或释放 fix；失败回收占位且不错误清除并发产生的 dirty 状态。</li>
     * </ol>
     *
     * @param maxPages 参与 {@code localLruDirtyPageCandidates} 的上界或规格值 {@code maxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    List<DirtyPageCandidate> localLruDirtyPageCandidates(int maxPages) {
        // 1、按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，在共享或持久副作用前拒绝非法状态。
        if (maxPages < 0) {
            throw new DatabaseValidationException("lru dirty max pages must not be negative: " + maxPages);
        }
        // 2、继续完成范围、身份与候选校验；通过后，遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，保持处理顺序与资源边界。
        if (maxPages == 0) {
            return List.of();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，完成页载入、替换、dirty snapshot 或状态转换，并维持领域不变量。
        List<DirtyPageCandidate> result = new ArrayList<>();
        for (BufferFrame frame : victimOrderSnapshot()) {
            latches.lockFrame(frame);
            try {
                if (frame.state == BufferFrameState.DIRTY && frame.dirty && frame.fixCount == 0
                        && frame.pageId != null && frame.oldestModificationLsn != null) {
                    result.add(new DirtyPageCandidate(frame.pageId, frame.oldestModificationLsn,
                            frame.newestModificationLsn));
                    if (result.size() == maxPages) {
                        break;
                    }
                }
            } finally {
                latches.unlockFrame(frame);
            }
        }
        // 4、返回受控 Guard/快照或释放 fix，以稳定返回或领域异常完成收口。
        return List.copyOf(result);
    }

    /**
     * 计算 {@code freeFrameCount} 所表达的Buffer Pool数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @return {@code freeFrameCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    int freeFrameCount() {
        latches.lockFreeList();
        try {
            return freeList.size();
        } finally {
            latches.unlockFreeList();
        }
    }

    /** 尝试复制一个未 fixed 的脏页镜像；DIRTY→FLUSHING。详见 {@link BufferPool#snapshotForFlush}。
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    Optional<FlushPageSnapshot> snapshotForFlush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        latches.lockPageHash();
        try {
            BufferFrame frame = pageHash.get(pageId);
            if (frame == null) {
                return Optional.empty();
            }
            latches.lockFrame(frame);
            try {
                if (frame.state != BufferFrameState.DIRTY || frame.fixCount != 0) {
                    return Optional.empty();
                }
                byte[] image = Arrays.copyOf(frame.data, frame.data.length);
                stateMachine.transition(frame, BufferFrameState.FLUSHING);
                return Optional.of(new FlushPageSnapshot(pageId,
                        frame.newestModificationLsn, frame.dirtyVersion, frame.generation, image));
            } finally {
                latches.unlockFrame(frame);
            }
        } finally {
            latches.unlockPageHash();
        }
    }

    /** flush 成功回调：版本符→CLEAN 清脏返回 true，不符→DIRTY 保脏返回 false。详见 {@link BufferPool#completeFlush}。
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code completeFlush} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    boolean completeFlush(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        latches.lockPageHash();
        try {
            BufferFrame frame = pageHash.get(snapshot.pageId());
            if (frame == null) {
                return false;
            }
            latches.lockFrame(frame);
            try {
                if (frame.state != BufferFrameState.FLUSHING) {
                    return false;
                }
                if (frame.fixCount == 0
                        && frame.dirtyVersion == snapshot.dirtyVersion()
                        && (snapshot.generation() == 0L || frame.generation == snapshot.generation())
                        && frame.newestModificationLsn.equals(snapshot.pageLsn())) {
                    clearDirty(frame);
                    stateMachine.transition(frame, BufferFrameState.CLEAN);
                    dirtyStateChangeListener.run();
                    return true;
                }
                stateMachine.transition(frame, BufferFrameState.DIRTY);
                dirtyStateChangeListener.run();
                return false;
            } finally {
                latches.unlockFrame(frame);
            }
        } finally {
            latches.unlockPageHash();
        }
    }

    /** flush 失败回调：FLUSHING→DIRTY 保留脏页待重刷。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void failFlush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        latches.lockPageHash();
        try {
            BufferFrame frame = pageHash.get(pageId);
            if (frame != null) {
                latches.lockFrame(frame);
                try {
                    if (frame.state == BufferFrameState.FLUSHING) {
                        stateMachine.transition(frame, BufferFrameState.DIRTY);
                        dirtyStateChangeListener.run();
                    }
                } finally {
                    latches.unlockFrame(frame);
                }
            }
        } finally {
            latches.unlockPageHash();
        }
    }

    /** 本分片最老 dirty LSN；无脏页返回 null（facade 跨分片取全局 min）。
     *
     * @return {@code localOldestDirtyLsnOrNull} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    Lsn localOldestDirtyLsnOrNull() {
        latches.lockFlushList();
        try {
            return dirtyPageList.oldestDirtyLsnOrNull();
        } finally {
            latches.unlockFlushList();
        }
    }

    /** 本分片是否存在任何 dirty frame。
     *
     * @return {@code hasDirtyPages} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    boolean hasDirtyPages() {
        latches.lockFlushList();
        try {
            return dirtyPageList.hasDirtyPages();
        } finally {
            latches.unlockFlushList();
        }
    }

    /**
     * 截断排空第一阶段（不移除）：在共享 deadline 内等待本分片该空间全部 fix 归零，再校验无脏帧。任一脏帧抛
     * {@link DirtyTablespaceInvalidationException}、超时/中断抛 {@link BufferPoolInvalidationTimeoutException}——
     * <b>此时尚未移除任何帧</b>，使 facade 能在任一分片失败时整体放弃，避免部分失效。
     *
     * @param spaceId      目标表空间。
     * @param deadlineNanos {@link System#nanoTime()} 基准的绝对截止时刻，全部分片共享。
     * @throws BufferPoolInvalidationTimeoutException 操作在约定时限内无法完成时抛出；调用方可回滚或稍后重试
     * @throws DirtyTablespaceInvalidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void awaitDrainedAndCheckClean(SpaceId spaceId, long deadlineNanos) {
        while (hasFixedFrame(spaceId)) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                throw new BufferPoolInvalidationTimeoutException(
                        "timed out waiting fixed frames for tablespace " + spaceId.value());
            }
            try {
                latches.awaitFrameReleased(remaining);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new BufferPoolInvalidationTimeoutException(
                        "interrupted waiting fixed frames for tablespace " + spaceId.value(), interrupted);
            }
        }
        for (BufferFrame frame : snapshotFrames()) {
            latches.lockFrame(frame);
            try {
                if (frame.pageId.spaceId().equals(spaceId) && frame.dirty) {
                    throw new DirtyTablespaceInvalidationException(
                            "dirty frame blocks tablespace invalidation: " + frame.pageId);
                }
            } finally {
                latches.unlockFrame(frame);
            }
        }
    }

    /**
     * 截断排空第二阶段（移除）：把本分片该空间全部帧从 pageHash/LRU 移除并复位回 free list。仅在
     * {@link #awaitDrainedAndCheckClean} 对**所有**分片通过后由 facade 调用；依赖调用方持 tablespace X lease 阻断新流量，
     * 故第一阶段确认的 fix=0/clean 在两阶段之间保持不变。
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     */
    void removeTablespaceFrames(SpaceId spaceId) {
        latches.lockPageHash();
        try {
            List<BufferFrame> targets = new ArrayList<>();
            for (BufferFrame frame : pageHash.values()) {
                latches.lockFrame(frame);
                try {
                    if (frame.pageId.spaceId().equals(spaceId)) {
                        targets.add(frame);
                    }
                } finally {
                    latches.unlockFrame(frame);
                }
            }
            for (BufferFrame frame : targets) {
                latches.lockFrame(frame);
                try {
                    pageHash.remove(frame.pageId);
                    removeFromLru(frame);
                    resetFrameToFree(frame);
                } finally {
                    latches.unlockFrame(frame);
                }
            }
        } finally {
            latches.unlockPageHash();
        }
    }

    /** 逐帧短锁检查是否仍有目标表空间 frame 被 fix。 */
    private boolean hasFixedFrame(SpaceId spaceId) {
        for (BufferFrame frame : snapshotFrames()) {
            latches.lockFrame(frame);
            try {
                if (frame.pageId.spaceId().equals(spaceId) && frame.fixCount > 0) {
                    return true;
                }
            } finally {
                latches.unlockFrame(frame);
            }
        }
        return false;
    }

    /** 本分片帧容量。 */
    int capacity() {
        return capacity;
    }

    /** 本分片当前驻留帧数。
     *
     * @return {@code residentCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    int residentCount() {
        latches.lockPageHash();
        try {
            return pageHash.size();
        } finally {
            latches.unlockPageHash();
        }
    }

    /** 本分片驻留页 PageId 不可变快照。
     *
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    List<PageId> residentPageIds() {
        latches.lockPageHash();
        try {
            return List.copyOf(pageHash.keySet());
        } finally {
            latches.unlockPageHash();
        }
    }

    /** 本分片在某连续页区间内的驻留页数。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param firstPageNo 参与 {@code countResidentInRange} 的原始数值身份 {@code firstPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param pageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @return {@code countResidentInRange} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    int countResidentInRange(SpaceId spaceId, long firstPageNo, int pageCount) {
        latches.lockPageHash();
        try {
            return pageHash.countInRange(spaceId, firstPageNo, pageCount);
        } finally {
            latches.unlockPageHash();
        }
    }

    /** 在 pageHashLock 下复制当前 frame 引用快照，调用方再逐个持 frameMutex 校验其当下状态。 */
    private List<BufferFrame> snapshotFrames() {
        latches.lockPageHash();
        try {
            return List.copyOf(pageHash.values());
        } finally {
            latches.unlockPageHash();
        }
    }

    /** 从 free list 取一个空闲 frame；不持有 frameMutex 返回，调用方随后独占重新绑定。
     *
     * @return {@code pollFreeFrame} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    private BufferFrame pollFreeFrame() {
        latches.lockFreeList();
        try {
            return freeList.poll();
        } finally {
            latches.unlockFreeList();
        }
    }

    /** 把已清空绑定关系的 frame 放回 free list；调用方必须已持 frameMutex 并完成状态复位。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    private void addFreeFrame(BufferFrame frame) {
        latches.lockFreeList();
        try {
            freeList.add(frame);
        } finally {
            latches.unlockFreeList();
        }
    }

    /** LRU 插入短临界区；调用方已完成 page hash 注册和 frame 状态初始化。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    private void insertIntoLru(BufferFrame frame) {
        latches.lockLruList();
        try {
            policy.onInsert(frame);
        } finally {
            latches.unlockLruList();
        }
    }

    /** LRU 移除短临界区；调用方已持 frameMutex，确保 frame 不会被并发重绑定。 */
    private void removeFromLru(BufferFrame frame) {
        latches.lockLruList();
        try {
            policy.onRemove(frame);
        } finally {
            latches.unlockLruList();
        }
    }

    /** 复制 LRU victim 顺序；释放 LRU 锁后再逐帧加 frameMutex 复核，避免 list 锁等待 frame 锁。 */
    private List<BufferFrame> victimOrderSnapshot() {
        latches.lockLruList();
        try {
            List<BufferFrame> order = new ArrayList<>();
            for (BufferFrame frame : policy.victimOrder()) {
                order.add(frame);
            }
            return order;
        } finally {
            latches.unlockLruList();
        }
    }

    /** 复制 flush list 候选；调用方随后必须重新通过 page hash + frameMutex 复核。 */
    private List<DirtyPageCandidate> dirtyCandidateSnapshot(Lsn targetLsn, int maxPages) {
        latches.lockFlushList();
        try {
            return dirtyPageList.candidatesUpTo(targetLsn, maxPages);
        } finally {
            latches.unlockFlushList();
        }
    }

    /** 调用须持目标 frameMutex；把 frame 当前 dirty LSN 边界发布到 flush list。
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    private void updateDirtyList(BufferFrame frame) {
        latches.lockFlushList();
        try {
            dirtyPageList.upsert(frame.pageId, frame.oldestModificationLsn, frame.newestModificationLsn);
        } finally {
            latches.unlockFlushList();
        }
    }

    /** 从 flush list 摘除指定页；pageId 为 null 说明 frame 已经是 FREE 或尚未绑定，直接忽略。 */
    private void removeFromDirtyList(PageId pageId) {
        if (pageId == null) {
            return;
        }
        latches.lockFlushList();
        try {
            dirtyPageList.remove(pageId);
        } finally {
            latches.unlockFlushList();
        }
    }

    private Lsn pageLsn(BufferFrame frame) {
        return Lsn.of(ByteBuffer.wrap(frame.data).getLong(PageEnvelopeLayout.PAGE_LSN));
    }

    /** 调用须持目标 frameMutex。详见原单实例池 markDirty 的 oldestModificationLsn==null 守卫说明。 */
    private void markDirty(BufferFrame frame) {
        Lsn pageLsn = pageLsn(frame);
        // oldestModificationLsn = 自上次刷盘以来最早的修改 LSN：只要当前未设置就设为本次 pageLSN。
        // 不能用 `!frame.dirty` 作条件——newPage 对驻留页重初始化会先置 dirty=true 而无 LSN，
        // 否则 commit 的 markDirty 会因 dirty 已真而漏设 oldestMod，留 dirty+null oldestMod 帧致 flush/checkpoint NPE。
        if (frame.oldestModificationLsn == null) {
            frame.oldestModificationLsn = pageLsn;
        }
        frame.newestModificationLsn = pageLsn;
        frame.dirtyVersion++;
        frame.dirty = true;
        updateDirtyList(frame);
        if (frame.state != BufferFrameState.FLUSHING) {
            stateMachine.transition(frame, BufferFrameState.DIRTY);
        }
    }

    /** 调用须持目标 frameMutex。 */
    private void clearDirty(BufferFrame frame) {
        removeFromDirtyList(frame.pageId);
        frame.dirty = false;
        frame.oldestModificationLsn = null;
        frame.newestModificationLsn = null;
    }

    /** 调用须持 pageHashLock 与目标 frameMutex。隔离旧表空间版本的 clean/unfixed frame，普通路径随后按 miss 重试。 */
    private void isolateStaleFrame(BufferFrame frame) {
        if (frame.fixCount != 0 || frame.dirty) {
            throw new BufferPoolStalePageException("stale frame is still fixed or dirty: " + frame.pageId);
        }
        stateMachine.transition(frame, BufferFrameState.STALE);
        pageHash.remove(frame.pageId);
        removeFromLru(frame);
        resetFrameToFree(frame);
    }

    /** 调用须持目标 frameMutex。把 clean/LOADING frame 复位回 free list，供载入失败、stale 和 invalidate 复用。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，过期映射在返回前拒绝。</li>
     *     <li>遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，慢 IO 或条件等待移到内部锁外。</li>
     *     <li>完成页载入、替换、dirty snapshot 或状态转换，并向等待者发布唯一完成或失败信号。</li>
     *     <li>返回受控 Guard/快照或释放 fix；失败回收占位且不错误清除并发产生的 dirty 状态。</li>
     * </ol>
     *
     * @param frame 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     */
    private void resetFrameToFree(BufferFrame frame) {
        // 1、按 PageId 路由分片并读取 page hash、frame 代际与生命周期状态，在共享或持久副作用前拒绝非法状态。
        clearDirty(frame);
        frame.pageId = null;
        // 2、继续完成范围、身份与候选校验；通过后，遵守 pageHashLock、frameMutex、列表锁与 page latch 顺序固定 frame，保持处理顺序与资源边界。
        frame.spaceVersion = null;
        if (frame.state != BufferFrameState.FREE) {
            stateMachine.transition(frame, BufferFrameState.FREE);
        }
        frame.fixCount = 0;
        // 3、在中间分支复核阶段性结果；满足条件后，完成页载入、替换、dirty snapshot 或状态转换，并维持领域不变量。
        frame.loadFuture = null;
        addFreeFrame(frame);
        latches.signalFrameReleased();
        // 4、返回受控 Guard/快照或释放 fix，以稳定返回或领域异常完成收口。
        dirtyStateChangeListener.run();
    }
}
