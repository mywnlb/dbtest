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

    private final PageStore pageStore;
    private final PageSize pageSize;
    private final int capacity;
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

    BufferPoolInstance(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy,
                        Duration loadTimeout, SpaceLifecycleClock lifecycleClock) {
        this(pageStore, pageSize, capacity, policy, loadTimeout, lifecycleClock, () -> { });
    }

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

    /** 注入淘汰脏页的 WAL 安全刷盘端口（set-once）。由 facade 在 FlushCoordinator 构造后、首 page access 前调用一次。 */
    void attachVictimFlusher(DirtyVictimFlusher flusher) {
        if (flusher == null) {
            throw new DatabaseValidationException("victim flusher must not be null");
        }
        if (this.victimFlusher != null) {
            throw new DatabaseValidationException("victim flusher already attached (set-once)");
        }
        this.victimFlusher = flusher;
    }

    /** 取页（命中或读穿），固定并取 page latch，返回 guard。**不上报 read-ahead 钩子**——钩子由 facade.getPage 路由后调。 */
    PageGuard getPage(PageId pageId, PageLatchMode mode) {
        return acquire(pageId, mode, true);
    }

    /** 页创建（不读盘的零帧）。要求 X latch；驻留则重初始化。详见 {@link BufferPool#newPage}。 */
    PageGuard newPage(PageId pageId, PageLatchMode mode) {
        return acquire(pageId, mode, false);
    }

    /**
     * getPage/newPage 公共骨架：page hash 锁内查映射，frameMutex 内固定/检查状态；miss 时短持 free/LRU 子锁取得
     * victim。任何 PageStore IO、PageLoadFuture 等待、dirty victim flush 都在锁外执行。
     */
    private PageGuard acquire(PageId pageId, PageLatchMode mode, boolean readFromDisk) {
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
                boolean cleaned = (victimCleaner == null)
                        ? flushLegacyPage(victimToClean)
                        : victimCleaner.flushVictim(victimToClean);
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
        latch.lock();
        Lock intentLatch = null;
        if (mode == PageLatchMode.SHARED_EXCLUSIVE) {
            intentLatch = chosen.pageIntentLatch;
            intentLatch.lock();
        }
        if (resetAfterLatch) {
            Arrays.fill(chosen.data, (byte) 0);
        }
        return new PageGuard(this, chosen, mode, latch, intentLatch);
    }

    /**
     * 出 Buffer Pool 内部锁读盘并发布 CLEAN（设计 §7.1/§7.3）。owner 已装好 LOADING 占位
     *（fixCount=1、loadFuture）。
     * 读盘成功 → 重取 hash/frame/list 锁发布 CLEAN+onInsert+清 loadFuture，出锁 complete；
     * 读盘失败 → 重取锁移除占位+复位 FREE 回 free list+清 loadFuture，出锁以异常 complete 唤醒等待者，向上抛根因。
     */
    private BufferFrame readAndPublish(PageId pageId, BufferFrame frame, TablespaceVersion loadVersion) {
        PageLoadFuture future;
        latches.lockFrame(frame);
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

    @Override
    public void release(BufferFrame frame, boolean wrote) {
        latches.lockFrame(frame);
        try {
            if (frame.fixCount <= 0) {
                throw new DatabaseValidationException("buffer frame release without active fix: " + frame.pageId);
            }
            if (wrote) {
                markDirty(frame);
            }
            frame.fixCount--;
            latches.signalFrameReleased();
            dirtyStateChangeListener.run();
        } finally {
            latches.unlockFrame(frame);
        }
    }

    /** Read-ahead 预取（§8.1）：未驻留且有空闲帧则载入 old 子链——不 fix、不提升。详见 {@link BufferPool#prefetch}。 */
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
     * 若该页驻留、未 fix 且为脏，则经 snapshot 协议写回 PageStore。
     *
     * <p>这是 legacy API：不提供 WAL gate / doublewrite 承诺；生产 WAL-safe flush 仍由 flush 模块调用
     * {@link #snapshotForFlush(PageId)} 后完成。本方法只保证不在 Buffer Pool 内部锁下进入物理 IO，且通过
     * dirtyVersion 防止 snapshot 后的新修改被误清。
     */
    void flush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        flushLegacyPage(pageId);
    }

    /**
     * 写回本分片所有当前可刷的 legacy 脏页。
     *
     * <p>每轮只在短锁内复制一批 {@code DIRTY && fixCount==0} 的 page id，随后逐页出锁 snapshot
     * 和写盘。若写盘期间页面再次变脏，{@link #completeFlush(FlushPageSnapshot)} 会保留 dirty，下一轮重新尝试。
     */
    void flushAll() {
        while (true) {
            List<PageId> candidates = localLegacyFlushCandidates();
            if (candidates.isEmpty()) {
                return;
            }
            boolean attempted = false;
            for (PageId candidate : candidates) {
                attempted |= flushLegacyPage(candidate);
            }
            if (!attempted) {
                return;
            }
        }
    }

    /**
     * legacy 同步 flush 的页写出 helper。数据流：先通过 {@link #snapshotForFlush(PageId)} 在短 metadata
     * 临界区内复制稳定页镜像并把 frame 标为 FLUSHING；随后释放所有 Buffer Pool metadata 锁进入 PageStore；
     * 成功后用 dirtyVersion/pageLSN 回调 {@link #completeFlush(FlushPageSnapshot)}，失败时用 {@link #failFlush(PageId)}
     * 复位为 DIRTY 并原样抛出领域异常。
     *
     * @return true 表示本轮确实拿到 snapshot 并写出；false 表示页不存在、已 clean、fixed、FLUSHING 或写出期间又变脏。
     */
    private boolean flushLegacyPage(PageId pageId) {
        Optional<FlushPageSnapshot> snapshot = snapshotForFlush(pageId);
        if (snapshot.isEmpty()) {
            return false;
        }
        FlushPageSnapshot image = snapshot.orElseThrow();
        try {
            latches.assertMetadataUnlocked("legacy page write");
            pageStore.writePage(image.pageId(), ByteBuffer.wrap(image.pageImage()));
            return completeFlush(image);
        } catch (RuntimeException writeError) {
            failFlush(image.pageId());
            throw writeError;
        }
    }

    /** 短锁复制当前可刷 legacy dirty 页定位；不携带 frame 引用，不跨 IO 持锁。 */
    private List<PageId> localLegacyFlushCandidates() {
        List<PageId> candidates = new ArrayList<>();
        for (BufferFrame frame : snapshotFrames()) {
            latches.lockFrame(frame);
            try {
                if (frame.state == BufferFrameState.DIRTY && frame.fixCount == 0) {
                    candidates.add(frame.pageId);
                }
            } finally {
                latches.unlockFrame(frame);
            }
        }
        return candidates;
    }

    /**
     * 本分片的 flush list 候选（oldest≤targetLsn，按 oldest 升序，≤maxPages）。候选表达 dirty view，不承诺当前可
     * snapshot；fixed 页也必须暴露给 drain，真正能否写盘由 {@link #snapshotForFlush(PageId)} 在短锁内复核。
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

    /** 尝试复制一个未 fixed 的脏页镜像；DIRTY→FLUSHING。详见 {@link BufferPool#snapshotForFlush}。 */
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
                        frame.newestModificationLsn, frame.dirtyVersion, image));
            } finally {
                latches.unlockFrame(frame);
            }
        } finally {
            latches.unlockPageHash();
        }
    }

    /** flush 成功回调：版本符→CLEAN 清脏返回 true，不符→DIRTY 保脏返回 false。详见 {@link BufferPool#completeFlush}。 */
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

    /** flush 失败回调：FLUSHING→DIRTY 保留脏页待重刷。 */
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

    /** 本分片最老 dirty LSN；无脏页返回 null（facade 跨分片取全局 min）。 */
    Lsn localOldestDirtyLsnOrNull() {
        latches.lockFlushList();
        try {
            return dirtyPageList.oldestDirtyLsnOrNull();
        } finally {
            latches.unlockFlushList();
        }
    }

    /** 本分片是否存在任何 dirty frame。 */
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

    /** 本分片当前驻留帧数。 */
    int residentCount() {
        latches.lockPageHash();
        try {
            return pageHash.size();
        } finally {
            latches.unlockPageHash();
        }
    }

    /** 本分片驻留页 PageId 不可变快照。 */
    List<PageId> residentPageIds() {
        latches.lockPageHash();
        try {
            return List.copyOf(pageHash.keySet());
        } finally {
            latches.unlockPageHash();
        }
    }

    /** 本分片在某连续页区间内的驻留页数。 */
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

    /** 从 free list 取一个空闲 frame；不持有 frameMutex 返回，调用方随后独占重新绑定。 */
    private BufferFrame pollFreeFrame() {
        latches.lockFreeList();
        try {
            return freeList.poll();
        } finally {
            latches.unlockFreeList();
        }
    }

    /** 把已清空绑定关系的 frame 放回 free list；调用方必须已持 frameMutex 并完成状态复位。 */
    private void addFreeFrame(BufferFrame frame) {
        latches.lockFreeList();
        try {
            freeList.add(frame);
        } finally {
            latches.unlockFreeList();
        }
    }

    /** LRU 插入短临界区；调用方已完成 page hash 注册和 frame 状态初始化。 */
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

    /** 调用须持目标 frameMutex；把 frame 当前 dirty LSN 边界发布到 flush list。 */
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
        pageHash.remove(frame.pageId);
        removeFromLru(frame);
        resetFrameToFree(frame);
    }

    /** 调用须持目标 frameMutex。把 clean/LOADING frame 复位回 free list，供载入失败、stale 和 invalidate 复用。 */
    private void resetFrameToFree(BufferFrame frame) {
        clearDirty(frame);
        frame.pageId = null;
        frame.spaceVersion = null;
        if (frame.state != BufferFrameState.FREE) {
            stateMachine.transition(frame, BufferFrameState.FREE);
        }
        frame.fixCount = 0;
        frame.loadFuture = null;
        addFreeFrame(frame);
        latches.signalFrameReleased();
        dirtyStateChangeListener.run();
    }
}
