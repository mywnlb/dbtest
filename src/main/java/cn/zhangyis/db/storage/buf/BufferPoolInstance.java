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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 单个 buffer pool 分片（§5.2 BufferPoolInstance）：并发隔离单元，自有 {@code instanceLock} + {@link PageHashTable} +
 * free list + {@link ReplacementPolicy} LRU + {@link FrameStateMachine}。它承接原单实例池的全部 per-pool 逻辑——
 * fix/new/prefetch/淘汰/写回/标脏/snapshot 刷盘/截断排空——只服务于路由到本分片的页（{@code hash(PageId)%N==thisIndex}）。
 *
 * <p><b>分片范围（本片采用单 instance 锁）</b>：本分片所有短临界区都由<b>一把</b> {@code instanceLock} 串保护
 * （不拆 §13.1 的 pageHashLock/freeListLock/lruListLock 子锁，留教学后续）；miss 读盘与脏 victim 刷盘仍在锁外
 * （per-frame LOADING 占位 + {@link PageLoadFuture}，§7.1/§7.3）。{@code BufferPoolRouter} 保证同一 {@link PageId} 恒落同一
 * 分片，故单页生命周期（fix→latch→release→淘汰）全在本分片内自洽，无需跨分片协调。
 *
 * <p><b>跨切面查询由 facade 聚合</b>：dirty 候选/oldest LSN/hasDirty/residentCount/截断等需跨全部分片的操作，由
 * {@code LruBufferPool} facade 逐分片调用本类的 {@code local*} 方法并在锁外合并；facade 一次只持一把 instance 锁，
 * 不同时持两把 → 无分片间锁序、无死锁。
 *
 * <p>同时实现 {@link FrameReleaser}：本分片创建的 {@link PageGuard} 以 {@code this} 为 releaser，{@code close()} 直接回归
 * 本分片 {@link #release}（在 {@code instanceLock} 下 OR 脏并 unfix），无需路由。
 */
final class BufferPoolInstance implements FrameReleaser {

    private final PageStore pageStore;
    private final PageSize pageSize;
    private final int capacity;
    private final ReplacementPolicy policy;

    /** 帧状态机：集中执行 BufferFrame.state 的合法转换（§5.7）。所有调用在 instanceLock 下串行，故无需自身加锁。 */
    private final FrameStateMachine stateMachine = new FrameStateMachine();

    /**
     * 淘汰脏 victim 时委托的 WAL 安全刷盘端口；null 表示独立/测试分片，淘汰脏帧退回 legacy {@link #writeBack}。
     * 由 facade 在 bootstrap 期统一注入（set-once）；flushVictim 经 facade 路由回本分片，故注入同一端口即可。
     */
    private volatile DirtyVictimFlusher victimFlusher;

    /** 保护本分片 pageHash / freeList / policy / 各帧 pageId·dirty·fixCount·state·loadFuture 的短临界区；盘 IO 在锁外。 */
    private final ReentrantLock instanceLock = new ReentrantLock();
    /** release() 在 fixCount 下降时唤醒截断排空；条件始终由 instanceLock 保护。 */
    private final Condition frameReleased = instanceLock.newCondition();
    /** 本分片 page hash：PageId→帧（含 LOADING 占位）。由 instanceLock 在外保护。 */
    private final PageHashTable pageHash = new PageHashTable();
    private final Deque<BufferFrame> freeList = new ArrayDeque<>();

    /** 命中 LOADING 页的等待者最长等待时长（纳秒），避免 IO owner 卡死时无限期阻塞。构造期固定。 */
    private final long loadTimeoutNanos;

    BufferPoolInstance(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy,
                       Duration loadTimeout) {
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
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.capacity = capacity;
        this.policy = policy;
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
        instanceLock.lock();
        try {
            if (this.victimFlusher != null) {
                throw new DatabaseValidationException("victim flusher already attached (set-once)");
            }
            this.victimFlusher = flusher;
        } finally {
            instanceLock.unlock();
        }
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
     * getPage/newPage 公共骨架：instanceLock 内取得 target 帧（命中固定 / 未命中取受害者并载入），释放锁后取 page latch，
     * 返回 guard。语义与原单实例池逐字一致（per-frame LOADING + 出锁读盘 + 脏 victim 出锁 WAL 刷盘 + cleanSkip 防空转）。
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
            PageId victimToClean = null;
            BufferFrame loadingVictim = null;
            PageLoadFuture awaitFuture = null;
            instanceLock.lock();
            try {
                BufferFrame resident = pageHash.get(pageId);
                if (resident != null) {
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
                        resident.fixCount++;
                        policy.onAccess(resident);
                        chosen = resident;
                        break;
                    }
                } else {
                    BufferFrame victim = obtainVictim(cleanSkip);
                    if (victim.dirty && victimFlusher != null) {
                        victimToClean = victim.pageId;
                    } else {
                        if (victim.dirty) {
                            writeBack(victim);
                        }
                        if (victim.pageId != null) {
                            pageHash.remove(victim.pageId);
                            policy.onRemove(victim);
                        }
                        if (readFromDisk) {
                            // 装 LOADING 占位（in-lock）：owner 自持 fixCount=1，建 load future、注册 pageHash；出锁读盘。
                            victim.pageId = pageId;
                            clearDirty(victim);
                            stateMachine.transition(victim, BufferFrameState.LOADING);
                            victim.fixCount = 1;
                            victim.loadFuture = new PageLoadFuture();
                            pageHash.put(pageId, victim);
                            loadingVictim = victim;
                        } else {
                            // newPage miss：无盘 IO，锁内清零并直接发布 CLEAN（清零内容延后到取 X latch 之后）。
                            Arrays.fill(victim.data, (byte) 0);
                            victim.pageId = pageId;
                            clearDirty(victim);
                            stateMachine.transition(victim, BufferFrameState.CLEAN);
                            victim.fixCount = 1;
                            pageHash.put(pageId, victim);
                            policy.onInsert(victim);
                            chosen = victim;
                            break;
                        }
                    }
                }
            } finally {
                instanceLock.unlock();
            }
            if (awaitFuture != null) {
                awaitFuture.await(loadTimeoutNanos, pageId);
                continue;
            }
            if (loadingVictim != null) {
                chosen = readAndPublish(pageId, loadingVictim);
                break;
            }
            boolean cleaned = victimFlusher.flushVictim(victimToClean);
            if (!cleaned) {
                if (cleanSkip == null) {
                    cleanSkip = new HashSet<>();
                }
                cleanSkip.add(victimToClean);
            }
        }
        Lock latch = (mode == PageLatchMode.EXCLUSIVE)
                ? chosen.pageLatch.writeLock()
                : chosen.pageLatch.readLock();
        latch.lock();
        if (resetAfterLatch) {
            Arrays.fill(chosen.data, (byte) 0);
        }
        return new PageGuard(this, chosen, mode, latch);
    }

    /**
     * 出 instanceLock 读盘并发布 CLEAN（设计 §7.1/§7.3）。owner 已在锁内装好 LOADING 占位（fixCount=1、loadFuture）。
     * 读盘成功 → 重取锁 LOADING→CLEAN+onInsert+清 loadFuture（owner 保留 fix=1，给等待者重查留安全窗），出锁 complete；
     * 读盘失败 → 重取锁移除占位+复位 FREE 回 free list+清 loadFuture，出锁以异常 complete 唤醒等待者，向上抛根因。
     */
    private BufferFrame readAndPublish(PageId pageId, BufferFrame frame) {
        PageLoadFuture future = frame.loadFuture;
        try {
            pageStore.readPage(pageId, ByteBuffer.wrap(frame.data));
        } catch (RuntimeException loadError) {
            instanceLock.lock();
            try {
                pageHash.remove(pageId);
                frame.pageId = null;
                clearDirty(frame);
                stateMachine.transition(frame, BufferFrameState.FREE);
                frame.fixCount = 0;
                frame.loadFuture = null;
                freeList.add(frame);
            } finally {
                instanceLock.unlock();
            }
            future.failExceptionally(loadError);
            throw loadError;
        }
        instanceLock.lock();
        try {
            stateMachine.transition(frame, BufferFrameState.CLEAN);
            policy.onInsert(frame);
            frame.loadFuture = null;
        } finally {
            instanceLock.unlock();
        }
        future.complete();
        return frame;
    }

    /**
     * 取受害者帧（调用须持 instanceLock）：优先空闲帧；否则 LRU 序首个未 fix 的干净帧；若无干净未固定帧则回首个未在本轮
     * {@code cleanSkip} 中的脏未固定帧（交调用方出锁刷干净后复用）；全部被固定或已 skip 则抛耗尽。
     */
    private BufferFrame obtainVictim(Set<PageId> cleanSkip) {
        BufferFrame free = freeList.poll();
        if (free != null) {
            return free;
        }
        BufferFrame firstDirty = null;
        for (BufferFrame frame : policy.victimOrder()) {
            if (frame.fixCount != 0) {
                continue;
            }
            if (frame.state == BufferFrameState.FLUSHING) {
                continue;
            }
            if (!frame.dirty) {
                return frame;
            }
            if (firstDirty == null && (cleanSkip == null || !cleanSkip.contains(frame.pageId))) {
                firstDirty = frame;
            }
        }
        if (firstDirty != null) {
            return firstDirty;
        }
        throw new BufferPoolExhaustedException("buffer pool instance exhausted: all " + capacity
                + " frames are fixed or pending durable flush");
    }

    /** 写回脏帧到 PageStore 并清脏。调用须持 instanceLock 且帧 fixCount==0（内容稳定）。 */
    private void writeBack(BufferFrame frame) {
        pageStore.writePage(frame.pageId, ByteBuffer.wrap(frame.data));
        clearDirty(frame);
        stateMachine.transition(frame, BufferFrameState.CLEAN);
    }

    @Override
    public void release(BufferFrame frame, boolean wrote) {
        instanceLock.lock();
        try {
            if (wrote) {
                markDirty(frame);
            }
            frame.fixCount--;
            frameReleased.signalAll();
        } finally {
            instanceLock.unlock();
        }
    }

    /** Read-ahead 预取（§8.1）：未驻留且有空闲帧则载入 old 子链——不 fix、不提升。详见 {@link BufferPool#prefetch}。 */
    void prefetch(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        BufferFrame loading;
        instanceLock.lock();
        try {
            if (pageHash.containsKey(pageId)) {
                return; // 已驻留或正在载入。
            }
            BufferFrame free = freeList.poll();
            if (free == null) {
                return; // 无空闲帧：read-ahead 直接丢弃，绝不淘汰脏页或挤占前台需求读。
            }
            free.pageId = pageId;
            clearDirty(free);
            stateMachine.transition(free, BufferFrameState.LOADING);
            free.fixCount = 1;
            free.loadFuture = new PageLoadFuture();
            pageHash.put(pageId, free);
            loading = free;
        } finally {
            instanceLock.unlock();
        }
        BufferFrame published;
        try {
            published = readAndPublish(pageId, loading);
        } catch (RuntimeException loadError) {
            return; // read-ahead 尽力而为：载入失败丢弃，占位已回收。
        }
        instanceLock.lock();
        try {
            published.fixCount--; // 立即 unfix 使预取页成为 old 冷页；不 onAccess（未被真实访问、不提升）。
            frameReleased.signalAll();
        } finally {
            instanceLock.unlock();
        }
    }

    /** 若该页驻留、未 fix 且为脏，则写回 PageStore 并清脏（legacy 同步路径）。 */
    void flush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        instanceLock.lock();
        try {
            BufferFrame frame = pageHash.get(pageId);
            if (frame != null && frame.fixCount == 0 && frame.dirty) {
                writeBack(frame);
            }
        } finally {
            instanceLock.unlock();
        }
    }

    /** 写回本分片所有未 fix 的脏页（legacy 同步路径）。 */
    void flushAll() {
        instanceLock.lock();
        try {
            for (BufferFrame frame : pageHash.values()) {
                if (frame.fixCount == 0 && frame.dirty) {
                    writeBack(frame);
                }
            }
        } finally {
            instanceLock.unlock();
        }
    }

    /** 本分片的 flush list 候选（oldest≤targetLsn，按 oldest 升序，≤maxPages）。facade 跨分片合并后再裁剪。 */
    List<DirtyPageCandidate> localDirtyPageCandidates(Lsn targetLsn, int maxPages) {
        instanceLock.lock();
        try {
            return pageHash.values().stream()
                    .filter(frame -> frame.dirty && frame.state != BufferFrameState.FLUSHING
                            && frame.oldestModificationLsn.value() <= targetLsn.value())
                    .sorted(Comparator.comparingLong(frame -> frame.oldestModificationLsn.value()))
                    .limit(maxPages)
                    .map(frame -> new DirtyPageCandidate(frame.pageId,
                            frame.oldestModificationLsn, frame.newestModificationLsn))
                    .collect(Collectors.toList());
        } finally {
            instanceLock.unlock();
        }
    }

    /** 尝试复制一个未 fixed 的脏页镜像；DIRTY→FLUSHING。详见 {@link BufferPool#snapshotForFlush}。 */
    Optional<FlushPageSnapshot> snapshotForFlush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        instanceLock.lock();
        try {
            BufferFrame frame = pageHash.get(pageId);
            if (frame == null || frame.state != BufferFrameState.DIRTY || frame.fixCount != 0) {
                return Optional.empty();
            }
            byte[] image = Arrays.copyOf(frame.data, frame.data.length);
            stateMachine.transition(frame, BufferFrameState.FLUSHING);
            return Optional.of(new FlushPageSnapshot(pageId,
                    frame.newestModificationLsn, frame.dirtyVersion, image));
        } finally {
            instanceLock.unlock();
        }
    }

    /** flush 成功回调：版本符→CLEAN 清脏返回 true，不符→DIRTY 保脏返回 false。详见 {@link BufferPool#completeFlush}。 */
    boolean completeFlush(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        instanceLock.lock();
        try {
            BufferFrame frame = pageHash.get(snapshot.pageId());
            if (frame == null || frame.state != BufferFrameState.FLUSHING) {
                return false;
            }
            if (frame.fixCount == 0
                    && frame.dirtyVersion == snapshot.dirtyVersion()
                    && frame.newestModificationLsn.equals(snapshot.pageLsn())) {
                clearDirty(frame);
                stateMachine.transition(frame, BufferFrameState.CLEAN);
                return true;
            }
            stateMachine.transition(frame, BufferFrameState.DIRTY);
            return false;
        } finally {
            instanceLock.unlock();
        }
    }

    /** flush 失败回调：FLUSHING→DIRTY 保留脏页待重刷。 */
    void failFlush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        instanceLock.lock();
        try {
            BufferFrame frame = pageHash.get(pageId);
            if (frame != null && frame.state == BufferFrameState.FLUSHING) {
                stateMachine.transition(frame, BufferFrameState.DIRTY);
            }
        } finally {
            instanceLock.unlock();
        }
    }

    /** 本分片最老 dirty LSN；无脏页返回 null（facade 跨分片取全局 min）。 */
    Lsn localOldestDirtyLsnOrNull() {
        instanceLock.lock();
        try {
            return pageHash.values().stream()
                    .filter(frame -> frame.dirty)
                    .map(frame -> frame.oldestModificationLsn)
                    .min(Comparator.comparingLong(Lsn::value))
                    .orElse(null);
        } finally {
            instanceLock.unlock();
        }
    }

    /** 本分片是否存在任何 dirty frame。 */
    boolean hasDirtyPages() {
        instanceLock.lock();
        try {
            return pageHash.values().stream().anyMatch(frame -> frame.dirty);
        } finally {
            instanceLock.unlock();
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
        instanceLock.lock();
        try {
            while (hasFixedFrame(spaceId)) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    throw new BufferPoolInvalidationTimeoutException(
                            "timed out waiting fixed frames for tablespace " + spaceId.value());
                }
                try {
                    frameReleased.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new BufferPoolInvalidationTimeoutException(
                            "interrupted waiting fixed frames for tablespace " + spaceId.value(), interrupted);
                }
            }
            for (BufferFrame frame : pageHash.values()) {
                if (frame.pageId.spaceId().equals(spaceId) && frame.dirty) {
                    throw new DirtyTablespaceInvalidationException(
                            "dirty frame blocks tablespace invalidation: " + frame.pageId);
                }
            }
        } finally {
            instanceLock.unlock();
        }
    }

    /**
     * 截断排空第二阶段（移除）：把本分片该空间全部帧从 pageHash/LRU 移除并复位回 free list。仅在
     * {@link #awaitDrainedAndCheckClean} 对**所有**分片通过后由 facade 调用；依赖调用方持 tablespace X lease 阻断新流量，
     * 故第一阶段确认的 fix=0/clean 在两阶段之间保持不变。
     */
    void removeTablespaceFrames(SpaceId spaceId) {
        instanceLock.lock();
        try {
            List<BufferFrame> targets = new ArrayList<>();
            for (BufferFrame frame : pageHash.values()) {
                if (frame.pageId.spaceId().equals(spaceId)) {
                    targets.add(frame);
                }
            }
            for (BufferFrame frame : targets) {
                pageHash.remove(frame.pageId);
                policy.onRemove(frame);
                frame.pageId = null;
                clearDirty(frame);
                stateMachine.transition(frame, BufferFrameState.FREE);
                frame.fixCount = 0;
                freeList.add(frame);
            }
        } finally {
            instanceLock.unlock();
        }
    }

    /** 调用须持 instanceLock。 */
    private boolean hasFixedFrame(SpaceId spaceId) {
        for (BufferFrame frame : pageHash.values()) {
            if (frame.pageId.spaceId().equals(spaceId) && frame.fixCount > 0) {
                return true;
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
        instanceLock.lock();
        try {
            return pageHash.size();
        } finally {
            instanceLock.unlock();
        }
    }

    /** 本分片驻留页 PageId 不可变快照。 */
    List<PageId> residentPageIds() {
        instanceLock.lock();
        try {
            return List.copyOf(pageHash.keySet());
        } finally {
            instanceLock.unlock();
        }
    }

    /** 本分片在某连续页区间内的驻留页数。 */
    int countResidentInRange(SpaceId spaceId, long firstPageNo, int pageCount) {
        instanceLock.lock();
        try {
            return pageHash.countInRange(spaceId, firstPageNo, pageCount);
        } finally {
            instanceLock.unlock();
        }
    }

    private Lsn pageLsn(BufferFrame frame) {
        return Lsn.of(ByteBuffer.wrap(frame.data).getLong(PageEnvelopeLayout.PAGE_LSN));
    }

    /** 调用须持 instanceLock。详见原单实例池 markDirty 的 oldestModificationLsn==null 守卫说明。 */
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
        if (frame.state != BufferFrameState.FLUSHING) {
            stateMachine.transition(frame, BufferFrameState.DIRTY);
        }
    }

    /** 调用须持 instanceLock。 */
    private void clearDirty(BufferFrame frame) {
        frame.dirty = false;
        frame.oldestModificationLsn = null;
        frame.newestModificationLsn = null;
    }
}
