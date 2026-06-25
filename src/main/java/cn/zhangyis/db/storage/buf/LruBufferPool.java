package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * LRU Buffer Pool 实现。单 poolLock 保护帧表/空闲列表/LRU/帧元数据，miss/evict/flush 的盘 IO 在 poolLock 内串行
 * （首版简化点：后续引入 per-frame loading 状态把 IO 移出池锁）。每帧 page latch 在 poolLock 之外获取，不嵌套。
 * fixCount>0 不可淘汰；脏帧淘汰经 PageStore.writePage 写回。
 *
 * <p>同时实现 FrameReleaser：PageGuard.close() 回调 release 在 poolLock 下 OR 脏并 unfix。
 */
public final class LruBufferPool implements BufferPool, FrameReleaser {

    private final PageStore pageStore;
    private final PageSize pageSize;
    private final int capacity;
    private final ReplacementPolicy policy;

    /** 帧状态机：集中执行 BufferFrame.state 的合法转换（§5.7）。所有调用在 poolLock 下串行，故无需自身加锁。 */
    private final FrameStateMachine stateMachine = new FrameStateMachine();

    /**
     * 淘汰脏 victim 时委托的 WAL 安全刷盘端口；null 表示独立/测试池，淘汰脏帧退回 legacy {@link #writeBack}。
     * set-once、bootstrap 注入、热路径只读，故 volatile 即可安全发布。
     */
    private volatile DirtyVictimFlusher victimFlusher;

    /** 保护 residentMap / freeList / policy / 各帧 pageId·dirty·fixCount；首版 miss/evict/flush 的盘 IO 也在其内串行。 */
    private final ReentrantLock poolLock = new ReentrantLock();
    /** release() 在 fixCount 下降时唤醒截断排空；条件始终由 poolLock 保护。 */
    private final Condition frameReleased = poolLock.newCondition();
    private final Map<PageId, BufferFrame> residentMap = new HashMap<>();
    private final Deque<BufferFrame> freeList = new ArrayDeque<>();

    public LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity) {
        // 默认采用 midpoint LRU（Phase A）：读入进 old 子链、提升窗 + youngDistanceThreshold 抗扫描污染。
        // 生产时钟用墙钟驱动 oldBlocksTime；测试可经 4 参构造注入可控时钟的策略。
        this(pageStore, pageSize, capacity, new MidpointLruReplacementPolicy(System::currentTimeMillis));
    }

    LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity, ReplacementPolicy policy) {
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
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.capacity = capacity;
        this.policy = policy;
        for (int i = 0; i < capacity; i++) {
            freeList.add(new BufferFrame(pageSize));
        }
    }

    /**
     * 注入淘汰脏页的 WAL 安全刷盘端口（set-once）。生产 {@code StorageEngine} 必须在构造 FlushCoordinator 之后、
     * 任何可能触发淘汰的 page access 之前调用一次；独立/测试池可不注入（脏 victim 退回 legacy writeBack）。
     */
    public void attachVictimFlusher(DirtyVictimFlusher flusher) {
        if (flusher == null) {
            throw new DatabaseValidationException("victim flusher must not be null");
        }
        // set-once：在 poolLock 下校验并发布，防止运行期被换成另一刷盘实现导致淘汰语义不一致。
        poolLock.lock();
        try {
            if (this.victimFlusher != null) {
                throw new DatabaseValidationException("victim flusher already attached (set-once)");
            }
            this.victimFlusher = flusher;
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public PageGuard getPage(PageId pageId, PageLatchMode mode) {
        return acquire(pageId, mode, true);
    }

    @Override
    public PageGuard newPage(PageId pageId, PageLatchMode mode) {
        return acquire(pageId, mode, false);
    }

    /**
     * getPage/newPage 公共骨架：poolLock 内取得 target 帧（命中固定 / 未命中取受害者并载入），
     * 释放 poolLock 后取 page latch，返回 guard。载入失败回收 victim 到空闲列表，不泄漏帧/锁。
     *
     * <p>WAL 安全淘汰（设计 §6.1 LRU_FLUSH / §8.3 / §9.2）：选中的 victim 若是脏帧且已注入 {@link #victimFlusher}，
     * 不能在 poolLock 内直接写盘（既违反 WAL，又违反"持 pool 锁时不取物理文件锁"）。改为在锁内只捕获其 PageId、
     * 释放 poolLock，出锁经端口做 WAL gate + checksum + doublewrite + 写盘 + completeFlush，再回环重选受害者
     * （届时该帧已清脏可复用，或被他人再 fix/再脏则另选）。无 flusher 的独立/测试池退回 legacy {@link #writeBack}。
     *
     * <p>防空转：本轮维护 {@code cleanSkip}，{@code flushVictim} 返回 false（不脏/redo 未 durable/又变脏）的页本轮
     * 不再选，至多尝试 capacity 个不同脏页后由 {@link #obtainVictim} 抛 {@link BufferPoolExhaustedException}（fail-safe，
     * 绝不腐败）。{@code flushVictim} 抛出的真 IO 失败直接向上传播，不进 skip set、不被吞成耗尽。
     */
    private PageGuard acquire(PageId pageId, PageLatchMode mode, boolean readFromDisk) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("page latch mode must not be null");
        }
        // 页创建/重初始化是写操作，必须持 X latch；不允许 newPage(page, SHARED) 走清零语义。
        if (!readFromDisk && mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("newPage requires EXCLUSIVE latch but got " + mode);
        }
        BufferFrame chosen;
        boolean resetAfterLatch = false;
        Set<PageId> cleanSkip = null;
        while (true) {
            PageId victimToClean = null;
            poolLock.lock();
            try {
                BufferFrame resident = residentMap.get(pageId);
                if (resident != null) {
                    if (!readFromDisk) {
                        // 页创建命中驻留页：重初始化（复用帧），对齐 InnoDB buf_page_create。
                        // 清零延后到取得 X latch 之后做（见方法末尾）——不能在 poolLock 内、未持 page latch 时改页内容，
                        // 否则会与持锁读者撞车、绕过 page latch 语义。dirty 在 poolLock 下置位（dirty 由 poolLock 保护）。
                        resident.dirty = true;
                        stateMachine.transition(resident, BufferFrameState.DIRTY);
                        resetAfterLatch = true;
                    }
                    resident.fixCount++;
                    policy.onAccess(resident);
                    chosen = resident;
                    break;
                }
                BufferFrame victim = obtainVictim(cleanSkip);
                if (victim.dirty && victimFlusher != null) {
                    // 脏 victim + 已注入 flusher：锁内只记 PageId，出锁刷盘后回环重选，绝不在 poolLock 内写盘。
                    victimToClean = victim.pageId;
                } else {
                    if (victim.dirty) {
                        // 无 flusher 的独立/测试池：退回 legacy 直接写回（字节级保持既有行为；不承诺 WAL 安全）。
                        writeBack(victim);
                    }
                    if (victim.pageId != null) {
                        residentMap.remove(victim.pageId);
                        policy.onRemove(victim);
                    }
                    try {
                        if (readFromDisk) {
                            pageStore.readPage(pageId, ByteBuffer.wrap(victim.data));
                        } else {
                            Arrays.fill(victim.data, (byte) 0);
                        }
                    } catch (RuntimeException loadError) {
                        victim.pageId = null;
                        clearDirty(victim);
                        stateMachine.transition(victim, BufferFrameState.FREE);
                        freeList.add(victim);
                        throw loadError;
                    }
                    victim.pageId = pageId;
                    clearDirty(victim);
                    stateMachine.transition(victim, BufferFrameState.CLEAN);
                    victim.fixCount = 1;
                    residentMap.put(pageId, victim);
                    policy.onInsert(victim);
                    chosen = victim;
                    break;
                }
            } finally {
                poolLock.unlock();
            }
            // 出 poolLock：经端口把脏 victim 经 WAL 管线刷干净（内部做 WAL gate/doublewrite/写盘/completeFlush）。
            // 真 IO 失败抛领域异常并向上传播；返回 false=本轮未清成→计入 skip set 回环重选，避免对未 durable 页空转。
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
        // 驻留页重初始化：在 X latch 保护下清零（不经 PageGuard → 不产 PAGE_BYTES；清零恢复语义由 PAGE_INIT 承担）。
        if (resetAfterLatch) {
            Arrays.fill(chosen.data, (byte) 0);
        }
        return new PageGuard(this, chosen, mode, latch);
    }

    /**
     * 取受害者帧（调用须持 poolLock）：优先空闲帧；否则 LRU 序首个未 fix 的**干净**帧（可直接复用）；
     * 若无干净未固定帧，则回首个未在本轮 {@code cleanSkip} 中的脏未固定帧（交由调用方出锁刷干净后复用）；
     * 全部被固定或已 skip 则抛耗尽。优先干净帧可避免不必要的刷盘。
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
                // 正在刷盘的帧处于 IO 中，不能被淘汰（否则与刷盘写竞争/重复刷）；待 flush 完成转 CLEAN 后再考虑。
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
        throw new BufferPoolExhaustedException("buffer pool exhausted: all " + capacity
                + " frames are fixed or pending durable flush");
    }

    /** 写回脏帧到 PageStore 并清脏。调用须持 poolLock 且帧 fixCount==0（内容稳定）。 */
    private void writeBack(BufferFrame frame) {
        pageStore.writePage(frame.pageId, ByteBuffer.wrap(frame.data));
        clearDirty(frame);
        stateMachine.transition(frame, BufferFrameState.CLEAN);
    }

    @Override
    public void release(BufferFrame frame, boolean wrote) {
        poolLock.lock();
        try {
            if (wrote) {
                markDirty(frame);
            }
            frame.fixCount--;
            frameReleased.signalAll();
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public void flush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        poolLock.lock();
        try {
            BufferFrame frame = residentMap.get(pageId);
            if (frame != null && frame.fixCount == 0 && frame.dirty) {
                writeBack(frame);
            }
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public void flushAll() {
        poolLock.lock();
        try {
            for (BufferFrame frame : residentMap.values()) {
                if (frame.fixCount == 0 && frame.dirty) {
                    writeBack(frame);
                }
            }
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public List<DirtyPageCandidate> dirtyPageCandidates(Lsn targetLsn, int maxPages) {
        if (targetLsn == null) {
            throw new DatabaseValidationException("target LSN must not be null");
        }
        if (maxPages < 0) {
            throw new DatabaseValidationException("max pages must not be negative: " + maxPages);
        }
        poolLock.lock();
        try {
            return residentMap.values().stream()
                    // 跳过 FLUSHING 帧：已有单 IO owner 在刷它，重复入候选会导致重复刷/竞争 snapshot。
                    .filter(frame -> frame.dirty && frame.state != BufferFrameState.FLUSHING
                            && frame.oldestModificationLsn.value() <= targetLsn.value())
                    .sorted(Comparator.comparingLong(frame -> frame.oldestModificationLsn.value()))
                    .limit(maxPages)
                    .map(frame -> new DirtyPageCandidate(frame.pageId,
                            frame.oldestModificationLsn, frame.newestModificationLsn))
                    .collect(Collectors.toUnmodifiableList());
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public Optional<FlushPageSnapshot> snapshotForFlush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        poolLock.lock();
        try {
            BufferFrame frame = residentMap.get(pageId);
            // 仅 DIRTY 帧可被认领刷盘：FLUSHING 已被另一 IO owner 持有（单 owner，拒二次 snapshot），
            // CLEAN/LOADING 无需刷；fixCount!=0 时内容不稳定，不取快照。state==DIRTY 蕴含 dirty==true。
            if (frame == null || frame.state != BufferFrameState.DIRTY || frame.fixCount != 0) {
                return Optional.empty();
            }
            byte[] image = Arrays.copyOf(frame.data, frame.data.length);
            // DIRTY → FLUSHING：标记单 IO owner 已开始刷盘；dirty 仍为 true（未 durable），故 oldest-dirty 边界仍含本帧。
            stateMachine.transition(frame, BufferFrameState.FLUSHING);
            return Optional.of(new FlushPageSnapshot(pageId,
                    frame.newestModificationLsn, frame.dirtyVersion, image));
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public boolean completeFlush(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        poolLock.lock();
        try {
            BufferFrame frame = residentMap.get(snapshot.pageId());
            // 只接受由本次 snapshot 置为 FLUSHING 的帧；若已被淘汰/复用/状态改变则放弃（返回 false，不动状态）。
            if (frame == null || frame.state != BufferFrameState.FLUSHING) {
                return false;
            }
            if (frame.fixCount == 0
                    && frame.dirtyVersion == snapshot.dirtyVersion()
                    && frame.newestModificationLsn.equals(snapshot.pageLsn())) {
                // snapshot 后未再被修改 → 落盘镜像即当前内容，FLUSHING → CLEAN 清脏。
                clearDirty(frame);
                stateMachine.transition(frame, BufferFrameState.CLEAN);
                return true;
            }
            // 刷盘期又被改（dirtyVersion 变）或被重新 fix → 落盘镜像已过期，FLUSHING → DIRTY 保留脏待重刷。
            stateMachine.transition(frame, BufferFrameState.DIRTY);
            return false;
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public void failFlush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        poolLock.lock();
        try {
            // 刷盘 IO 失败：FLUSHING → DIRTY，保留脏页待下一轮重刷（dirty 全程为 true，不丢未落盘修改）。
            BufferFrame frame = residentMap.get(pageId);
            if (frame != null && frame.state == BufferFrameState.FLUSHING) {
                stateMachine.transition(frame, BufferFrameState.DIRTY);
            }
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public Lsn oldestDirtyLsnOr(Lsn cleanBoundary) {
        if (cleanBoundary == null) {
            throw new DatabaseValidationException("clean boundary must not be null");
        }
        poolLock.lock();
        try {
            return residentMap.values().stream()
                    .filter(frame -> frame.dirty)
                    .map(frame -> frame.oldestModificationLsn)
                    .min(Comparator.comparingLong(Lsn::value))
                    .orElse(cleanBoundary);
        } finally {
            poolLock.unlock();
        }
    }

    /**
     * 等待目标空间所有 fix 归零，然后原子检查 dirty 并从 resident/LRU 移除。等待期间 Condition 会释放 poolLock，
     * 允许 PageGuard.close 回调 release；成功路径不做磁盘 IO，调用方必须预先完成安全 flush。
     */
    @Override
    public void invalidateTablespace(SpaceId spaceId, Duration timeout) {
        if (spaceId == null) {
            throw new DatabaseValidationException("invalidate tablespace space id must not be null");
        }
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("invalidate tablespace timeout must be positive");
        }
        long remaining;
        try {
            remaining = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("invalidate tablespace timeout is too large", overflow);
        }
        poolLock.lock();
        try {
            while (hasFixedFrame(spaceId)) {
                if (remaining <= 0) {
                    throw new BufferPoolInvalidationTimeoutException(
                            "timed out waiting fixed frames for tablespace " + spaceId.value());
                }
                try {
                    remaining = frameReleased.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new BufferPoolInvalidationTimeoutException(
                            "interrupted waiting fixed frames for tablespace " + spaceId.value(), interrupted);
                }
            }
            List<BufferFrame> targets = new ArrayList<>();
            for (BufferFrame frame : residentMap.values()) {
                if (frame.pageId.spaceId().equals(spaceId)) {
                    if (frame.dirty) {
                        throw new DirtyTablespaceInvalidationException(
                                "dirty frame blocks tablespace invalidation: " + frame.pageId);
                    }
                    targets.add(frame);
                }
            }
            for (BufferFrame frame : targets) {
                residentMap.remove(frame.pageId);
                policy.onRemove(frame);
                frame.pageId = null;
                clearDirty(frame);
                stateMachine.transition(frame, BufferFrameState.FREE);
                frame.fixCount = 0;
                freeList.add(frame);
            }
        } finally {
            poolLock.unlock();
        }
    }

    /** 调用须持 poolLock。 */
    private boolean hasFixedFrame(SpaceId spaceId) {
        for (BufferFrame frame : residentMap.values()) {
            if (frame.pageId.spaceId().equals(spaceId) && frame.fixCount > 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int residentCount() {
        poolLock.lock();
        try {
            return residentMap.size();
        } finally {
            poolLock.unlock();
        }
    }

    @Override
    public void close() {
        flushAll();
    }

    private Lsn pageLsn(BufferFrame frame) {
        return Lsn.of(ByteBuffer.wrap(frame.data).getLong(PageEnvelopeLayout.PAGE_LSN));
    }

    /** 调用须持 poolLock。 */
    private void markDirty(BufferFrame frame) {
        Lsn pageLsn = pageLsn(frame);
        // oldestModificationLsn = 自上次刷盘以来最早的修改 LSN：只要当前未设置就设为本次 pageLSN。
        // 不能用 `!frame.dirty` 作为条件——newPage 对驻留页重初始化时会先在 poolLock 内置 dirty=true（见 acquire
        // 重初始化路径）而尚未有 LSN；若此处仅在 `!dirty` 时设置，commit 的 markDirty 会因 dirty 已为真而跳过，
        // 留下 dirty=true 但 oldestModificationLsn=null 的帧，导致 dirtyPageCandidates/checkpoint NPE。
        if (frame.oldestModificationLsn == null) {
            frame.oldestModificationLsn = pageLsn;
        }
        frame.newestModificationLsn = pageLsn;
        frame.dirtyVersion++;
        frame.dirty = true;
        // 状态：非刷盘中则进 DIRTY；若帧正 FLUSHING（刷盘期被写）则保持 FLUSHING——dirtyVersion 已推进，
        // completeFlush 会因版本不符判定落盘镜像过期并退回 DIRTY，故此处不抢转，避免丢失"刷盘镜像已陈旧"信号。
        if (frame.state != BufferFrameState.FLUSHING) {
            stateMachine.transition(frame, BufferFrameState.DIRTY);
        }
    }

    /** 调用须持 poolLock。 */
    private void clearDirty(BufferFrame frame) {
        frame.dirty = false;
        frame.oldestModificationLsn = null;
        frame.newestModificationLsn = null;
    }
}
