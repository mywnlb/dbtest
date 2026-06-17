package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
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

    /** 保护 residentMap / freeList / policy / 各帧 pageId·dirty·fixCount；首版 miss/evict/flush 的盘 IO 也在其内串行。 */
    private final ReentrantLock poolLock = new ReentrantLock();
    private final Map<PageId, BufferFrame> residentMap = new HashMap<>();
    private final Deque<BufferFrame> freeList = new ArrayDeque<>();

    public LruBufferPool(PageStore pageStore, PageSize pageSize, int capacity) {
        this(pageStore, pageSize, capacity, new LruReplacementPolicy());
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
        poolLock.lock();
        try {
            BufferFrame resident = residentMap.get(pageId);
            if (resident != null) {
                if (!readFromDisk) {
                    // 页创建命中驻留页：重初始化（复用帧），对齐 InnoDB buf_page_create。
                    // 清零延后到取得 X latch 之后做（见方法末尾）——不能在 poolLock 内、未持 page latch 时改页内容，
                    // 否则会与持锁读者撞车、绕过 page latch 语义。dirty 在 poolLock 下置位（dirty 由 poolLock 保护）。
                    resident.dirty = true;
                    resetAfterLatch = true;
                }
                resident.fixCount++;
                policy.onAccess(resident);
                chosen = resident;
            } else {
                BufferFrame victim = obtainVictim();
                if (victim.pageId != null) {
                    if (victim.dirty) {
                        writeBack(victim);
                    }
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
                    freeList.add(victim);
                    throw loadError;
                }
                victim.pageId = pageId;
                clearDirty(victim);
                victim.fixCount = 1;
                residentMap.put(pageId, victim);
                policy.onInsert(victim);
                chosen = victim;
            }
        } finally {
            poolLock.unlock();
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

    /** 取受害者帧：优先空闲列表；否则 LRU 序首个未 fix 帧；都没有则抛耗尽。调用须持 poolLock。 */
    private BufferFrame obtainVictim() {
        BufferFrame free = freeList.poll();
        if (free != null) {
            return free;
        }
        for (BufferFrame frame : policy.victimOrder()) {
            if (frame.fixCount == 0) {
                return frame;
            }
        }
        throw new BufferPoolExhaustedException("buffer pool exhausted: all " + capacity + " frames are fixed");
    }

    /** 写回脏帧到 PageStore 并清脏。调用须持 poolLock 且帧 fixCount==0（内容稳定）。 */
    private void writeBack(BufferFrame frame) {
        pageStore.writePage(frame.pageId, ByteBuffer.wrap(frame.data));
        clearDirty(frame);
    }

    @Override
    public void release(BufferFrame frame, boolean wrote) {
        poolLock.lock();
        try {
            if (wrote) {
                markDirty(frame);
            }
            frame.fixCount--;
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
                    .filter(frame -> frame.dirty && frame.oldestModificationLsn.value() <= targetLsn.value())
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
            if (frame == null || !frame.dirty || frame.fixCount != 0) {
                return Optional.empty();
            }
            byte[] image = Arrays.copyOf(frame.data, frame.data.length);
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
            if (frame == null || !frame.dirty || frame.fixCount != 0) {
                return false;
            }
            if (frame.dirtyVersion == snapshot.dirtyVersion()
                    && frame.newestModificationLsn.equals(snapshot.pageLsn())) {
                clearDirty(frame);
                return true;
            }
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
            // F1 没有 FLUSHING 中间态；失败只保留 dirty，等待下一轮选择。
            residentMap.get(pageId);
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
        if (!frame.dirty) {
            frame.oldestModificationLsn = pageLsn;
        }
        frame.newestModificationLsn = pageLsn;
        frame.dirtyVersion++;
        frame.dirty = true;
    }

    /** 调用须持 poolLock。 */
    private void clearDirty(BufferFrame frame) {
        frame.dirty = false;
        frame.oldestModificationLsn = null;
        frame.newestModificationLsn = null;
    }
}
