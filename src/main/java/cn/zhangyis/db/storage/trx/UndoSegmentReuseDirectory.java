package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.FreeUndoSegmentRef;
import cn.zhangyis.db.storage.undo.UndoLogKind;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * rollback segment 可复用 undo owner 的统一运行期投影。INSERT/UPDATE cache 是有界 LIFO，free list 是跨 kind FIFO；
 * page3 与 FREE 首页链才是 crash 后权威，本类只用 lease 保护“内存预留—物理 MTR—内存发布”窗口。
 *
 * <p><b>并发边界</b>：短锁只保护三个容器、三个 transition fence 和全局 drain gate，锁内不访问 Buffer Pool、
 * FSP、redo 或文件。物理写开始后的异常保留 fence，要求实例 fail-stop，禁止内存投影继续覆盖未知磁盘结果。
 */
public final class UndoSegmentReuseDirectory {

    /** 每个 kind 的持久 cache 容量；free FIFO 不设配置上限。 */
    private final int capacityPerKind;
    /** INSERT/UPDATE cache，顺序均为栈底到栈顶。 */
    private final List<CachedUndoSegmentRef> insertStack = new ArrayList<>();
    /** UPDATE cache 的运行期投影，顺序为栈底到栈顶。 */
    private final List<CachedUndoSegmentRef> updateStack = new ArrayList<>();
    /** free FIFO，迭代顺序为 head 到 tail。 */
    private final ArrayDeque<FreeUndoSegmentRef> freeQueue = new ArrayDeque<>();
    /** 保护全部运行期投影状态的短锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 各 owner 集合是否已有 lease 跨出短锁执行物理 MTR。 */
    private boolean insertTransition;
    /** UPDATE cache 是否已有 lease 跨出短锁执行物理 MTR。 */
    private boolean updateTransition;
    /** free FIFO 是否已有 lease 跨出短锁执行物理 MTR。 */
    private boolean freeTransition;
    /** truncate 是否独占三个 owner 集合。 */
    private boolean draining;

    /**
     * 创建空运行期目录。cache 容量必须和 page3 format/read 使用的容量一致；free FIFO 没有配置容量，
     * 仅受 Java 集合的 {@link Integer#MAX_VALUE} 上限约束。
     *
     * @param capacityPerKind INSERT 与 UPDATE 各自可持有的最大 cache owner 数，允许为零。
     */
    public UndoSegmentReuseDirectory(int capacityPerKind) {
        if (capacityPerKind < 0) {
            throw new DatabaseValidationException("undo cache capacity must not be negative: " + capacityPerKind);
        }
        this.capacityPerKind = capacityPerKind;
    }

    /** 返回稳定 cache top 证据；transition/drain 中不暴露候选。 */
    public Optional<CacheCandidate> peekCache(UndoLogKind kind) {
        requireCacheKind(kind);
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (draining || cacheTransition(kind) || stack.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new CacheCandidate(stack.getLast(), stack.size()));
        } finally {
            lock.unlock();
        }
    }

    /** 返回稳定 free head、可选 successor、tail 与 length，供 page3/head-page CAS 规划。 */
    public Optional<FreeCandidate> peekFree() {
        lock.lock();
        try {
            if (draining || freeTransition || freeQueue.isEmpty()) {
                return Optional.empty();
            }
            FreeUndoSegmentRef head = freeQueue.getFirst();
            FreeUndoSegmentRef successor = freeQueue.size() > 1
                    ? freeQueue.stream().skip(1).findFirst().orElseThrow() : null;
            return Optional.of(new FreeCandidate(head, Optional.ofNullable(successor),
                    freeQueue.getLast(), freeQueue.size()));
        } finally {
            lock.unlock();
        }
    }

    /** 为 cache top 复用建立非阻塞 transition fence。 */
    CachePopLease reserveCachePop(CacheCandidate expected) {
        if (expected == null) {
            throw new DatabaseValidationException("undo cache pop candidate must not be null");
        }
        UndoLogKind kind = expected.segment().kind();
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (draining || cacheTransition(kind) || stack.size() != expected.expectedCount()
                    || stack.isEmpty() || !stack.getLast().equals(expected.segment())) {
                throw new UndoWriteStalePlanException("cached undo top changed before reuse: kind=" + kind);
            }
            setCacheTransition(kind, true);
            return new CachePopLease(this, expected);
        } finally {
            lock.unlock();
        }
    }

    /** 为 free head 复用建立非阻塞 transition fence。 */
    FreePopLease reserveFreePop(FreeCandidate expected) {
        if (expected == null) {
            throw new DatabaseValidationException("undo free pop candidate must not be null");
        }
        lock.lock();
        try {
            if (draining || freeTransition || !matchesFreeCandidate(expected)) {
                throw new UndoWriteStalePlanException("free undo head changed before reuse");
            }
            freeTransition = true;
            return new FreePopLease(this, expected);
        } finally {
            lock.unlock();
        }
    }

    /** cache 有容量且 owner 未重复时预留一次 push；忙或满时不等待。 */
    Optional<CachePushLease> tryReserveCachePush(CachedUndoSegmentRef segment) {
        if (segment == null) {
            throw new DatabaseValidationException("cached undo push segment must not be null");
        }
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(segment.kind());
            if (draining || cacheTransition(segment.kind()) || stack.size() >= capacityPerKind) {
                return Optional.empty();
            }
            requireUniqueNewPages(List.of(segment.handle().firstPageId()));
            setCacheTransition(segment.kind(), true);
            return Optional.of(new CachePushLease(this, segment, stack.size()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 预留一批一至两个 free tail push。free 不设配置上限，但运行期集合不能超过 Integer.MAX_VALUE；超限返回 empty，
     * finalizer 据此选择物理 drop。
     */
    Optional<FreePushLease> tryReserveFreePush(List<FreeUndoSegmentRef> segments) {
        if (segments == null || segments.isEmpty() || segments.size() > 2
                || segments.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("free undo push must contain one or two segments");
        }
        lock.lock();
        try {
            if (draining || freeTransition || (long) freeQueue.size() + segments.size() > Integer.MAX_VALUE) {
                return Optional.empty();
            }
            requireUniqueNewPages(segments.stream().map(item -> item.handle().firstPageId()).toList());
            freeTransition = true;
            return Optional.of(new FreePushLease(this, segments, freeQueue.size(),
                    Optional.ofNullable(freeQueue.peekLast())));
        } finally {
            lock.unlock();
        }
    }

    /** truncate 使用的非阻塞全局 gate；任何普通 owner transition 忙时立即返回 empty。 */
    public Optional<DrainLease> tryBeginDrain() {
        lock.lock();
        try {
            if (draining || insertTransition || updateTransition || freeTransition) {
                return Optional.empty();
            }
            draining = true;
            return Optional.of(new DrainLease(this));
        } finally {
            lock.unlock();
        }
    }

    /** recovery 完成全部 page3/首页/FSP 校验后一次安装三个持久 owner 集合。 */
    public void restore(List<CachedUndoSegmentRef> insert, List<CachedUndoSegmentRef> update,
                        List<FreeUndoSegmentRef> free) {
        if (insert == null || update == null || free == null) {
            throw new DatabaseValidationException("recovered undo reuse collections must not be null");
        }
        lock.lock();
        try {
            if (draining || insertTransition || updateTransition || freeTransition
                    || !insertStack.isEmpty() || !updateStack.isEmpty() || !freeQueue.isEmpty()) {
                throw new DatabaseValidationException("undo reuse restore requires an empty idle directory");
            }
            validateRecoveredStack(insert, UndoLogKind.INSERT);
            validateRecoveredStack(update, UndoLogKind.UPDATE);
            if (free.size() > Integer.MAX_VALUE || free.stream().anyMatch(java.util.Objects::isNull)) {
                throw new DatabaseValidationException("recovered undo free list is invalid");
            }
            Set<PageId> pages = new HashSet<>();
            allPages(insert, update, free).forEach(page -> {
                if (!pages.add(page)) {
                    throw new DatabaseValidationException("duplicate recovered reusable undo first page: " + page);
                }
            });
            insertStack.addAll(insert);
            updateStack.addAll(update);
            freeQueue.addAll(free);
        } finally {
            lock.unlock();
        }
    }

    public int cachedCount(UndoLogKind kind) {
        requireCacheKind(kind);
        lock.lock();
        try {
            return stack(kind).size();
        } finally {
            lock.unlock();
        }
    }

    public int freeCount() {
        lock.lock();
        try {
            return freeQueue.size();
        } finally {
            lock.unlock();
        }
    }

    public int capacityPerKind() {
        return capacityPerKind;
    }

    /** 复制三个 owner 集合；cache 为 bottom→top，free 为 head→tail。 */
    public ReuseSnapshot snapshot() {
        lock.lock();
        try {
            return new ReuseSnapshot(insertStack, updateStack, List.copyOf(freeQueue));
        } finally {
            lock.unlock();
        }
    }

    private void validateRecoveredStack(List<CachedUndoSegmentRef> recovered, UndoLogKind expectedKind) {
        if (recovered.size() > capacityPerKind || recovered.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("recovered undo cache exceeds configured capacity for " + expectedKind);
        }
        for (CachedUndoSegmentRef ref : recovered) {
            if (ref.kind() != expectedKind) {
                throw new DatabaseValidationException("recovered undo cache kind mismatch: expected="
                        + expectedKind + ", current=" + ref.kind());
            }
        }
    }

    private void requireUniqueNewPages(List<PageId> candidates) {
        Set<PageId> existing = new HashSet<>(allPages(insertStack, updateStack, List.copyOf(freeQueue)));
        for (PageId page : candidates) {
            if (!existing.add(page)) {
                throw new DatabaseValidationException("undo segment already exists in reuse directory: " + page);
            }
        }
    }

    private boolean matchesFreeCandidate(FreeCandidate candidate) {
        if (freeQueue.size() != candidate.expectedCount() || freeQueue.isEmpty()
                || !freeQueue.getFirst().equals(candidate.segment())
                || !freeQueue.getLast().equals(candidate.expectedTail())) {
            return false;
        }
        Optional<FreeUndoSegmentRef> currentSuccessor = freeQueue.size() > 1
                ? freeQueue.stream().skip(1).findFirst() : Optional.empty();
        return currentSuccessor.equals(candidate.successor());
    }

    private List<CachedUndoSegmentRef> stack(UndoLogKind kind) {
        return switch (kind) {
            case INSERT -> insertStack;
            case UPDATE -> updateStack;
            case TEMPORARY -> throw new DatabaseValidationException("temporary undo has no cache stack");
        };
    }

    private boolean cacheTransition(UndoLogKind kind) {
        return kind == UndoLogKind.INSERT ? insertTransition : updateTransition;
    }

    private void setCacheTransition(UndoLogKind kind, boolean value) {
        if (kind == UndoLogKind.INSERT) {
            insertTransition = value;
        } else if (kind == UndoLogKind.UPDATE) {
            updateTransition = value;
        } else {
            throw new DatabaseValidationException("temporary undo has no cache transition");
        }
    }

    private static void requireCacheKind(UndoLogKind kind) {
        if (kind == null || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("ordinary undo cache kind must be INSERT or UPDATE");
        }
    }

    private static List<PageId> allPages(List<CachedUndoSegmentRef> insert,
                                         List<CachedUndoSegmentRef> update,
                                         List<FreeUndoSegmentRef> free) {
        List<PageId> pages = new ArrayList<>(insert.size() + update.size() + free.size());
        insert.forEach(item -> pages.add(item.handle().firstPageId()));
        update.forEach(item -> pages.add(item.handle().firstPageId()));
        free.forEach(item -> pages.add(item.handle().firstPageId()));
        return pages;
    }

    /** 规划期冻结的 cache LIFO top 与持久 count。 */
    public record CacheCandidate(CachedUndoSegmentRef segment, int expectedCount) {
        public CacheCandidate {
            if (segment == null || expectedCount <= 0) {
                throw new DatabaseValidationException("invalid undo cache candidate");
            }
        }
    }

    /** 规划期冻结的 free FIFO head、successor、tail 与持久 count。 */
    public record FreeCandidate(FreeUndoSegmentRef segment, Optional<FreeUndoSegmentRef> successor,
                                FreeUndoSegmentRef expectedTail, int expectedCount) {
        public FreeCandidate {
            if (segment == null || successor == null || expectedTail == null || expectedCount <= 0
                    || (expectedCount == 1) != successor.isEmpty()) {
                throw new DatabaseValidationException("invalid undo free candidate");
            }
        }
    }

    /** 统一目录快照：cache bottom→top，free head→tail。 */
    public record ReuseSnapshot(List<CachedUndoSegmentRef> insert, List<CachedUndoSegmentRef> update,
                                List<FreeUndoSegmentRef> free) {
        public ReuseSnapshot {
            if (insert == null || update == null || free == null) {
                throw new DatabaseValidationException("undo reuse snapshot fields must not be null");
            }
            insert = List.copyOf(insert);
            update = List.copyOf(update);
            free = List.copyOf(free);
        }
    }

    /** truncate 单批：cache 按 top→bottom，free 按 head→tail。 */
    public record DrainBatch(int expectedInsertCount, int expectedUpdateCount, int expectedFreeCount,
                             List<CachedUndoSegmentRef> insertTopFirst,
                             List<CachedUndoSegmentRef> updateTopFirst,
                             List<FreeUndoSegmentRef> freeHeadFirst) {
        public DrainBatch {
            if (expectedInsertCount < 0 || expectedUpdateCount < 0 || expectedFreeCount < 0
                    || insertTopFirst == null || updateTopFirst == null || freeHeadFirst == null
                    || insertTopFirst.isEmpty() && updateTopFirst.isEmpty() && freeHeadFirst.isEmpty()) {
                throw new DatabaseValidationException("invalid undo reuse drain batch");
            }
            insertTopFirst = List.copyOf(insertTopFirst);
            updateTopFirst = List.copyOf(updateTopFirst);
            freeHeadFirst = List.copyOf(freeHeadFirst);
        }

        public int size() {
            return insertTopFirst.size() + updateTopFirst.size() + freeHeadFirst.size();
        }
    }

    /** cache top pop guard。 */
    static final class CachePopLease extends TransitionLease {
        private final CacheCandidate candidate;

        private CachePopLease(UndoSegmentReuseDirectory owner, CacheCandidate candidate) {
            super(owner);
            this.candidate = candidate;
        }

        CacheCandidate candidate() {
            return candidate;
        }

        @Override void validate() { owner.validateCachePop(candidate); }
        @Override void publish() { owner.completeCachePop(candidate); }
        @Override void abortBeforePhysical() { owner.releaseCacheTransition(candidate.segment().kind()); }
    }

    /** finalization cache push guard。 */
    static final class CachePushLease extends TransitionLease {
        private final CachedUndoSegmentRef segment;
        private final int expectedCount;

        private CachePushLease(UndoSegmentReuseDirectory owner, CachedUndoSegmentRef segment, int expectedCount) {
            super(owner);
            this.segment = segment;
            this.expectedCount = expectedCount;
        }

        CachedUndoSegmentRef segment() { return segment; }
        int expectedCount() { return expectedCount; }
        @Override void validate() { owner.validateCachePush(segment, expectedCount); }
        @Override void publish() { owner.completeCachePush(segment, expectedCount); }
        @Override void abortBeforePhysical() { owner.releaseCacheTransition(segment.kind()); }
    }

    /** free head pop guard。 */
    static final class FreePopLease extends TransitionLease {
        private final FreeCandidate candidate;

        private FreePopLease(UndoSegmentReuseDirectory owner, FreeCandidate candidate) {
            super(owner);
            this.candidate = candidate;
        }

        FreeCandidate candidate() { return candidate; }
        @Override void validate() { owner.validateFreePop(candidate); }
        @Override void publish() { owner.completeFreePop(candidate); }
        @Override void abortBeforePhysical() { owner.releaseFreeTransition(); }
    }

    /** finalization free tail batch push guard。 */
    static final class FreePushLease extends TransitionLease {
        private final List<FreeUndoSegmentRef> segments;
        private final int expectedCount;
        private final Optional<FreeUndoSegmentRef> expectedTail;

        private FreePushLease(UndoSegmentReuseDirectory owner, List<FreeUndoSegmentRef> segments,
                              int expectedCount, Optional<FreeUndoSegmentRef> expectedTail) {
            super(owner);
            this.segments = List.copyOf(segments);
            this.expectedCount = expectedCount;
            this.expectedTail = expectedTail;
        }

        List<FreeUndoSegmentRef> segments() { return segments; }
        int expectedCount() { return expectedCount; }
        Optional<FreeUndoSegmentRef> expectedTail() { return expectedTail; }
        @Override void validate() { owner.validateFreePush(this); }
        @Override void publish() { owner.completeFreePush(this); }
        @Override void abortBeforePhysical() { owner.releaseFreeTransition(); }
    }

    /** 三类 owner transition 的公共 fail-stop 状态机。 */
    private abstract static class TransitionLease implements AutoCloseable {
        final UndoSegmentReuseDirectory owner;
        private boolean physicalMutationStarted;
        private boolean completed;
        private boolean closed;

        private TransitionLease(UndoSegmentReuseDirectory owner) { this.owner = owner; }
        abstract void validate();
        abstract void publish();
        abstract void abortBeforePhysical();

        void physicalMutationStarted() {
            requireOpen();
            validate();
            physicalMutationStarted = true;
        }

        void complete() {
            requireOpen();
            if (!physicalMutationStarted) {
                throw new DatabaseValidationException("reuse transition cannot complete before physical mutation");
            }
            publish();
            completed = true;
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            if (!completed && !physicalMutationStarted) abortBeforePhysical();
        }

        private void requireOpen() {
            if (closed) throw new DatabaseValidationException("operation on closed undo reuse lease");
        }
    }

    /** lifecycle X owner 的全局 drain guard。 */
    public static final class DrainLease implements AutoCloseable {
        private final UndoSegmentReuseDirectory owner;
        private DrainBatch inFlight;
        private boolean physicalMutationStarted;
        private boolean finished;
        private boolean closed;

        private DrainLease(UndoSegmentReuseDirectory owner) { this.owner = owner; }

        /**
         * 按 INSERT cache top、UPDATE cache top、free head 顺序冻结下一批 owner；同一时刻只允许一批 in-flight。
         *
         * @param maxSegments 批次上限，必须为正数。
         * @return 空目录返回 empty，否则返回含运行期 expected count 的不可变批次。
         */
        public Optional<DrainBatch> nextBatch(int maxSegments) {
            requireOpen();
            if (maxSegments <= 0 || inFlight != null) {
                throw new DatabaseValidationException("invalid/in-flight undo reuse drain batch request");
            }
            inFlight = owner.nextDrainBatch(maxSegments);
            return Optional.ofNullable(inFlight);
        }

        /** 在任何 FSP/page3 修改前复核批次并把失败语义切换为 fail-stop。 */
        public void physicalMutationStarted(DrainBatch batch) {
            requireMatching(batch);
            owner.validateDrainBatch(batch);
            physicalMutationStarted = true;
        }

        /** MTR commit 后从运行期目录删除本批 owner，并允许规划下一批。 */
        public void completeBatch(DrainBatch batch) {
            requireMatching(batch);
            if (!physicalMutationStarted) {
                throw new DatabaseValidationException("reuse drain cannot complete before physical mutation");
            }
            owner.completeDrainBatch(batch);
            inFlight = null;
            physicalMutationStarted = false;
        }

        /** 全部批次完成且目录为空后释放 drain gate。 */
        public void finish() {
            requireOpen();
            if (inFlight != null) throw new DatabaseValidationException("reuse drain still has an in-flight batch");
            owner.finishDrain();
            finished = true;
        }

        /** 物理修改前退出会撤销 drain gate；越过物理边界后保留 gate，强制实例 fail-stop。 */
        @Override
        public void close() {
            if (closed) return;
            closed = true;
            if (!finished && !physicalMutationStarted) owner.cancelDrain();
        }

        private void requireMatching(DrainBatch batch) {
            requireOpen();
            if (batch == null || inFlight == null || !inFlight.equals(batch)) {
                throw new DatabaseValidationException("undo reuse drain batch does not match current lease");
            }
        }

        private void requireOpen() {
            if (closed) throw new DatabaseValidationException("operation on closed undo reuse drain lease");
        }
    }

    private void validateCachePop(CacheCandidate candidate) {
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(candidate.segment().kind());
            if (!cacheTransition(candidate.segment().kind()) || draining
                    || stack.size() != candidate.expectedCount() || !stack.getLast().equals(candidate.segment())) {
                throw new DatabaseValidationException("undo cache pop lease lost its owner");
            }
        } finally { lock.unlock(); }
    }

    private void completeCachePop(CacheCandidate candidate) {
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(candidate.segment().kind());
            if (!cacheTransition(candidate.segment().kind()) || stack.size() != candidate.expectedCount()
                    || !stack.getLast().equals(candidate.segment())) {
                throw new DatabaseValidationException("undo cache pop completion owner mismatch");
            }
            stack.removeLast();
            setCacheTransition(candidate.segment().kind(), false);
        } finally { lock.unlock(); }
    }

    private void validateCachePush(CachedUndoSegmentRef segment, int expectedCount) {
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(segment.kind());
            if (!cacheTransition(segment.kind()) || draining || stack.size() != expectedCount
                    || stack.size() >= capacityPerKind) {
                throw new DatabaseValidationException("undo cache push lease lost owner/capacity");
            }
        } finally { lock.unlock(); }
    }

    private void completeCachePush(CachedUndoSegmentRef segment, int expectedCount) {
        lock.lock();
        try {
            validateCachePushUnderLock(segment, expectedCount);
            stack(segment.kind()).add(segment);
            setCacheTransition(segment.kind(), false);
        } finally { lock.unlock(); }
    }

    private void validateCachePushUnderLock(CachedUndoSegmentRef segment, int expectedCount) {
        List<CachedUndoSegmentRef> stack = stack(segment.kind());
        if (!cacheTransition(segment.kind()) || stack.size() != expectedCount || stack.size() >= capacityPerKind) {
            throw new DatabaseValidationException("undo cache push completion owner/capacity mismatch");
        }
    }

    private void validateFreePop(FreeCandidate candidate) {
        lock.lock();
        try {
            if (!freeTransition || draining || !matchesFreeCandidate(candidate)) {
                throw new DatabaseValidationException("undo free pop lease lost its owner");
            }
        } finally { lock.unlock(); }
    }

    private void completeFreePop(FreeCandidate candidate) {
        lock.lock();
        try {
            if (!freeTransition || !matchesFreeCandidate(candidate)) {
                throw new DatabaseValidationException("undo free pop completion owner mismatch");
            }
            freeQueue.removeFirst();
            freeTransition = false;
        } finally { lock.unlock(); }
    }

    private void validateFreePush(FreePushLease lease) {
        lock.lock();
        try { requireFreePushState(lease); } finally { lock.unlock(); }
    }

    private void completeFreePush(FreePushLease lease) {
        lock.lock();
        try {
            requireFreePushState(lease);
            freeQueue.addAll(lease.segments());
            freeTransition = false;
        } finally { lock.unlock(); }
    }

    private void requireFreePushState(FreePushLease lease) {
        if (!freeTransition || draining || freeQueue.size() != lease.expectedCount()
                || !Optional.ofNullable(freeQueue.peekLast()).equals(lease.expectedTail())) {
            throw new DatabaseValidationException("undo free push lease lost owner/count");
        }
    }

    private void releaseCacheTransition(UndoLogKind kind) {
        lock.lock();
        try {
            if (!cacheTransition(kind)) throw new DatabaseValidationException("cache transition fence not held");
            setCacheTransition(kind, false);
        } finally { lock.unlock(); }
    }

    private void releaseFreeTransition() {
        lock.lock();
        try {
            if (!freeTransition) throw new DatabaseValidationException("free transition fence not held");
            freeTransition = false;
        } finally { lock.unlock(); }
    }

    private DrainBatch nextDrainBatch(int maxSegments) {
        lock.lock();
        try {
            requireDraining();
            if (insertStack.isEmpty() && updateStack.isEmpty() && freeQueue.isEmpty()) return null;
            int remaining = maxSegments;
            List<CachedUndoSegmentRef> insert = topFirst(insertStack, remaining);
            remaining -= insert.size();
            List<CachedUndoSegmentRef> update = topFirst(updateStack, remaining);
            remaining -= update.size();
            List<FreeUndoSegmentRef> free = freeQueue.stream().limit(remaining).toList();
            return new DrainBatch(insertStack.size(), updateStack.size(), freeQueue.size(), insert, update, free);
        } finally { lock.unlock(); }
    }

    private void validateDrainBatch(DrainBatch batch) {
        lock.lock();
        try { requireDraining(); requireDrainTops(batch); } finally { lock.unlock(); }
    }

    private void completeDrainBatch(DrainBatch batch) {
        lock.lock();
        try {
            requireDraining();
            requireDrainTops(batch);
            removeTop(insertStack, batch.insertTopFirst());
            removeTop(updateStack, batch.updateTopFirst());
            for (FreeUndoSegmentRef expected : batch.freeHeadFirst()) {
                if (!freeQueue.removeFirst().equals(expected)) {
                    throw new DatabaseValidationException("undo free drain head changed during completion");
                }
            }
        } finally { lock.unlock(); }
    }

    private void requireDrainTops(DrainBatch batch) {
        if (insertStack.size() != batch.expectedInsertCount()
                || updateStack.size() != batch.expectedUpdateCount()
                || freeQueue.size() != batch.expectedFreeCount()
                || !topFirst(insertStack, batch.insertTopFirst().size()).equals(batch.insertTopFirst())
                || !topFirst(updateStack, batch.updateTopFirst().size()).equals(batch.updateTopFirst())
                || !freeQueue.stream().limit(batch.freeHeadFirst().size()).toList().equals(batch.freeHeadFirst())) {
            throw new DatabaseValidationException("undo reuse drain batch no longer matches directory");
        }
    }

    private void finishDrain() {
        lock.lock();
        try {
            requireDraining();
            if (!insertStack.isEmpty() || !updateStack.isEmpty() || !freeQueue.isEmpty()) {
                throw new DatabaseValidationException("undo reuse drain cannot finish before all owners are empty");
            }
            draining = false;
        } finally { lock.unlock(); }
    }

    private void cancelDrain() {
        lock.lock();
        try { requireDraining(); draining = false; } finally { lock.unlock(); }
    }

    private void requireDraining() {
        if (!draining || insertTransition || updateTransition || freeTransition) {
            throw new DatabaseValidationException("undo reuse directory is not exclusively draining");
        }
    }

    private static List<CachedUndoSegmentRef> topFirst(List<CachedUndoSegmentRef> stack, int max) {
        int count = Math.min(Math.max(max, 0), stack.size());
        List<CachedUndoSegmentRef> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) result.add(stack.get(stack.size() - 1 - i));
        return List.copyOf(result);
    }

    private static void removeTop(List<CachedUndoSegmentRef> stack, List<CachedUndoSegmentRef> expected) {
        for (CachedUndoSegmentRef item : expected) {
            if (stack.isEmpty() || !stack.getLast().equals(item)) {
                throw new DatabaseValidationException("undo cache drain top changed during completion");
            }
            stack.removeLast();
        }
    }
}
