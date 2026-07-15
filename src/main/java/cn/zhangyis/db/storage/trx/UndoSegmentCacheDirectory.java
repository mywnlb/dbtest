package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.UndoLogKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * rollback segment cached undo 栈的运行期投影。INSERT/UPDATE 各自使用固定容量 LIFO；page3 才是 crash 后权威，
 * 本类通过 push/pop/drain RAII lease 保护“内存预留—物理 MTR—内存发布”窗口。
 *
 * <p><b>并发边界</b>：{@link #lock} 只保护两个短数组、per-kind transition 标志和全局 drain 标志。锁内不访问
 * Buffer Pool、FSP、redo 或文件，也不等待 lifecycle/page latch。truncate 在持 lifecycle X lease 时只能
 * {@link #tryBeginDrain()}，失败必须释放 X 后重试，禁止在 X 下等待尚未取得 MTR S lease 的 finalizer。
 */
public final class UndoSegmentCacheDirectory {

    /** 每个 kind 的持久容量；0 显式禁用。 */
    private final int capacityPerKind;
    /** INSERT cached 栈，顺序为栈底到栈顶。 */
    private final List<CachedUndoSegmentRef> insertStack = new ArrayList<>();
    /** UPDATE cached 栈，顺序为栈底到栈顶。 */
    private final List<CachedUndoSegmentRef> updateStack = new ArrayList<>();
    /** 保护全部目录状态的短锁，不跨越任何物理操作。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** INSERT 是否已有 push/pop lease 跨出短锁执行物理 MTR。 */
    private boolean insertTransition;
    /** UPDATE 是否已有 push/pop lease 跨出短锁执行物理 MTR。 */
    private boolean updateTransition;
    /** truncate 是否已封锁两个栈；为 true 时普通 push/pop 均不可进入。 */
    private boolean draining;

    /**
     * 创建固定容量运行期投影。容量必须与 page3 持久格式完全一致；0 表保留 owner 转移协议但禁用 push。
     *
     * @param capacityPerKind INSERT、UPDATE 各自允许缓存的 segment 数。
     */
    public UndoSegmentCacheDirectory(int capacityPerKind) {
        if (capacityPerKind < 0) {
            throw new DatabaseValidationException("undo cache capacity must not be negative: " + capacityPerKind);
        }
        this.capacityPerKind = capacityPerKind;
    }

    /** 当前 kind 的不可变栈顶规划证据；drain/transition 中返回 empty，让新计划避免依赖不稳定 owner。 */
    public Optional<CacheCandidate> peek(UndoLogKind kind) {
        requireCacheKind(kind);
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (draining || transition(kind) || stack.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new CacheCandidate(stack.getLast(), stack.size()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 预留 expected cache top 供首写复用。证据变化或 drain/并发 transition 均是可重规划 stale，且此时尚未触碰页。
     */
    PopLease reservePop(CacheCandidate expected) {
        if (expected == null) {
            throw new DatabaseValidationException("undo cache pop candidate must not be null");
        }
        UndoLogKind kind = expected.segment().kind();
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (draining || transition(kind) || stack.size() != expected.expectedCount()
                    || stack.isEmpty() || !stack.getLast().equals(expected.segment())) {
                throw new UndoWriteStalePlanException("cached undo top changed before reuse: kind=" + kind);
            }
            setTransition(kind, true);
            return new PopLease(this, expected);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 尝试为终结后的单页 segment 预留 push。容量满、drain 或同 kind transition 正忙时不等待，调用方直接 drop 新段。
     */
    Optional<PushLease> tryReservePush(CachedUndoSegmentRef segment) {
        if (segment == null) {
            throw new DatabaseValidationException("cached undo push segment must not be null");
        }
        UndoLogKind kind = segment.kind();
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (draining || transition(kind) || stack.size() >= capacityPerKind) {
                return Optional.empty();
            }
            if (containsFirstPage(segment)) {
                throw new DatabaseValidationException("undo segment already exists in cache directory: "
                        + segment.handle().firstPageId());
            }
            setTransition(kind, true);
            return Optional.of(new PushLease(this, segment, stack.size()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * lifecycle X owner 使用的非阻塞全局 drain gate。若已有 push/pop 跨出短锁，返回 empty；调用方不得在 X 下等待。
     */
    public Optional<DrainLease> tryBeginDrain() {
        lock.lock();
        try {
            if (draining || insertTransition || updateTransition) {
                return Optional.empty();
            }
            draining = true;
            return Optional.of(new DrainLease(this));
        } finally {
            lock.unlock();
        }
    }

    /** 恢复期在全部 page3/first-page/FSP 证据验证完成后一次性安装两个持久栈。 */
    public void restore(List<CachedUndoSegmentRef> insert, List<CachedUndoSegmentRef> update) {
        if (insert == null || update == null) {
            throw new DatabaseValidationException("recovered undo cache stacks must not be null");
        }
        lock.lock();
        try {
            if (draining || insertTransition || updateTransition || !insertStack.isEmpty() || !updateStack.isEmpty()) {
                throw new DatabaseValidationException("undo cache restore requires an empty idle directory");
            }
            validateRecoveredStack(insert, UndoLogKind.INSERT);
            validateRecoveredStack(update, UndoLogKind.UPDATE);
            Set<Object> pages = new HashSet<>();
            for (CachedUndoSegmentRef ref : concat(insert, update)) {
                if (!pages.add(ref.handle().firstPageId())) {
                    throw new DatabaseValidationException("duplicate recovered cached undo first page: "
                            + ref.handle().firstPageId());
                }
            }
            insertStack.addAll(insert);
            updateStack.addAll(update);
        } finally {
            lock.unlock();
        }
    }

    /** 当前 kind 已发布 cached segment 数，用于诊断与测试；RESERVED transition 尚未计入/移出。 */
    public int cachedCount(UndoLogKind kind) {
        requireCacheKind(kind);
        lock.lock();
        try {
            return stack(kind).size();
        } finally {
            lock.unlock();
        }
    }

    public int capacityPerKind() {
        return capacityPerKind;
    }

    /**
     * 复制两个运行期栈，顺序均为栈底到栈顶。truncate 在取得 drain gate 后用它与 page3 全量比对，
     * 不能只比较 count 后就在 FSP drop 之后才发现 top owner 漂移。
     */
    public CacheSnapshot snapshot() {
        lock.lock();
        try {
            return new CacheSnapshot(insertStack, updateStack);
        } finally {
            lock.unlock();
        }
    }

    private void validateRecoveredStack(List<CachedUndoSegmentRef> recovered, UndoLogKind expectedKind) {
        if (recovered.size() > capacityPerKind || recovered.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("recovered undo cache exceeds configured capacity for "
                    + expectedKind);
        }
        for (CachedUndoSegmentRef ref : recovered) {
            if (ref.kind() != expectedKind) {
                throw new DatabaseValidationException("recovered undo cache kind mismatch: expected="
                        + expectedKind + ", current=" + ref.kind());
            }
        }
    }

    private boolean containsFirstPage(CachedUndoSegmentRef candidate) {
        return concat(insertStack, updateStack).stream()
                .anyMatch(item -> item.handle().firstPageId().equals(candidate.handle().firstPageId()));
    }

    private List<CachedUndoSegmentRef> stack(UndoLogKind kind) {
        return switch (kind) {
            case INSERT -> insertStack;
            case UPDATE -> updateStack;
            case TEMPORARY -> throw new DatabaseValidationException("temporary undo has no cache stack");
        };
    }

    private boolean transition(UndoLogKind kind) {
        return switch (kind) {
            case INSERT -> insertTransition;
            case UPDATE -> updateTransition;
            case TEMPORARY -> throw new DatabaseValidationException("temporary undo has no cache transition");
        };
    }

    private void setTransition(UndoLogKind kind, boolean value) {
        switch (kind) {
            case INSERT -> insertTransition = value;
            case UPDATE -> updateTransition = value;
            case TEMPORARY -> throw new DatabaseValidationException("temporary undo has no cache transition");
        }
    }

    private static void requireCacheKind(UndoLogKind kind) {
        if (kind == null || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("ordinary undo cache kind must be INSERT or UPDATE");
        }
    }

    private static List<CachedUndoSegmentRef> concat(List<CachedUndoSegmentRef> first,
                                                      List<CachedUndoSegmentRef> second) {
        List<CachedUndoSegmentRef> all = new ArrayList<>(first.size() + second.size());
        all.addAll(first);
        all.addAll(second);
        return all;
    }

    /** 规划期冻结的 LIFO top 与持久 count。 */
    public record CacheCandidate(CachedUndoSegmentRef segment, int expectedCount) {
        public CacheCandidate {
            if (segment == null || expectedCount <= 0) {
                throw new DatabaseValidationException("invalid undo cache candidate");
            }
        }
    }

    /** 两个普通 undo cache 栈的不可变运行期快照，列表顺序为栈底到栈顶。 */
    public record CacheSnapshot(List<CachedUndoSegmentRef> insert,
                                List<CachedUndoSegmentRef> update) {
        public CacheSnapshot {
            if (insert == null || update == null
                    || insert.stream().anyMatch(java.util.Objects::isNull)
                    || update.stream().anyMatch(java.util.Objects::isNull)) {
                throw new DatabaseValidationException("undo cache snapshot stacks must not contain null");
            }
            insert = List.copyOf(insert);
            update = List.copyOf(update);
        }
    }

    /** truncate 一批最多若干 top entries；两个列表均按栈顶到栈底排列。 */
    public record DrainBatch(int expectedInsertCount, int expectedUpdateCount,
                             List<CachedUndoSegmentRef> insertTopFirst,
                             List<CachedUndoSegmentRef> updateTopFirst) {
        public DrainBatch {
            if (expectedInsertCount < 0 || expectedUpdateCount < 0
                    || insertTopFirst == null || updateTopFirst == null
                    || insertTopFirst.isEmpty() && updateTopFirst.isEmpty()) {
                throw new DatabaseValidationException("invalid undo cache drain batch");
            }
            insertTopFirst = List.copyOf(insertTopFirst);
            updateTopFirst = List.copyOf(updateTopFirst);
        }

        /** 当前批次全部 segment，保持 INSERT 后 UPDATE 的稳定诊断顺序。 */
        public List<CachedUndoSegmentRef> segments() {
            return concat(insertTopFirst, updateTopFirst);
        }
    }

    /** cached top pop 租约；物理边界后的失败保留 per-kind fence。 */
    static final class PopLease implements AutoCloseable {
        private final UndoSegmentCacheDirectory owner;
        private final CacheCandidate candidate;
        private boolean physicalMutationStarted;
        private boolean completed;
        private boolean closed;

        private PopLease(UndoSegmentCacheDirectory owner, CacheCandidate candidate) {
            this.owner = owner;
            this.candidate = candidate;
        }

        CacheCandidate candidate() {
            return candidate;
        }

        void physicalMutationStarted() {
            requireOpen();
            owner.validatePop(candidate);
            physicalMutationStarted = true;
        }

        void complete() {
            requireOpen();
            if (!physicalMutationStarted) {
                throw new DatabaseValidationException("cache pop cannot complete before physical mutation");
            }
            owner.completePop(candidate);
            completed = true;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (!completed) {
                owner.closeTransition(candidate.segment().kind(), physicalMutationStarted);
            }
        }

        private void requireOpen() {
            if (closed) {
                throw new DatabaseValidationException("operation on closed undo cache pop lease");
            }
        }
    }

    /** finalization cache push 租约；容量和 expected count 在物理写前冻结。 */
    static final class PushLease implements AutoCloseable {
        private final UndoSegmentCacheDirectory owner;
        private final CachedUndoSegmentRef segment;
        private final int expectedCount;
        private boolean physicalMutationStarted;
        private boolean completed;
        private boolean closed;

        private PushLease(UndoSegmentCacheDirectory owner, CachedUndoSegmentRef segment, int expectedCount) {
            this.owner = owner;
            this.segment = segment;
            this.expectedCount = expectedCount;
        }

        CachedUndoSegmentRef segment() {
            return segment;
        }

        int expectedCount() {
            return expectedCount;
        }

        void physicalMutationStarted() {
            requireOpen();
            owner.validatePush(segment, expectedCount);
            physicalMutationStarted = true;
        }

        void complete() {
            requireOpen();
            if (!physicalMutationStarted) {
                throw new DatabaseValidationException("cache push cannot complete before physical mutation");
            }
            owner.completePush(segment, expectedCount);
            completed = true;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (!completed) {
                owner.closeTransition(segment.kind(), physicalMutationStarted);
            }
        }

        private void requireOpen() {
            if (closed) {
                throw new DatabaseValidationException("operation on closed undo cache push lease");
            }
        }
    }

    /** lifecycle X owner 的全局 drain guard；不持 lock 跨 MTR，但阻止普通 push/pop。 */
    public static final class DrainLease implements AutoCloseable {
        private final UndoSegmentCacheDirectory owner;
        private DrainBatch inFlight;
        private boolean physicalMutationStarted;
        private boolean finished;
        private boolean closed;

        private DrainLease(UndoSegmentCacheDirectory owner) {
            this.owner = owner;
        }

        /** 取得下一批 top entries；返回 empty 表示两个栈均已排空。 */
        public Optional<DrainBatch> nextBatch(int maxSegments) {
            requireOpen();
            if (maxSegments <= 0 || inFlight != null) {
                throw new DatabaseValidationException("invalid/in-flight undo cache drain batch request");
            }
            inFlight = owner.nextDrainBatch(maxSegments);
            return Optional.ofNullable(inFlight);
        }

        /** 在本批首个 FSP/page3 写之前建立 fail-stop 边界。 */
        public void physicalMutationStarted(DrainBatch batch) {
            requireMatchingBatch(batch);
            owner.validateDrainBatch(batch);
            physicalMutationStarted = true;
        }

        /** 批次 MTR commit 后发布内存 pop；之后可以请求下一批。 */
        public void completeBatch(DrainBatch batch) {
            requireMatchingBatch(batch);
            if (!physicalMutationStarted) {
                throw new DatabaseValidationException("cache drain batch cannot complete before physical mutation");
            }
            owner.completeDrainBatch(batch);
            inFlight = null;
            physicalMutationStarted = false;
        }

        /** 两个栈为空后释放全局 gate。 */
        public void finish() {
            requireOpen();
            if (inFlight != null) {
                throw new DatabaseValidationException("cannot finish undo cache drain with an in-flight batch");
            }
            owner.finishDrain();
            finished = true;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (!finished) {
                owner.closeDrain(physicalMutationStarted);
            }
        }

        private void requireMatchingBatch(DrainBatch batch) {
            requireOpen();
            if (batch == null || inFlight == null || !inFlight.equals(batch)) {
                throw new DatabaseValidationException("undo cache drain batch does not match current lease");
            }
        }

        private void requireOpen() {
            if (closed) {
                throw new DatabaseValidationException("operation on closed undo cache drain lease");
            }
        }
    }

    private void validatePop(CacheCandidate candidate) {
        lock.lock();
        try {
            UndoLogKind kind = candidate.segment().kind();
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (!transition(kind) || draining || stack.size() != candidate.expectedCount()
                    || !stack.getLast().equals(candidate.segment())) {
                throw new DatabaseValidationException("undo cache pop lease lost its owner");
            }
        } finally {
            lock.unlock();
        }
    }

    private void completePop(CacheCandidate candidate) {
        lock.lock();
        try {
            UndoLogKind kind = candidate.segment().kind();
            List<CachedUndoSegmentRef> stack = stack(kind);
            if (!transition(kind) || stack.size() != candidate.expectedCount()
                    || !stack.getLast().equals(candidate.segment())) {
                throw new DatabaseValidationException("undo cache pop completion owner mismatch");
            }
            stack.removeLast();
            setTransition(kind, false);
        } finally {
            lock.unlock();
        }
    }

    private void validatePush(CachedUndoSegmentRef segment, int expectedCount) {
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(segment.kind());
            if (!transition(segment.kind()) || draining || stack.size() != expectedCount
                    || stack.size() >= capacityPerKind || containsFirstPage(segment)) {
                throw new DatabaseValidationException("undo cache push lease lost its owner/capacity");
            }
        } finally {
            lock.unlock();
        }
    }

    private void completePush(CachedUndoSegmentRef segment, int expectedCount) {
        lock.lock();
        try {
            List<CachedUndoSegmentRef> stack = stack(segment.kind());
            if (!transition(segment.kind()) || stack.size() != expectedCount
                    || stack.size() >= capacityPerKind || containsFirstPage(segment)) {
                throw new DatabaseValidationException("undo cache push completion owner/capacity mismatch");
            }
            stack.add(segment);
            setTransition(segment.kind(), false);
        } finally {
            lock.unlock();
        }
    }

    private void closeTransition(UndoLogKind kind, boolean physicalMutationStarted) {
        lock.lock();
        try {
            if (!transition(kind)) {
                throw new DatabaseValidationException("undo cache transition fence is not held for " + kind);
            }
            if (!physicalMutationStarted) {
                setTransition(kind, false);
            }
        } finally {
            lock.unlock();
        }
    }

    private DrainBatch nextDrainBatch(int maxSegments) {
        lock.lock();
        try {
            requireDraining();
            if (insertStack.isEmpty() && updateStack.isEmpty()) {
                return null;
            }
            int remaining = maxSegments;
            List<CachedUndoSegmentRef> insert = topFirst(insertStack, remaining);
            remaining -= insert.size();
            List<CachedUndoSegmentRef> update = topFirst(updateStack, remaining);
            return new DrainBatch(insertStack.size(), updateStack.size(), insert, update);
        } finally {
            lock.unlock();
        }
    }

    private void validateDrainBatch(DrainBatch batch) {
        lock.lock();
        try {
            requireDraining();
            requireDrainTops(batch);
        } finally {
            lock.unlock();
        }
    }

    private void completeDrainBatch(DrainBatch batch) {
        lock.lock();
        try {
            requireDraining();
            requireDrainTops(batch);
            removeTop(insertStack, batch.insertTopFirst());
            removeTop(updateStack, batch.updateTopFirst());
        } finally {
            lock.unlock();
        }
    }

    private void requireDrainTops(DrainBatch batch) {
        if (insertStack.size() != batch.expectedInsertCount()
                || updateStack.size() != batch.expectedUpdateCount()
                || !topFirst(insertStack, batch.insertTopFirst().size()).equals(batch.insertTopFirst())
                || !topFirst(updateStack, batch.updateTopFirst().size()).equals(batch.updateTopFirst())) {
            throw new DatabaseValidationException("undo cache drain batch no longer matches directory tops");
        }
    }

    private void finishDrain() {
        lock.lock();
        try {
            requireDraining();
            if (!insertStack.isEmpty() || !updateStack.isEmpty()) {
                throw new DatabaseValidationException("undo cache drain cannot finish before both stacks are empty");
            }
            draining = false;
        } finally {
            lock.unlock();
        }
    }

    private void closeDrain(boolean physicalMutationStarted) {
        lock.lock();
        try {
            requireDraining();
            if (!physicalMutationStarted) {
                draining = false;
            }
        } finally {
            lock.unlock();
        }
    }

    private void requireDraining() {
        if (!draining || insertTransition || updateTransition) {
            throw new DatabaseValidationException("undo cache directory is not exclusively draining");
        }
    }

    private static List<CachedUndoSegmentRef> topFirst(List<CachedUndoSegmentRef> stack, int max) {
        int count = Math.min(Math.max(max, 0), stack.size());
        List<CachedUndoSegmentRef> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(stack.get(stack.size() - 1 - i));
        }
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
