package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * rollback-segment 持久 history 链的运行时投影与单写转换门。
 *
 * <p>commit append 和 purge head unlink 都跨越“读取预检 → 磁盘 MTR → 内存发布”，不能只在队列增删瞬间加锁。
 * 本类用显式锁保护一个 transition flag；等待有独立 timeout，锁绝不跨磁盘 IO 持有。lease 冻结 head/tail/count，
 * 在进入首个物理修改前复核投影未变化，MTR commit 后才由 {@code complete()} 发布内存结果。
 *
 * <p>若 lease 在物理修改前关闭，会释放 transition 并唤醒等待者；若已调用 {@code physicalMutationStarted()}，
 * 未完成就关闭则保留 fail-stop flag，后续操作只能有界超时，避免进程继续覆盖未知的磁盘链状态。
 */
public final class HistoryList {

    /** 兼容低层测试的默认等待上限；生产组合根显式注入 EngineConfig。 */
    private static final Duration DEFAULT_TRANSITION_TIMEOUT = Duration.ofSeconds(5);

    /** 保护队列与 transition 状态；不跨任何 page/MTR IO 持有。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** transition 完成或在物理修改前取消时唤醒等待者。 */
    private final Condition transitionIdle = lock.newCondition();
    /** 等待另一个 commit/purge transition 的独立上限。 */
    private final Duration transitionTimeout;
    /** 按持久 prev/next 遍历顺序排列；TransactionNo 不要求单调。 */
    private final ArrayDeque<HistoryEntry> committed = new ArrayDeque<>();
    /** 当前唯一转换；非 null 时其他 writer 必须等待。 */
    private TransitionLease activeTransition;

    public HistoryList() {
        this(DEFAULT_TRANSITION_TIMEOUT);
    }

    public HistoryList(Duration transitionTimeout) {
        if (transitionTimeout == null || transitionTimeout.isZero() || transitionTimeout.isNegative()) {
            throw new DatabaseValidationException("history transition timeout must be positive");
        }
        this.transitionTimeout = transitionTimeout;
    }

    /**
     * 预约一次队尾 append。返回前只冻结运行时快照，不产生磁盘或队列副作用；调用方可安全执行只读预检。
     */
    public AppendLease beginAppend(HistoryEntry entry) {
        requireEntry(entry, "history append");
        lock.lock();
        try {
            awaitIdle();
            AppendLease lease = new AppendLease(entry, committed.peekFirst(), committed.peekLast(), committed.size());
            activeTransition = lease;
            return lease;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 预约一次队首摘除。expected 必须仍是运行时 head；purge 的 B+Tree 任务应在调用本方法前完成，缩短门占用时间。
     */
    public HeadRemovalLease beginHeadRemoval(HistoryEntry expected) {
        requireEntry(expected, "history head removal");
        lock.lock();
        try {
            awaitIdle();
            HistoryEntry current = committed.peekFirst();
            if (!expected.equals(current)) {
                throw new DatabaseValidationException("committed history head mismatch: expected="
                        + expected + ", current=" + current);
            }
            HeadRemovalLease lease = new HeadRemovalLease(expected, committed.peekLast(), committed.size());
            activeTransition = lease;
            return lease;
        } finally {
            lock.unlock();
        }
    }

    /** 查看物理链首节点，不移除。 */
    public Optional<HistoryEntry> peekCommitted() {
        lock.lock();
        try {
            return Optional.ofNullable(committed.peekFirst());
        } finally {
            lock.unlock();
        }
    }

    /** 当前持久 history 投影长度。 */
    public int committedSize() {
        lock.lock();
        try {
            return committed.size();
        } finally {
            lock.unlock();
        }
    }

    /** 返回按物理 prev/next 链顺序冻结的不可变快照，供诊断和持久 preflight 使用。 */
    public List<HistoryEntry> snapshot() {
        lock.lock();
        try {
            return List.copyOf(committed);
        } finally {
            lock.unlock();
        }
    }

    /**
     * recovery 只允许在空且无转换的组合根初始化阶段恢复物理链顺序。事务号可以局部倒序，但 identity 必须唯一。
     */
    public void restore(List<HistoryEntry> physicalOrder) {
        if (physicalOrder == null || physicalOrder.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("restored history entries must not be null");
        }
        lock.lock();
        try {
            if (activeTransition != null || !committed.isEmpty()) {
                throw new DatabaseValidationException("history restore requires an empty idle projection");
            }
            Set<Long> transactionNos = new HashSet<>();
            Set<Long> creators = new HashSet<>();
            Set<PageId> firstPages = new HashSet<>();
            for (HistoryEntry entry : physicalOrder) {
                if (!transactionNos.add(entry.transactionNo().value())
                        || !creators.add(entry.creatorTrxId().value())
                        || !firstPages.add(entry.undoFirstPageId())) {
                    throw new DatabaseValidationException("restored history contains duplicate identity: " + entry);
                }
            }
            committed.addAll(physicalOrder);
        } finally {
            lock.unlock();
        }
    }

    /** 等待当前转换完成；中断与超时都发生在任何新物理修改前，调用方可重试。 */
    private void awaitIdle() {
        long remaining;
        try {
            remaining = transitionTimeout.toNanos();
        } catch (ArithmeticException overflow) {
            remaining = Long.MAX_VALUE;
        }
        while (activeTransition != null) {
            if (remaining <= 0L) {
                throw new HistoryTransitionTimeoutException("history transition did not become idle within "
                        + transitionTimeout);
            }
            try {
                remaining = transitionIdle.awaitNanos(remaining);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new HistoryTransitionInterruptedException(
                        "interrupted while waiting for history transition", interrupted);
            }
        }
    }

    private static void requireEntry(HistoryEntry entry, String operation) {
        if (entry == null) {
            throw new DatabaseValidationException(operation + " entry must not be null");
        }
    }

    /** 一次跨 IO history 转换的公共生命周期。 */
    public abstract class TransitionLease implements AutoCloseable {
        private final HistoryEntry expectedHead;
        private final HistoryEntry expectedTail;
        private final int expectedSize;
        private boolean physicalMutationStarted;
        private boolean completed;

        private TransitionLease(HistoryEntry expectedHead, HistoryEntry expectedTail, int expectedSize) {
            this.expectedHead = expectedHead;
            this.expectedTail = expectedTail;
            this.expectedSize = expectedSize;
        }

        /** 冻结时的物理 head；空链为 empty。 */
        public Optional<HistoryEntry> expectedHead() {
            return Optional.ofNullable(expectedHead);
        }

        /** 冻结时的物理 tail；空链为 empty。 */
        public Optional<HistoryEntry> expectedTail() {
            return Optional.ofNullable(expectedTail);
        }

        /** 冻结时的节点数。 */
        public int expectedSize() {
            return expectedSize;
        }

        /**
         * 标记即将进入物理修改阶段。调用前再次复核运行时投影；此后即使 MTR 尚可回滚，也保守视为结果不确定，
         * 未 {@link #complete()} 就关闭会保留 fail-stop fence。
         */
        public final void physicalMutationStarted() {
            lock.lock();
            try {
                requireOwner();
                if (physicalMutationStarted) {
                    throw new DatabaseValidationException("history physical mutation boundary entered twice");
                }
                requireFrozenProjection();
                physicalMutationStarted = true;
            } finally {
                lock.unlock();
            }
        }

        /** 磁盘 MTR 已提交后发布队列变更并唤醒下一个转换。 */
        public final void complete() {
            lock.lock();
            try {
                requireOwner();
                if (!physicalMutationStarted) {
                    throw new DatabaseValidationException("history transition completed before physical mutation");
                }
                requireFrozenProjection();
                publish();
                completed = true;
                activeTransition = null;
                transitionIdle.signalAll();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public final void close() {
            lock.lock();
            try {
                if (completed) {
                    return;
                }
                requireOwner();
                if (!physicalMutationStarted) {
                    activeTransition = null;
                    transitionIdle.signalAll();
                    return;
                }
                // 已越过物理边界时故意不清 flag：未知磁盘状态不能允许同进程继续写链。
                throw new DatabaseFatalException("history transition abandoned after physical mutation boundary");
            } finally {
                lock.unlock();
            }
        }

        private void requireOwner() {
            if (activeTransition != this) {
                throw new DatabaseValidationException("history transition lease is stale or already completed");
            }
        }

        private void requireFrozenProjection() {
            if (committed.size() != expectedSize
                    || !Objects.equals(committed.peekFirst(), expectedHead)
                    || !Objects.equals(committed.peekLast(), expectedTail)) {
                throw new DatabaseFatalException("history runtime projection changed during active transition");
            }
        }

        abstract void publish();
    }

    /** commit append lease；entry 只在 persistent MTR commit 后进入运行时队尾。 */
    public final class AppendLease extends TransitionLease {
        private final HistoryEntry entry;

        private AppendLease(HistoryEntry entry, HistoryEntry expectedHead,
                            HistoryEntry expectedTail, int expectedSize) {
            super(expectedHead, expectedTail, expectedSize);
            this.entry = entry;
        }

        public HistoryEntry entry() {
            return entry;
        }

        @Override
        void publish() {
            committed.addLast(entry);
        }
    }

    /** purge head-removal lease；expected 只在 persistent unlink commit 后从运行时队首删除。 */
    public final class HeadRemovalLease extends TransitionLease {
        private final HistoryEntry expected;

        private HeadRemovalLease(HistoryEntry expected, HistoryEntry expectedTail, int expectedSize) {
            super(expected, expectedTail, expectedSize);
            this.expected = expected;
        }

        public HistoryEntry expected() {
            return expected;
        }

        @Override
        void publish() {
            HistoryEntry current = committed.peekFirst();
            if (!expected.equals(current)) {
                throw new DatabaseFatalException("history head changed before removal publication");
            }
            committed.removeFirst();
        }
    }
}
