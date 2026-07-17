package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.TablePurgeBarrierInterruptedException;
import cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    /** commit append/purge remove/recovery restore 改变表引用计数时唤醒 DROP barrier。 */
    private final Condition tableReferencesChanged = lock.newCondition();
    /** 等待另一个 commit/purge transition 的独立上限。 */
    private final Duration transitionTimeout;
    /** 按持久 prev/next 遍历顺序排列；TransactionNo 不要求单调。 */
    private final ArrayDeque<HistoryEntry> committed = new ArrayDeque<>();
    /** tableId→包含该表的 committed history entry 数；与 committed 队列在同一锁内原子发布。 */
    private final Map<Long, Integer> tableReferenceCounts = new HashMap<>();
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

    /**
     * 返回当前运行时 committed history 投影长度。
     *
     * @return 在短显式锁内取得的非负 entry 数；不包含 active undo 或正在构造但未发布的 append lease。
     */
    public int committedSize() {
        lock.lock();
        try {
            return committed.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 返回目标表当前被多少条 committed history entry 引用。
     *
     * @param tableId DD 分配的稳定正表 id。
     * @return 包含该表的 committed entry 数；同一 entry 内重复记录只计一次，没有引用时返回零。
     * @throws DatabaseValidationException table id 非正时抛出。
     */
    int tableReferenceCount(long tableId) {
        requireTableId(tableId);
        lock.lock();
        try {
            return tableReferenceCounts.getOrDefault(tableId, 0);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 有界等待目标表引用归零。Condition 只在 history 显式锁内等待；调用方进入本方法前不得持有 MTR/page/file 资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 table id/timeout 并转换等待预算；超大 Duration 饱和为 {@link Long#MAX_VALUE} 纳秒。</li>
     *     <li>持 history lock 循环检查引用数；非零时用 Condition 释放锁并有界等待 purge finalization 唤醒。</li>
     *     <li>引用归零后在同一锁内确认并返回；超时或中断不修改队列、计数或 DD 状态。</li>
     * </ol>
     *
     * @param tableId 待 DROP/恢复续作表的稳定正 id。
     * @param timeout 最大等待时长，必须为正值。
     * @throws DatabaseValidationException table id 或 timeout 无效时抛出。
     * @throws TablePurgeBarrierTimeoutException 预算耗尽且引用仍非零时抛出；调用方应保持 ACTIVE/DROP_PENDING 原状态。
     * @throws TablePurgeBarrierInterruptedException 等待线程被中断时抛出；方法会恢复 interrupt flag。
     */
    void awaitTableUnreferenced(long tableId, Duration timeout) {
        // 1. 校验与时间换算先于共享锁，失败时不需要释放任何 history 资源。
        requireTableId(tableId);
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("table purge barrier timeout must be positive");
        }
        long remaining;
        try {
            remaining = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            remaining = Long.MAX_VALUE;
        }
        // 2. Condition.awaitNanos 会原子释放/重取同一显式锁；循环防止虚假唤醒和其它表的无关通知。
        lock.lock();
        try {
            while (tableReferenceCounts.getOrDefault(tableId, 0) > 0) {
                if (remaining <= 0L) {
                    throw new TablePurgeBarrierTimeoutException(
                            "timed out waiting for table history references: table=" + tableId
                                    + ", references=" + tableReferenceCounts.getOrDefault(tableId, 0)
                                    + ", timeout=" + timeout);
                }
                try {
                    remaining = tableReferencesChanged.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new TablePurgeBarrierInterruptedException(
                            "interrupted while waiting for table history references: table=" + tableId,
                            interrupted);
                }
            }
            // 3. 只有在锁内观察到计数归零才返回；队列变化与计数投影不会被并发 append/removal 撕裂。
        } finally {
            lock.unlock();
        }
    }

    /**
     * 返回按物理 prev/next 链顺序冻结的不可变快照，供诊断和持久 preflight 使用。
     *
     * @return head→tail 顺序的不可变 entry 列表；返回后不持 history lock。
     */
    public List<HistoryEntry> snapshot() {
        lock.lock();
        try {
            return List.copyOf(committed);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在组合根初始化阶段恢复 persistent history 的物理链顺序，并同步重建 affected-table 引用计数。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在加锁前校验列表及元素非空，避免用不完整恢复结果污染共享投影。</li>
     *     <li>持锁确认队列、引用计数和 transition 均为空；运行期不得覆盖已有 history。</li>
     *     <li>校验 transaction no、creator id、first page identity 全局唯一，再按物理顺序发布队列。</li>
     *     <li>从每条 entry 的 affectedTableIds 重建计数并唤醒等待者；异常时不对外开放流量。</li>
     * </ol>
     *
     * @param physicalOrder recovery 已按 page3/first-page 链闭包校验的 head→tail 不可变顺序。
     * @throws DatabaseValidationException 列表为空引用、当前投影非空/有转换或任一稳定 identity 重复时抛出。
     * @throws cn.zhangyis.db.common.exception.DatabaseFatalException 表引用计数溢出时抛出，恢复必须 fail-closed。
     */
    public void restore(List<HistoryEntry> physicalOrder) {
        // 1. 纯输入校验不接触共享队列。
        if (physicalOrder == null || physicalOrder.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("restored history entries must not be null");
        }
        // 2. restore 只能发生在空闲初始化状态，不能与前台 append/purge transition 并发。
        lock.lock();
        try {
            if (activeTransition != null || !committed.isEmpty() || !tableReferenceCounts.isEmpty()) {
                throw new DatabaseValidationException("history restore requires an empty idle projection");
            }
            // 3. 三类 identity 分别唯一，防止不同 slot/first page 被错误折叠成同一历史事务。
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
            // 4. 队列与计数在同一锁内发布；signal 让恢复期/测试中已有等待者重新检查条件。
            committed.addAll(physicalOrder);
            for (HistoryEntry entry : physicalOrder) {
                addTableReferences(entry);
            }
            tableReferencesChanged.signalAll();
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

    private static void requireTableId(long tableId) {
        if (tableId <= 0L) {
            throw new DatabaseValidationException("history table id must be positive: " + tableId);
        }
    }

    /** 当前已持有 lock；同一 entry 对同一表只加一次，因为 HistoryEntry 集合已去重。 */
    private void addTableReferences(HistoryEntry entry) {
        for (long tableId : entry.affectedTableIds()) {
            int current = tableReferenceCounts.getOrDefault(tableId, 0);
            if (current == Integer.MAX_VALUE) {
                throw new DatabaseFatalException(
                        "history table reference count exhausted: table=" + tableId);
            }
            tableReferenceCounts.put(tableId, current + 1);
        }
    }

    /** 当前已持有 lock；缺失/负数说明队列与计数投影漂移，必须 fail-stop。 */
    private void removeTableReferences(HistoryEntry entry) {
        for (long tableId : entry.affectedTableIds()) {
            Integer current = tableReferenceCounts.get(tableId);
            if (current == null || current <= 0) {
                throw new DatabaseFatalException(
                        "history table reference projection is missing: table=" + tableId);
            }
            if (current == 1) {
                tableReferenceCounts.remove(tableId);
            } else {
                tableReferenceCounts.put(tableId, current - 1);
            }
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
                tableReferencesChanged.signalAll();
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
            addTableReferences(entry);
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
            removeTableReferences(expected);
            committed.removeFirst();
        }
    }
}
