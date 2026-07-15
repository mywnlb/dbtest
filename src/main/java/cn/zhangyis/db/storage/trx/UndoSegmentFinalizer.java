package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderSnapshot;
import cn.zhangyis.db.storage.undo.RollbackSegmentHistoryBase;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoHistoryNodeSnapshot;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * undo segment 终态协调器。它按 eligibility 把 FSP segment drop 或 page3 active→cache owner 转移放入同一
 * MTR/redo batch，并在 commit 后才发布内存 slot/cache，闭合 INSERT commit、live/recovery rollback 与
 * committed purge 的 crash 边界。
 *
 * <p><b>锁序</b>：预检 MTR 先读 page3、再读普通 undo first page；返回前释放二者。随后独立只读 MTR 读取
 * inode page2 并物化 drop 规模，提交后才进入 finalization 写 MTR。写 MTR 先由
 * {@link UndoSpaceAllocator#dropUndoSegment} 修改 FSP page0/page2，再获取 page3 X latch 做 owner CAS。当前 XDES
 * 内嵌 page0，故不需要逆序例外；任何时刻都不跨 MTR 保留 page latch/fix，也不在内存 slot/history 锁内等待 IO。
 *
 * <p><b>失败语义</b>：可预测的 owner/state/head 冲突全部在物理写前抛出。进入 finalization MTR 后的异常属于
 * fail-stop：MTR 无 content undo，不能承诺同进程 buffer 可重试，故统一抛 {@link UndoFinalizationException}。
 */
public final class UndoSegmentFinalizer {

    /** finalization 预检与最终物理批次的短 MTR 来源。 */
    private final MiniTransactionManager mtrManager;
    /** 打开 undo first page、校验状态并取得 segment handle 的稳定入口。 */
    private final UndoLogSegmentAccess undoAccess;
    /** 在最终 MTR 内释放 segment inode、extent 与 page 的 FSP 端口。 */
    private final UndoSpaceAllocator undoAllocator;
    /** page3 恢复权威仓储；finalization 只允许 expected-owner CAS clear。 */
    private final RollbackSegmentHeaderRepository headerRepository;
    /** page3 提交成功后发布释放的运行期 slot 投影。 */
    private final RollbackSegmentSlotManager slotManager;
    /** INSERT/UPDATE cached segment 的运行期 LIFO 投影与 transition lease 来源。 */
    private final UndoSegmentCacheDirectory cacheDirectory;
    /** 仅测试使用的 commit 后 crash point；生产构造固定 no-op。 */
    private final UndoFinalizationFaultInjector faultInjector;

    /**
     * 构造生产 finalizer；fault injector 固定为 no-op。
     *
     * @param mtrManager       预检与最终写批次的 MTR 来源。
     * @param undoAccess       undo first page 读取入口。
     * @param undoAllocator    segment drop 端口。
     * @param headerRepository page3 owner CAS 仓储。
     * @param slotManager      运行期 slot 投影。
     */
    public UndoSegmentFinalizer(MiniTransactionManager mtrManager, UndoLogSegmentAccess undoAccess,
                                 UndoSpaceAllocator undoAllocator,
                                 RollbackSegmentHeaderRepository headerRepository,
                                 RollbackSegmentSlotManager slotManager,
                                 UndoSegmentCacheDirectory cacheDirectory) {
        this(mtrManager, undoAccess, undoAllocator, headerRepository, slotManager, cacheDirectory,
                UndoFinalizationFaultInjector.none());
    }

    /** 包内测试构造器，只允许在成功 commit 后注入模拟 crash。 */
    UndoSegmentFinalizer(MiniTransactionManager mtrManager, UndoLogSegmentAccess undoAccess,
                          UndoSpaceAllocator undoAllocator,
                          RollbackSegmentHeaderRepository headerRepository,
                          RollbackSegmentSlotManager slotManager,
                          UndoSegmentCacheDirectory cacheDirectory,
                          UndoFinalizationFaultInjector faultInjector) {
        if (mtrManager == null || undoAccess == null || undoAllocator == null || headerRepository == null
                || slotManager == null || cacheDirectory == null || faultInjector == null) {
            throw new DatabaseValidationException("undo finalizer collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.undoAccess = undoAccess;
        this.undoAllocator = undoAllocator;
        this.headerRepository = headerRepository;
        this.slotManager = slotManager;
        this.cacheDirectory = cacheDirectory;
        this.faultInjector = faultInjector;
    }

    /**
     * 原子提交事务拥有的独立 undo logs。INSERT-only 按 eligibility cache/drop；UPDATE-only 只写 COMMITTED
     * header；mixed 在同一 MTR 中先终结 INSERT owner，再写 UPDATE header，最后只追加一次事务 commit delta。
     */
    void finalizeCommit(Transaction transaction, UndoContext context, HistoryList.AppendLease historyLease) {
        if (transaction == null || context == null || transaction.state() != TransactionState.ACTIVE
                || transaction.transactionId().isNone() || transaction.transactionNo().isNone()) {
            throw new TransactionStateException("undo commit finalization requires ACTIVE write transaction and commitNo");
        }
        UndoLogBinding insert = context.binding(UndoLogKind.INSERT);
        UndoLogBinding update = context.binding(UndoLogKind.UPDATE);
        if (insert == null && update == null) {
            throw new TransactionStateException("undo commit finalization requires at least one undo log");
        }
        if ((update == null) != (historyLease == null)) {
            throw new TransactionStateException("UPDATE undo commit requires exactly one history append lease");
        }
        PreparedHistoryAppend historyAppend = update == null ? null
                : prepareHistoryAppend(update, transaction, historyLease);
        if (insert == null) {
            commitUpdateOnly(transaction, historyAppend, historyLease);
            return;
        }
        try (RollbackSegmentSlotManager.BatchFinalizationLease lease =
                     slotManager.beginBatchFinalization(List.of(insert))) {
            PreparedActive insertPrepared = prepareActive(insert, transaction.transactionId(), false);
            UndoSegmentDropPlan insertDrop = inspectDropPlan(insertPrepared.handle());
            UndoSegmentCacheDirectory.PushLease cachePush = reserveCachePush(insertPrepared, insertDrop);
            try (cachePush) {
                boolean cached = cachePush != null;
                MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                        RedoBudgetPurpose.UNDO_COMMIT, UndoRedoBudgetEstimator.commit(insertDrop, cached)));
                try {
                    if (historyLease != null) {
                        historyLease.physicalMutationStarted();
                    }
                    lease.physicalMutationStarted();
                    if (cachePush != null) {
                        cachePush.physicalMutationStarted();
                    } else {
                        undoAllocator.dropUndoSegment(mtr, insertPrepared.handle());
                    }
                    FinalizationDisposition insertDisposition = new FinalizationDisposition(
                            insertPrepared, insertDrop, cachePush);
                    publishFinalizationOwners(mtr, List.of(insertDisposition), update == null);
                    if (update != null) {
                        headerRepository.appendHistory(mtr, update.firstPageId().spaceId(),
                                historyAppend.base(), update.slotId(), update.firstPageId(),
                                transaction.transactionNo());
                        List<CachedUndoSegmentRef> resets = cachePush == null ? List.of()
                                : List.of(new CachedUndoSegmentRef(UndoLogKind.INSERT, insertPrepared.handle()));
                        undoAccess.appendHistoryNode(mtr,
                                historyAppend.oldTail().map(UndoHistoryNodeSnapshot::firstPageId),
                                update.firstPageId(), transaction.transactionId(), transaction.transactionNo(), resets);
                    }
                    TransactionStateRedoDeltas.appendCommit(mtr, transaction);
                    mtrManager.commit(mtr);
                } catch (RuntimeException error) {
                    rollbackActiveMtr(mtr, error);
                    throw new UndoFinalizationException("atomic undo commit finalization failed", error);
                }
                UndoLogBinding diagnostic = update == null ? insert : update;
                faultInjector.afterCommit(update == null ? UndoFinalizationKind.INSERT_COMMIT
                                : UndoFinalizationKind.UPDATE_COMMIT,
                        diagnostic.slotId(), diagnostic.firstPageId());
                try {
                    if (cachePush != null) {
                        cachePush.complete();
                    }
                    lease.complete();
                    if (historyLease != null) {
                        historyLease.complete();
                    }
                } catch (RuntimeException error) {
                    throw new UndoFinalizationException(
                            "undo commit finalization persisted but memory publication failed", error);
                }
            }
        }
    }

    private void commitUpdateOnly(Transaction transaction, PreparedHistoryAppend prepared,
                                  HistoryList.AppendLease historyLease) {
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                RedoBudgetPurpose.UNDO_COMMIT, UndoRedoBudgetEstimator.commit(null)));
        try {
            historyLease.physicalMutationStarted();
            UndoLogBinding update = prepared.update().binding();
            headerRepository.appendHistory(mtr, update.firstPageId().spaceId(), prepared.base(),
                    update.slotId(), update.firstPageId(), transaction.transactionNo());
            undoAccess.appendHistoryNode(mtr,
                    prepared.oldTail().map(UndoHistoryNodeSnapshot::firstPageId), update.firstPageId(),
                    transaction.transactionId(), transaction.transactionNo());
            TransactionStateRedoDeltas.appendCommit(mtr, transaction);
            mtrManager.commit(mtr);
        } catch (RuntimeException error) {
            rollbackActiveMtr(mtr, error);
            throw new UndoFinalizationException("UPDATE undo commit finalization failed", error);
        }
        UndoLogBinding update = prepared.update().binding();
        faultInjector.afterCommit(UndoFinalizationKind.UPDATE_COMMIT, update.slotId(), update.firstPageId());
        try {
            historyLease.complete();
        } catch (RuntimeException error) {
            throw new UndoFinalizationException(
                    "UPDATE undo commit persisted but history publication failed", error);
        }
    }

    /**
     * live full rollback：只允许回收 ACTIVE 且持久/内存 logical head 都为 EMPTY 的 segment。
     *
     * @param transaction 正处于 ROLLING_BACK 的 live 事务。
     * @param context     已发布 EMPTY logical head 的 undo context。
     */
    void finalizeLiveRollback(Transaction transaction, UndoContext context) {
        if (transaction == null || context == null) {
            throw new DatabaseValidationException("rollback finalization transaction/context must not be null");
        }
        if (transaction.state() != TransactionState.ROLLING_BACK
                || context.bindings().stream().anyMatch(binding -> !binding.logicalHead().isEmpty())) {
            throw new TransactionStateException("live rollback finalization requires ROLLING_BACK and all EMPTY heads");
        }
        finalizeActiveBatch(UndoFinalizationKind.LIVE_ROLLBACK, context.bindings(),
                transaction.transactionId(), transaction);
    }

    /** recovery 对同一 creator 的 INSERT/UPDATE slots 做一次原子 cache/drop owner 终结与 terminal delta。 */
    void finalizeRecoveredRollback(Collection<UndoLogBinding> bindings, TransactionId creatorTransactionId) {
        finalizeActiveBatch(UndoFinalizationKind.RECOVERY_ROLLBACK, bindings, creatorTransactionId, null);
    }

    private void finalizeActiveBatch(UndoFinalizationKind kind, Collection<UndoLogBinding> bindings,
                                     TransactionId creator, Transaction transaction) {
        if (bindings == null || bindings.isEmpty()) {
            throw new DatabaseValidationException("undo rollback batch must not be empty");
        }
        List<UndoLogBinding> ordered = bindings.stream()
                .sorted(Comparator.comparingInt((UndoLogBinding binding) -> binding.firstPageId().spaceId().value())
                        .thenComparingLong(binding -> binding.firstPageId().pageNo().value()))
                .toList();
        try (RollbackSegmentSlotManager.BatchFinalizationLease lease =
                     slotManager.beginBatchFinalization(ordered)) {
            List<PreparedActive> prepared = ordered.stream()
                    .map(binding -> prepareActive(binding, creator, true))
                    .toList();
            List<UndoSegmentDropPlan> plans = prepared.stream()
                    .map(item -> inspectDropPlan(item.handle()))
                    .toList();
            try (CachePushGroup cachePushes = reserveCachePushes(prepared, plans)) {
                List<FinalizationDisposition> dispositions = cachePushes.dispositions();
                List<UndoSegmentDropPlan> droppedPlans = dispositions.stream()
                        .filter(item -> item.cachePush() == null)
                        .map(FinalizationDisposition::dropPlan)
                        .toList();
                int cachedCount = dispositions.size() - droppedPlans.size();
                MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                        RedoBudgetPurpose.UNDO_FINALIZATION,
                        UndoRedoBudgetEstimator.finalization(droppedPlans, cachedCount, true)));
                try {
                    lease.physicalMutationStarted();
                    for (FinalizationDisposition disposition : dispositions) {
                        if (disposition.cachePush() == null) {
                            undoAllocator.dropUndoSegment(mtr, disposition.prepared().handle());
                        } else {
                            disposition.cachePush().physicalMutationStarted();
                        }
                    }
                    publishFinalizationOwners(mtr, dispositions);
                    if (kind == UndoFinalizationKind.LIVE_ROLLBACK) {
                        TransactionStateRedoDeltas.appendRollbackComplete(mtr, transaction);
                    } else {
                        TransactionStateRedoDeltas.appendRecoveredRollback(mtr, creator);
                    }
                    mtrManager.commit(mtr);
                } catch (RuntimeException error) {
                    rollbackActiveMtr(mtr, error);
                    throw new UndoFinalizationException("multi-segment undo rollback finalization failed", error);
                }
                UndoLogBinding diagnostic = ordered.getFirst();
                faultInjector.afterCommit(kind, diagnostic.slotId(), diagnostic.firstPageId());
                try {
                    cachePushes.complete();
                    lease.complete();
                } catch (RuntimeException error) {
                    throw new UndoFinalizationException(
                            "undo rollback finalization persisted but memory publication failed", error);
                }
            }
        }
    }

    private PreparedActive prepareActive(UndoLogBinding binding, TransactionId creator,
                                         boolean requireEmptyHead) {
        PageId memoryOwner = slotManager.undoFirstPageId(binding.slotId());
        if (!memoryOwner.equals(binding.firstPageId())) {
            throw new UndoLogFormatException("memory rseg slot owner mismatch: expected="
                    + binding.firstPageId() + ", current=" + memoryOwner);
        }
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            RollbackSegmentHeaderSnapshot snapshot = headerRepository.read(read,
                    binding.firstPageId().spaceId(), slotManager.rollbackSegmentId(), slotManager.slotCapacity(),
                    cacheDirectory.capacityPerKind());
            if (!binding.firstPageId().equals(snapshot.occupiedSlots().get(binding.slotId()))) {
                throw new UndoLogFormatException("persistent rseg slot owner mismatch for " + binding.kind());
            }
            UndoLogSegment segment = undoAccess.open(read, binding.firstPageId(), PageLatchMode.SHARED);
            if (!segment.creatorTransactionId().equals(creator) || segment.undoKind() != binding.kind()
                    || !segment.isActive()) {
                throw new UndoLogFormatException("active undo binding identity/state mismatch for " + binding.kind());
            }
            if (requireEmptyHead && (!segment.logicalHead().isEmpty() || !binding.logicalHead().isEmpty())) {
                throw new UndoLogFormatException("rollback finalization requires EMPTY " + binding.kind() + " head");
            }
            UndoSegmentHandle handle = segment.handle();
            mtrManager.commit(read);
            return new PreparedActive(binding, handle);
        } catch (RuntimeException error) {
            rollbackActiveMtr(read, error);
            throw error;
        }
    }

    /**
     * UPDATE commit 的全量只读预检。page3、new node、old tail 分属独立短 MTR，返回后只保留不可变快照；
     * history transition 已阻止其它 append/unlink 改变运行时链端点，但不持 Java lock 跨 IO。
     */
    private PreparedHistoryAppend prepareHistoryAppend(UndoLogBinding update, Transaction transaction,
                                                        HistoryList.AppendLease lease) {
        if (update == null || update.kind() != UndoLogKind.UPDATE || transaction == null || lease == null) {
            throw new DatabaseValidationException("history append preparation requires UPDATE binding/lease");
        }
        HistoryEntry expectedEntry = new HistoryEntry(transaction.transactionNo(), transaction.transactionId(),
                update.firstPageId().spaceId(), update.firstPageId(), update.slotId());
        if (!expectedEntry.equals(lease.entry())) {
            throw new TransactionStateException("history append lease identity differs from UPDATE binding");
        }
        PageId memoryOwner = slotManager.undoFirstPageId(update.slotId());
        if (!memoryOwner.equals(update.firstPageId())) {
            throw new UndoLogFormatException("memory UPDATE slot owner mismatch before history append");
        }

        RollbackSegmentHeaderSnapshot header;
        MiniTransaction page3Read = mtrManager.beginReadOnly();
        try {
            header = headerRepository.read(page3Read, update.firstPageId().spaceId(),
                    slotManager.rollbackSegmentId(), slotManager.slotCapacity(), cacheDirectory.capacityPerKind());
            mtrManager.commit(page3Read);
        } catch (RuntimeException error) {
            rollbackActiveMtr(page3Read, error);
            throw error;
        }
        if (!update.firstPageId().equals(header.occupiedSlots().get(update.slotId()))) {
            throw new UndoLogFormatException("persistent UPDATE slot owner mismatch before history append");
        }
        requireHistoryBaseMatchesLease(header.historyBase(), lease);

        UndoHistoryNodeSnapshot newNode = inspectHistoryNode(update.firstPageId());
        if (!newNode.isActive() || newNode.kind() != UndoLogKind.UPDATE
                || !newNode.creatorTransactionId().equals(transaction.transactionId())
                || !newNode.committedTransactionNo().isNone()
                || newNode.previousHistoryPageId().isPresent() || newNode.nextHistoryPageId().isPresent()) {
            throw new UndoLogFormatException("new history node must be an unlinked ACTIVE UPDATE undo first page");
        }
        Optional<UndoHistoryNodeSnapshot> oldTail = lease.expectedTail()
                .map(entry -> inspectHistoryNode(entry.undoFirstPageId()));
        if (oldTail.isPresent()) {
            UndoHistoryNodeSnapshot tail = oldTail.orElseThrow();
            HistoryEntry runtimeTail = lease.expectedTail().orElseThrow();
            if (!tail.isCommitted() || tail.kind() != UndoLogKind.UPDATE
                    || !tail.creatorTransactionId().equals(runtimeTail.creatorTrxId())
                    || !tail.committedTransactionNo().equals(runtimeTail.transactionNo())
                    || tail.nextHistoryPageId().isPresent()) {
                throw new UndoLogFormatException("persistent history tail differs from runtime projection");
            }
        }
        return new PreparedHistoryAppend(new PreparedActive(update, newNode.handle()),
                header.historyBase(), oldTail);
    }

    /** 对运行时冻结的 head/tail/count 与 page3 base 做精确交叉校验。 */
    private static void requireHistoryBaseMatchesLease(RollbackSegmentHistoryBase base,
                                                       HistoryList.TransitionLease lease) {
        Optional<PageId> expectedHead = lease.expectedHead().map(HistoryEntry::undoFirstPageId);
        Optional<PageId> expectedTail = lease.expectedTail().map(HistoryEntry::undoFirstPageId);
        if (base.length() != lease.expectedSize() || !base.headPageId().equals(expectedHead)
                || !base.tailPageId().equals(expectedTail)) {
            throw new UndoLogFormatException("runtime history projection differs from page3 base: base="
                    + base + ", runtimeHead=" + expectedHead + ", runtimeTail=" + expectedTail
                    + ", runtimeSize=" + lease.expectedSize());
        }
    }

    /** 一张 first page 一个只读 MTR，避免沿跨事务 history 链累计 fix/latch。 */
    private UndoHistoryNodeSnapshot inspectHistoryNode(PageId firstPageId) {
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            UndoHistoryNodeSnapshot snapshot = undoAccess.inspectHistoryNode(read, firstPageId);
            mtrManager.commit(read);
            return snapshot;
        } catch (RuntimeException error) {
            rollbackActiveMtr(read, error);
            throw error;
        }
    }

    /**
     * committed purge：只允许回收 header identity 与 history entry 完全一致的 COMMITTED segment。
     *
     * @param entry 当前 committed history 队首 identity。
     */
    void finalizePurgedHistory(HistoryEntry entry, HistoryList.HeadRemovalLease historyLease) {
        if (entry == null || historyLease == null || !entry.equals(historyLease.expected())) {
            throw new DatabaseValidationException("purge finalization history entry/lease mismatch");
        }
        PreparedPurge prepared = preparePurge(entry, historyLease);
        try (RollbackSegmentSlotManager.FinalizationLease slotLease =
                     slotManager.beginFinalization(entry.slotId(), entry.undoFirstPageId())) {
            UndoSegmentDropPlan dropPlan = inspectDropPlan(prepared.removed().handle());
            PreparedActive active = new PreparedActive(
                    new UndoLogBinding(UndoLogKind.UPDATE, entry.slotId(), entry.undoFirstPageId(),
                            prepared.removed().logicalHead()), prepared.removed().handle());
            UndoSegmentCacheDirectory.PushLease cachePush = reserveCachePush(active, dropPlan);
            try (cachePush) {
                boolean cached = cachePush != null;
                MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(
                        RedoBudgetPurpose.UNDO_FINALIZATION,
                        UndoRedoBudgetEstimator.finalization(cached ? List.of() : List.of(dropPlan),
                                cached ? 1 : 0, false)));
                try {
                    historyLease.physicalMutationStarted();
                    slotLease.physicalMutationStarted();
                    if (cachePush == null) {
                        undoAllocator.dropUndoSegment(mtr, prepared.removed().handle());
                    } else {
                        cachePush.physicalMutationStarted();
                    }
                    headerRepository.removeHistoryHead(mtr, entry.undoSpaceId(), prepared.base(),
                            entry.slotId(), entry.undoFirstPageId(),
                            prepared.newHead().map(UndoHistoryNodeSnapshot::firstPageId));
                    publishFinalizationOwners(mtr,
                            List.of(new FinalizationDisposition(active, dropPlan, cachePush)), false);
                    undoAccess.unlinkHistoryHead(mtr, prepared.removed(), prepared.newHead(), cached);
                    mtrManager.commit(mtr);
                } catch (RuntimeException error) {
                    rollbackActiveMtr(mtr, error);
                    throw new UndoFinalizationException(
                            "persistent history unlink failed after physical mutation began: " + entry, error);
                }

                faultInjector.afterCommit(UndoFinalizationKind.PURGE, entry.slotId(), entry.undoFirstPageId());
                try {
                    // 新 runtime head 暴露前先完成其旧 owner 的 slot/cache 投影发布。
                    if (cachePush != null) {
                        cachePush.complete();
                    }
                    slotLease.complete();
                    historyLease.complete();
                } catch (RuntimeException error) {
                    throw new UndoFinalizationException(
                            "persistent history unlink committed but memory publication failed: " + entry, error);
                }
            }
        }
    }

    /** purge unlink 前精确核对 page3 base、removed head 与可选 successor 的双向链接。 */
    private PreparedPurge preparePurge(HistoryEntry entry, HistoryList.HeadRemovalLease lease) {
        PageId memoryOwner = slotManager.undoFirstPageId(entry.slotId());
        if (!memoryOwner.equals(entry.undoFirstPageId())) {
            throw new UndoLogFormatException("memory rseg slot owner mismatch before purge unlink");
        }
        RollbackSegmentHeaderSnapshot header;
        MiniTransaction page3Read = mtrManager.beginReadOnly();
        try {
            header = headerRepository.read(page3Read, entry.undoSpaceId(), slotManager.rollbackSegmentId(),
                    slotManager.slotCapacity(), cacheDirectory.capacityPerKind());
            mtrManager.commit(page3Read);
        } catch (RuntimeException error) {
            rollbackActiveMtr(page3Read, error);
            throw error;
        }
        requireHistoryBaseMatchesLease(header.historyBase(), lease);
        if (!entry.undoFirstPageId().equals(header.occupiedSlots().get(entry.slotId()))
                || !header.historyBase().headPageId().equals(Optional.of(entry.undoFirstPageId()))) {
            throw new UndoLogFormatException("persistent purge head/slot owner differs from runtime entry");
        }
        UndoHistoryNodeSnapshot removed = inspectHistoryNode(entry.undoFirstPageId());
        if (!removed.isCommitted() || removed.kind() != UndoLogKind.UPDATE
                || !removed.creatorTransactionId().equals(entry.creatorTrxId())
                || !removed.committedTransactionNo().equals(entry.transactionNo())
                || removed.previousHistoryPageId().isPresent()) {
            throw new UndoLogFormatException("persistent purge head identity/state mismatch");
        }
        Optional<UndoHistoryNodeSnapshot> newHead = removed.nextHistoryPageId().map(this::inspectHistoryNode);
        if ((header.historyBase().length() == 1L) != newHead.isEmpty()) {
            throw new UndoLogFormatException("persistent purge head successor disagrees with history length");
        }
        if (newHead.isPresent()) {
            UndoHistoryNodeSnapshot next = newHead.orElseThrow();
            if (!next.isCommitted() || next.kind() != UndoLogKind.UPDATE
                    || !next.previousHistoryPageId().equals(Optional.of(removed.firstPageId()))) {
                throw new UndoLogFormatException("persistent history successor does not point back to head");
            }
        }
        return new PreparedPurge(header.historyBase(), removed, newHead);
    }

    /**
     * 在最终写批次前读取 inode 权威规模，用于把 redo admission 与实际 fragment/extent 数量绑定。
     * 该 MTR 与 page3/undo 预检严格分离，提交返回后只保留不可变值对象，避免 page2 latch 跨入 drop 写路径。
     */
    private UndoSegmentDropPlan inspectDropPlan(UndoSegmentHandle handle) {
        MiniTransaction planMtr = mtrManager.beginReadOnly();
        try {
            UndoSegmentDropPlan plan = undoAllocator.inspectDropPlan(planMtr, handle);
            mtrManager.commit(planMtr);
            return plan;
        } catch (RuntimeException error) {
            rollbackActiveMtr(planMtr, error);
            throw error;
        }
    }

    /**
     * 尝试为单 fragment 页 segment 取得 cache push lease。缓存容量满、drain 或同 kind transition 正忙时返回 null，
     * finalizer 沿用 drop；不等待 cache 短状态，避免终态 IO 被性能优化反向阻塞。
     */
    private UndoSegmentCacheDirectory.PushLease reserveCachePush(PreparedActive prepared,
                                                                  UndoSegmentDropPlan plan) {
        if (!isCacheEligible(prepared.handle(), plan)) {
            return null;
        }
        CachedUndoSegmentRef cached = new CachedUndoSegmentRef(prepared.binding().kind(), prepared.handle());
        return cacheDirectory.tryReservePush(cached).orElse(null);
    }

    private CachePushGroup reserveCachePushes(List<PreparedActive> prepared,
                                               List<UndoSegmentDropPlan> plans) {
        if (prepared.size() != plans.size()) {
            throw new DatabaseValidationException("undo finalization prepared/plan size mismatch");
        }
        List<FinalizationDisposition> dispositions = new ArrayList<>(prepared.size());
        List<UndoSegmentCacheDirectory.PushLease> acquired = new ArrayList<>();
        try {
            for (int i = 0; i < prepared.size(); i++) {
                UndoSegmentCacheDirectory.PushLease push = reserveCachePush(prepared.get(i), plans.get(i));
                if (push != null) {
                    acquired.add(push);
                }
                dispositions.add(new FinalizationDisposition(prepared.get(i), plans.get(i), push));
            }
            return new CachePushGroup(dispositions, acquired);
        } catch (RuntimeException failure) {
            for (int i = acquired.size() - 1; i >= 0; i--) {
                try {
                    acquired.get(i).close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    private static boolean isCacheEligible(UndoSegmentHandle handle, UndoSegmentDropPlan plan) {
        return handle.firstPageId().equals(handle.lastPageId())
                && plan.usedPageCount() == 1L
                && plan.fragmentPageCount() == 1L
                && plan.extentCount() == 0L;
    }

    /**
     * 发布 finalization 的持久 owner：drop 目标清 active slot，cache 目标执行 active→cache push；随后才按页号升序
     * 重置 cached first page。调用方已经先完成所有 FSP drop，因此整体页序为 page0/page2→page3→普通 undo 页。
     */
    private void publishFinalizationOwners(MiniTransaction mtr,
                                            List<FinalizationDisposition> dispositions) {
        publishFinalizationOwners(mtr, dispositions, true);
    }

    /**
     * 发布 page3 owner，并按调用方选择是否在本方法内重置 cached first page。mixed UPDATE commit 把 reset 合并到
     * history first-page batch，确保所有普通 undo 页严格按 pageNo 获取。
     */
    private void publishFinalizationOwners(MiniTransaction mtr,
                                            List<FinalizationDisposition> dispositions,
                                            boolean resetCachedPages) {
        List<RollbackSegmentHeaderRepository.CachePush> pushes = dispositions.stream()
                .filter(item -> item.cachePush() != null)
                .map(item -> new RollbackSegmentHeaderRepository.CachePush(
                        item.prepared().binding().slotId(), item.prepared().binding().firstPageId(),
                        item.prepared().binding().kind(), item.cachePush().expectedCount()))
                .toList();
        if (!pushes.isEmpty()) {
            headerRepository.moveActiveSlotsToCache(mtr,
                    dispositions.getFirst().prepared().binding().firstPageId().spaceId(), pushes);
        }
        for (FinalizationDisposition item : dispositions.stream()
                .filter(candidate -> candidate.cachePush() == null)
                .sorted(Comparator.comparingInt(candidate -> candidate.prepared().binding().slotId().value()))
                .toList()) {
            UndoLogBinding binding = item.prepared().binding();
            headerRepository.clearSlot(mtr, binding.firstPageId().spaceId(),
                    binding.slotId(), binding.firstPageId());
        }
        if (!resetCachedPages) {
            return;
        }
        for (FinalizationDisposition item : dispositions.stream()
                .filter(candidate -> candidate.cachePush() != null)
                .sorted(Comparator
                        .comparingInt((FinalizationDisposition candidate) ->
                                candidate.prepared().binding().firstPageId().spaceId().value())
                        .thenComparingLong(candidate ->
                                candidate.prepared().binding().firstPageId().pageNo().value()))
                .toList()) {
            undoAccess.open(mtr, item.prepared().binding().firstPageId(), PageLatchMode.EXCLUSIVE)
                    .resetForCache();
        }
    }

    /** 只在 MTR 仍 ACTIVE 时释放 memo；COMMITTING 失败保持原始结果不确定原因。 */
    private void rollbackActiveMtr(MiniTransaction mtr, RuntimeException original) {
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            return;
        }
        try {
            mtrManager.rollbackUncommitted(mtr);
        } catch (RuntimeException rollbackError) {
            original.addSuppressed(rollbackError);
        }
    }

    private record PreparedActive(UndoLogBinding binding, UndoSegmentHandle handle) {
    }

    private record PreparedHistoryAppend(PreparedActive update, RollbackSegmentHistoryBase base,
                                         Optional<UndoHistoryNodeSnapshot> oldTail) {
        private PreparedHistoryAppend {
            if (update == null || base == null || oldTail == null) {
                throw new DatabaseValidationException("prepared history append fields must not be null");
            }
        }
    }

    private record PreparedPurge(RollbackSegmentHistoryBase base, UndoHistoryNodeSnapshot removed,
                                 Optional<UndoHistoryNodeSnapshot> newHead) {
        private PreparedPurge {
            if (base == null || removed == null || newHead == null) {
                throw new DatabaseValidationException("prepared purge fields must not be null");
            }
        }
    }

    private record FinalizationDisposition(PreparedActive prepared, UndoSegmentDropPlan dropPlan,
                                           UndoSegmentCacheDirectory.PushLease cachePush) {
        private FinalizationDisposition {
            if (prepared == null || dropPlan == null) {
                throw new DatabaseValidationException("undo finalization disposition fields must not be null");
            }
        }
    }

    /** 动态数量 cache push lease 的 RAII 组合；commit 后统一发布，异常时按各 lease 的物理边界回退或保留 fence。 */
    private static final class CachePushGroup implements AutoCloseable {
        private final List<FinalizationDisposition> dispositions;
        private final List<UndoSegmentCacheDirectory.PushLease> leases;

        private CachePushGroup(List<FinalizationDisposition> dispositions,
                               List<UndoSegmentCacheDirectory.PushLease> leases) {
            this.dispositions = List.copyOf(dispositions);
            this.leases = List.copyOf(leases);
        }

        private List<FinalizationDisposition> dispositions() {
            return dispositions;
        }

        private void complete() {
            for (UndoSegmentCacheDirectory.PushLease lease : leases) {
                lease.complete();
            }
        }

        @Override
        public void close() {
            RuntimeException failure = null;
            for (int i = leases.size() - 1; i >= 0; i--) {
                try {
                    leases.get(i).close();
                } catch (RuntimeException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
