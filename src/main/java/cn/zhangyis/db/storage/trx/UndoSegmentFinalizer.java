package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

/**
 * undo segment 终态协调器。它把 FSP segment drop 与 page3 slot clear 放入同一 MTR/redo batch，并在 commit
 * 后才发布内存 slot 释放，闭合 INSERT commit、live/recovery rollback 与 committed purge 的 crash 边界。
 *
 * <p><b>锁序</b>：预检 MTR 先读 page3、再读普通 undo first page；返回前释放二者。finalization MTR 先由
 * {@link UndoSpaceAllocator#dropUndoSegment} 修改 FSP page0/page2，再获取 page3 X latch 做 owner CAS。当前 XDES
 * 内嵌 page0，故不需要逆序例外；任何时刻都不在内存 slot/history 锁内等待 page latch 或 IO。
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
                                RollbackSegmentSlotManager slotManager) {
        this(mtrManager, undoAccess, undoAllocator, headerRepository, slotManager,
                UndoFinalizationFaultInjector.none());
    }

    /** 包内测试构造器，只允许在成功 commit 后注入模拟 crash。 */
    UndoSegmentFinalizer(MiniTransactionManager mtrManager, UndoLogSegmentAccess undoAccess,
                         UndoSpaceAllocator undoAllocator,
                         RollbackSegmentHeaderRepository headerRepository,
                         RollbackSegmentSlotManager slotManager,
                         UndoFinalizationFaultInjector faultInjector) {
        if (mtrManager == null || undoAccess == null || undoAllocator == null || headerRepository == null
                || slotManager == null || faultInjector == null) {
            throw new DatabaseValidationException("undo finalizer collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.undoAccess = undoAccess;
        this.undoAllocator = undoAllocator;
        this.headerRepository = headerRepository;
        this.slotManager = slotManager;
        this.faultInjector = faultInjector;
    }

    /**
     * 纯 INSERT commit：在同一 batch 完成 drop+clear，并追加 recovery table 消费的 commit 终态/高水位证据。
     *
     * @param transaction 仍为 ACTIVE、已预留提交号的写事务。
     * @param context     该事务的 insert-only undo identity。
     */
    void finalizeInsertCommit(Transaction transaction, UndoContext context) {
        if (transaction == null || context == null) {
            throw new DatabaseValidationException("insert finalization transaction/context must not be null");
        }
        if (transaction.state() != TransactionState.ACTIVE || transaction.transactionId().isNone()
                || transaction.transactionNo().isNone() || context.hasUpdateUndo()) {
            throw new TransactionStateException("insert finalization requires ACTIVE write transaction, assigned "
                    + "transactionNo and insert-only undo");
        }
        FinalizationIdentity identity = new FinalizationIdentity(context.slotId(), context.undoFirstPageId(),
                transaction.transactionId(), TransactionNo.NONE);
        finalizeSegment(UndoFinalizationKind.INSERT_COMMIT, identity, transaction);
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
        if (transaction.state() != TransactionState.ROLLING_BACK || !context.logicalHead().isEmpty()) {
            throw new TransactionStateException("live rollback finalization requires ROLLING_BACK and EMPTY head");
        }
        FinalizationIdentity identity = new FinalizationIdentity(context.slotId(), context.undoFirstPageId(),
                transaction.transactionId(), TransactionNo.NONE);
        finalizeSegment(UndoFinalizationKind.LIVE_ROLLBACK, identity, transaction);
    }

    /**
     * recovery rollback：无 live Transaction，但必须匹配恢复扫描 identity 且 logical head 已为 EMPTY。
     *
     * @param slotId               page3 扫描得到的 slot。
     * @param firstPageId          slot 指向的 undo first page。
     * @param creatorTransactionId first-page header 中已核对的 creator。
     */
    void finalizeRecoveredRollback(UndoSlotId slotId, PageId firstPageId, TransactionId creatorTransactionId) {
        FinalizationIdentity identity = new FinalizationIdentity(
                slotId, firstPageId, creatorTransactionId, TransactionNo.NONE);
        finalizeSegment(UndoFinalizationKind.RECOVERY_ROLLBACK, identity, null);
    }

    /**
     * committed purge：只允许回收 header identity 与 history entry 完全一致的 COMMITTED segment。
     *
     * @param entry 当前 committed history 队首 identity。
     */
    void finalizePurgedHistory(HistoryEntry entry) {
        if (entry == null) {
            throw new DatabaseValidationException("purge finalization history entry must not be null");
        }
        FinalizationIdentity identity = new FinalizationIdentity(entry.slotId(), entry.undoFirstPageId(),
                entry.creatorTrxId(), entry.transactionNo());
        finalizeSegment(UndoFinalizationKind.PURGE, identity, null);
    }

    /**
     * 先以非阻塞 lease 独占运行期 owner，再执行短读预检、原子 drop+clear batch、crash hook 与内存发布。
     * lease 不持 slot lock 跨越 IO；它只把重复终态命令挡在任何 page/FSP 访问之前。
     */
    private void finalizeSegment(UndoFinalizationKind kind, FinalizationIdentity identity, Transaction transaction) {
        try (RollbackSegmentSlotManager.FinalizationLease lease =
                     slotManager.beginFinalization(identity.slotId(), identity.firstPageId())) {
            PreparedFinalization prepared = prepare(kind, identity);
            MiniTransaction finalizationMtr = mtrManager.begin(
                    mtrManager.budgetFor(RedoBudgetPurpose.UNDO_FINALIZATION));
            try {
                // 从首个 FSP 写开始 MTR 无 content undo；此后任何异常都让 lease 保持 FINALIZING，禁止同进程复用。
                lease.physicalMutationStarted();
                // dropSegment 固定先触碰 FSP page0/page2；page3 clear 随后获取更高页号，遵守全局页序。
                undoAllocator.dropUndoSegment(finalizationMtr, prepared.handle());
                headerRepository.clearSlot(finalizationMtr, identity.firstPageId().spaceId(),
                        identity.slotId(), identity.firstPageId());
                if (kind == UndoFinalizationKind.INSERT_COMMIT) {
                    TransactionStateRedoDeltas.appendCommit(finalizationMtr, transaction);
                } else if (kind == UndoFinalizationKind.LIVE_ROLLBACK) {
                    TransactionStateRedoDeltas.appendRollbackComplete(finalizationMtr, transaction);
                } else if (kind == UndoFinalizationKind.RECOVERY_ROLLBACK) {
                    TransactionStateRedoDeltas.appendRecoveredRollback(
                            finalizationMtr, identity.creatorTransactionId());
                }
                mtrManager.commit(finalizationMtr);
            } catch (RuntimeException error) {
                rollbackActiveMtr(finalizationMtr, error);
                throw new UndoFinalizationException(
                        "undo segment finalization failed after physical mutation began: kind="
                                + kind + ", slot=" + identity.slotId().value()
                                + ", firstPage=" + identity.firstPageId(), error);
            }

            // crash hook 位于 commit 与内存发布之间；若注入异常，lease close 保持 FINALIZING，模拟进程内 fail-stop。
            faultInjector.afterCommit(kind, identity.slotId(), identity.firstPageId());
            try {
                lease.complete();
            } catch (RuntimeException error) {
                throw new UndoFinalizationException(
                        "undo finalization committed but memory slot publication failed: kind="
                                + kind + ", slot=" + identity.slotId().value()
                                + ", firstPage=" + identity.firstPageId(), error);
            }
        }
    }

    /**
     * 所有可预测校验均在 drop 前完成。page3 S latch 先于 undo first-page S latch 获取；提交返回后只保留 handle 值对象。
     */
    private PreparedFinalization prepare(UndoFinalizationKind kind, FinalizationIdentity identity) {
        PageId memoryOwner = slotManager.insertUndoFirstPageId(identity.slotId());
        if (!memoryOwner.equals(identity.firstPageId())) {
            throw new UndoLogFormatException("memory rseg slot owner mismatch: expected=" + identity.firstPageId()
                    + ", current=" + memoryOwner);
        }

        MiniTransaction readMtr = mtrManager.beginReadOnly();
        try {
            RollbackSegmentHeaderSnapshot snapshot = headerRepository.read(readMtr,
                    identity.firstPageId().spaceId(), slotManager.rollbackSegmentId(), slotManager.slotCapacity());
            PageId persistentOwner = snapshot.occupiedSlots().get(identity.slotId());
            if (!identity.firstPageId().equals(persistentOwner)) {
                throw new UndoLogFormatException("persistent rseg slot owner mismatch: expected="
                        + identity.firstPageId() + ", current=" + persistentOwner);
            }
            UndoLogSegment segment = undoAccess.open(readMtr, identity.firstPageId(), PageLatchMode.SHARED);
            validateSegment(kind, identity, segment);
            UndoSegmentHandle handle = segment.handle();
            mtrManager.commit(readMtr);
            return new PreparedFinalization(handle);
        } catch (RuntimeException error) {
            rollbackActiveMtr(readMtr, error);
            throw error;
        }
    }

    /** 根据终态原因验证 first-page 权威状态，防止提前 drop 仍服务 rollback/MVCC/purge 的 undo。 */
    private void validateSegment(UndoFinalizationKind kind, FinalizationIdentity identity, UndoLogSegment segment) {
        if (!segment.creatorTransactionId().equals(identity.creatorTransactionId())) {
            throw new UndoLogFormatException("undo finalization creator mismatch: expected="
                    + identity.creatorTransactionId().value() + ", current="
                    + segment.creatorTransactionId().value());
        }
        switch (kind) {
            case INSERT_COMMIT -> requireActive(segment, kind);
            case LIVE_ROLLBACK, RECOVERY_ROLLBACK -> {
                requireActive(segment, kind);
                if (!segment.logicalHead().isEmpty()) {
                    throw new UndoLogFormatException(kind + " finalization requires EMPTY logical head: "
                            + segment.logicalHead());
                }
            }
            case PURGE -> {
                if (!segment.isCommitted()) {
                    throw new UndoLogFormatException("purge finalization requires COMMITTED undo state: "
                            + segment.state());
                }
                if (!segment.committedTransactionNo().equals(identity.commitNo())) {
                    throw new UndoLogFormatException("purge finalization commitNo mismatch: expected="
                            + identity.commitNo().value() + ", current="
                            + segment.committedTransactionNo().value());
                }
            }
        }
    }

    private void requireActive(UndoLogSegment segment, UndoFinalizationKind kind) {
        if (!segment.isActive()) {
            throw new UndoLogFormatException(kind + " finalization requires ACTIVE undo state: " + segment.state());
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

    /** 一条终态命令的权威 identity；commitNo 仅 PURGE 使用，其余为 NONE。 */
    private record FinalizationIdentity(UndoSlotId slotId, PageId firstPageId,
                                        TransactionId creatorTransactionId, TransactionNo commitNo) {
        private FinalizationIdentity {
            if (slotId == null || firstPageId == null || creatorTransactionId == null || commitNo == null) {
                throw new DatabaseValidationException("undo finalization identity fields must not be null");
            }
            if (creatorTransactionId.isNone()) {
                throw new DatabaseValidationException("undo finalization creator transaction must not be NONE");
            }
        }
    }

    /** drop 所需 handle 的短读结果；不持有 page guard 或 MTR。 */
    private record PreparedFinalization(UndoSegmentHandle handle) {
    }
}
