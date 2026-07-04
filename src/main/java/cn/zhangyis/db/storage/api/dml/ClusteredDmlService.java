package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadMode;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadService;
import cn.zhangyis.db.storage.btree.BTreeDeleteMarkResult;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.BTreeUpdateResult;
import cn.zhangyis.db.storage.btree.BTreeUniqueCheckResult;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.recovery.RecoveryState;
import cn.zhangyis.db.storage.recovery.RecoveryTrafficGate;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.trx.RollbackService;
import cn.zhangyis.db.storage.trx.RollbackSummary;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.UndoLogManager;
import cn.zhangyis.db.storage.trx.lock.LockManager;

import java.util.Optional;

/**
 * 单表/单聚簇索引 DML facade。它是 SQL executor 未来进入 storage 层的稳定边界：调用方显式传入事务、
 * 聚簇索引快照和已归一化记录，本服务只编排 transaction、undo、current-read、B+Tree、redo durability
 * 与 row-lock release，不解析 SQL、不访问 BufferFrame/RecordCursor/裸文件。
 */
public final class ClusteredDmlService {

    /** 授锁后重定位最多重试次数；current-read 下页 latch 已释放，重试用于处理结构变化或锁落点变化。 */
    private static final int DEFAULT_RELOCATION_RETRIES = 3;

    /** 事务状态机与写 id / transaction no 分配来源。 */
    private final TransactionManager transactionManager;
    /** undo 写入与 commit history/reclaim 编排入口。 */
    private final UndoLogManager undoLogManager;
    /** 业务写 MTR 工厂；每条聚簇记录修改使用一个短 MTR。 */
    private final MiniTransactionManager mtrManager;
    /** 聚簇 B+Tree 写入原语；只接收值对象，不依赖事务/SQL。 */
    private final SplitCapableBTreeIndexService btree;
    /** current-read 协调器；所有可能等待 row lock 的路径必须经它释放 page latch 后等待。 */
    private final BTreeCurrentReadService currentRead;
    /** full transaction rollback 执行器；按 undo 链反向恢复聚簇记录。 */
    private final RollbackService rollbackService;
    /** 事务 row-lock 真相来源；commit/rollback 收尾释放事务持锁。 */
    private final LockManager lockManager;
    /** redo 持久化边界；commit durability policy 等待该 manager 的 write/flush 状态。 */
    private final RedoLogManager redo;
    /** 启动恢复流量门控；非 OPEN 时拒绝普通 DML 进入。 */
    private final RecoveryTrafficGate recoveryGate;

    /**
     * 构造 DML facade。所有 collaborator 都来自 {@code StorageEngine} 组合根，保证 DML 与测试/恢复/后台 purge
     * 使用同一套事务、undo、锁和 redo 状态，而不是另建旁路实例。
     */
    public ClusteredDmlService(TransactionManager transactionManager, UndoLogManager undoLogManager,
                               MiniTransactionManager mtrManager, SplitCapableBTreeIndexService btree,
                               BTreeCurrentReadService currentRead, RollbackService rollbackService,
                               LockManager lockManager, RedoLogManager redo, RecoveryTrafficGate recoveryGate) {
        if (transactionManager == null || undoLogManager == null || mtrManager == null || btree == null
                || currentRead == null || rollbackService == null || lockManager == null
                || redo == null || recoveryGate == null) {
            throw new DatabaseValidationException("clustered DML service collaborators must not be null");
        }
        this.transactionManager = transactionManager;
        this.undoLogManager = undoLogManager;
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.currentRead = currentRead;
        this.rollbackService = rollbackService;
        this.lockManager = lockManager;
        this.redo = redo;
        this.recoveryGate = recoveryGate;
    }

    /**
     * 执行单聚簇索引 INSERT。数据流为：分配 write id -> unique current-read 取得 duplicate/gap 锁 ->
     * 在一个业务 MTR 内写 INSERT undo 并插入聚簇记录 -> commit MTR 返回 end LSN。若 unique check 发现物理
     * 同 key 记录，抛出重复键领域异常，调用方仍需按事务语义决定 commit/rollback。
     */
    public DmlWriteResult insert(ClusteredInsertCommand command) {
        if (command == null) {
            throw new DatabaseValidationException("clustered insert command must not be null");
        }
        requireOpenForDml();
        Transaction txn = command.transaction();
        TransactionId txnId = transactionManager.assignWriteId(txn);
        BTreeCurrentReadRequest request = currentReadRequest(txn, txnId, command.lockWaitTimeout());
        BTreeUniqueCheckResult unique = currentRead.checkUniqueForInsert(command.index(), command.key(), request);
        if (unique.duplicate()) {
            throw new DmlDuplicateKeyException("duplicate clustered key for index " + command.index().indexId());
        }

        MiniTransaction mtr = mtrManager.begin();
        try {
            RollPointer rollPointer = undoLogManager.beforeInsert(txn, mtr, command.tableId(),
                    command.index().indexId(), command.key().values(), command.index().keyDef(),
                    command.index().schema());
            btree.insertClustered(mtr, command.index(), command.record(), txnId, rollPointer);
            Lsn endLsn = mtrManager.commit(mtr);
            return new DmlWriteResult(true, 1, endLsn, txnId);
        } catch (RuntimeException e) {
            rollbackActiveMtr(mtr, e);
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("clustered insert failed", e);
        }
    }

    /**
     * 执行单聚簇索引 UPDATE。数据流为：分配 write id -> point current-read FOR UPDATE 授锁并重定位 ->
     * miss 返回 affectedRows=0；命中则读取旧隐藏列，在一个业务 MTR 内写 UPDATE undo、盖新 roll pointer、
     * 替换聚簇记录。当前只支持不改变聚簇 key 的页内/重定位更新。
     */
    public DmlWriteResult update(ClusteredUpdateCommand command) {
        if (command == null) {
            throw new DatabaseValidationException("clustered update command must not be null");
        }
        requireOpenForDml();
        Transaction txn = command.transaction();
        TransactionId txnId = transactionManager.assignWriteId(txn);
        BTreeCurrentReadRequest request = currentReadRequest(txn, txnId, command.lockWaitTimeout());
        Optional<BTreeLookupResult> locked = currentRead.lockPoint(command.index(), command.key(),
                request, BTreeCurrentReadMode.FOR_UPDATE);
        if (locked.isEmpty()) {
            return new DmlWriteResult(false, 0, redo.currentLsn(), txnId);
        }
        BTreeLookupResult old = locked.orElseThrow();
        HiddenColumns oldHidden = requireHiddenColumns(old.record(), "update");

        MiniTransaction mtr = mtrManager.begin();
        try {
            RollPointer rollPointer = undoLogManager.beforeUpdate(txn, mtr, command.tableId(),
                    command.index().indexId(), command.key().values(), old.record().columnValues(), oldHidden,
                    command.index().keyDef(), command.index().schema());
            LogicalRecord stamped = stampedRecord(command.newRecord(), false,
                    new HiddenColumns(txnId, rollPointer));
            BTreeUpdateResult replaced = btree.replaceClustered(mtr, command.index(), command.key(),
                    stamped, oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            Lsn endLsn = mtrManager.commit(mtr);
            return new DmlWriteResult(replaced.replaced(), replaced.replaced() ? 1 : 0, endLsn, txnId);
        } catch (RuntimeException e) {
            rollbackActiveMtr(mtr, e);
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("clustered update failed", e);
        }
    }

    /**
     * 执行单聚簇索引 DELETE。数据流为：分配 write id -> point current-read FOR UPDATE 授锁并重定位 ->
     * miss 返回 affectedRows=0；命中则在一个业务 MTR 内写 DELETE_MARK undo，并只翻聚簇记录 delete-mark，
     * 物理删除留给 purge。
     */
    public DmlWriteResult delete(ClusteredDeleteCommand command) {
        if (command == null) {
            throw new DatabaseValidationException("clustered delete command must not be null");
        }
        requireOpenForDml();
        Transaction txn = command.transaction();
        TransactionId txnId = transactionManager.assignWriteId(txn);
        BTreeCurrentReadRequest request = currentReadRequest(txn, txnId, command.lockWaitTimeout());
        Optional<BTreeLookupResult> locked = currentRead.lockPoint(command.index(), command.key(),
                request, BTreeCurrentReadMode.FOR_UPDATE);
        if (locked.isEmpty()) {
            return new DmlWriteResult(false, 0, redo.currentLsn(), txnId);
        }
        BTreeLookupResult old = locked.orElseThrow();
        HiddenColumns oldHidden = requireHiddenColumns(old.record(), "delete");

        MiniTransaction mtr = mtrManager.begin();
        try {
            RollPointer rollPointer = undoLogManager.beforeDelete(txn, mtr, command.tableId(),
                    command.index().indexId(), command.key().values(), old.record().columnValues(), oldHidden,
                    command.index().keyDef(), command.index().schema());
            BTreeDeleteMarkResult marked = btree.setClusteredDeleteMark(mtr, command.index(), command.key(),
                    true, new HiddenColumns(txnId, rollPointer), oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            Lsn endLsn = mtrManager.commit(mtr);
            return new DmlWriteResult(marked.changed(), marked.changed() ? 1 : 0, endLsn, txnId);
        } catch (RuntimeException e) {
            rollbackActiveMtr(mtr, e);
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("clustered delete failed", e);
        }
    }

    /**
     * 提交数据库事务。顺序为 prepareCommit(只预留提交号) -> undo onCommit 持久化提交状态 ->
     * transaction commit(移出 active/进入 COMMITTED) -> redo durability policy 等待 -> row-lock release。
     * 这样 onCommit 失败时事务仍保持 ACTIVE 且 row locks 不释放，避免恢复期把“已对外提交但 undo 仍 ACTIVE”
     * 的事务误回滚。
     */
    public DmlCommitResult commit(DmlCommitCommand command) {
        if (command == null) {
            throw new DatabaseValidationException("DML commit command must not be null");
        }
        requireOpenForDml();
        Transaction txn = command.transaction();
        TransactionId txnId = txn.transactionId();
        boolean transactionCommitted = false;
        try {
            transactionManager.prepareCommit(txn);
            undoLogManager.onCommit(txn);
            transactionManager.commit(txn);
            transactionCommitted = true;
            Lsn commitLsn = redo.currentLsn();
            boolean durable = command.durabilityPolicy()
                    .awaitCommitDurable(redo, commitLsn, command.durabilityTimeout());
            if (!durable) {
                throw new DmlOperationException("commit redo did not reach durability policy before timeout");
            }
            int released = releaseLocks(txnId);
            return new DmlCommitResult(txn.transactionNo(), true, released);
        } catch (RuntimeException e) {
            if (transactionCommitted) {
                releaseLocksOnFailure(txnId, e);
            }
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("DML commit failed", e);
        }
    }

    /**
     * 回滚数据库事务。调用 rollback service 沿 undo 链反向应用记录级撤销，然后释放事务 row locks；
     * 若回滚过程中出现异常，facade 仍尝试释放本事务已持有的 row locks，并把释放失败作为 suppressed cause 保留。
     */
    public DmlRollbackResult rollback(DmlRollbackCommand command) {
        if (command == null) {
            throw new DatabaseValidationException("DML rollback command must not be null");
        }
        requireOpenForDml();
        Transaction txn = command.transaction();
        TransactionId txnId = txn.transactionId();
        try {
            RollbackSummary summary = rollbackService.rollback(txn, command.clusteredIndex());
            int released = releaseLocks(txnId);
            return new DmlRollbackResult(summary, released);
        } catch (RuntimeException e) {
            releaseLocksOnFailure(txnId, e);
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("DML rollback failed", e);
        }
    }

    private void requireOpenForDml() {
        RecoveryState state = recoveryGate.state();
        if (state != RecoveryState.OPEN) {
            throw new DmlOperationException("DML rejected while recovery gate is " + state);
        }
    }

    private static BTreeCurrentReadRequest currentReadRequest(Transaction txn, TransactionId txnId,
                                                             java.time.Duration lockWaitTimeout) {
        return new BTreeCurrentReadRequest(txnId, txn.isolationLevel(), lockWaitTimeout,
                DEFAULT_RELOCATION_RETRIES);
    }

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

    private static HiddenColumns requireHiddenColumns(LogicalRecord record, String operation) {
        HiddenColumns hidden = record.hiddenColumns();
        if (hidden == null) {
            throw new DmlOperationException("clustered " + operation + " requires hidden columns on current row");
        }
        return hidden;
    }

    private static LogicalRecord stampedRecord(LogicalRecord source, boolean deleted, HiddenColumns hiddenColumns) {
        return new LogicalRecord(source.schemaVersion(), source.columnValues(), deleted, source.recordType(),
                hiddenColumns);
    }

    private int releaseLocks(TransactionId txnId) {
        return txnId.isNone() ? 0 : lockManager.releaseAll(txnId);
    }

    private void releaseLocksOnFailure(TransactionId txnId, RuntimeException original) {
        if (txnId.isNone()) {
            return;
        }
        try {
            lockManager.releaseAll(txnId);
        } catch (RuntimeException releaseError) {
            original.addSuppressed(releaseError);
        }
    }
}
