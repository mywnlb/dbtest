package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;

/**
 * 事务状态 logical redo 收集小工具。它位于 trx 包，负责把运行时 {@link TransactionState}
 * 映射成 redo 包的稳定磁盘枚举；redo 包不反向依赖事务状态机。
 */
final class TransactionStateRedoDeltas {

    private TransactionStateRedoDeltas() {
    }

    /**
     * 在 commit MTR 中追加提交状态 redo。该 record 不修改物理页；undo first page 的 COMMITTED marker
     * 仍是当前恢复判定提交/未提交的权威状态，本 record 只补充 redo 顺序中的事务状态诊断边界。
     *
     * @param mtr 当前 commit MTR。
     * @param txn 正在提交或已预留提交号的事务。
     */
    static void appendCommit(MiniTransaction mtr, Transaction txn) {
        append(mtr, txn, toRedoState(txn.state()), TransactionStateDeltaState.COMMITTED,
                TransactionStateDeltaReason.COMMIT);
    }

    /**
     * 在 finishRollback 前追加回滚完成 redo。调用点必须已经完整走完 undo 链并释放 slot；
     * 恢复期不能仅凭该 record 跳过 undo/rseg 状态检查。
     *
     * @param mtr 当前短 MTR。
     * @param txn 正处于 ROLLING_BACK 的事务。
     */
    static void appendRollbackComplete(MiniTransaction mtr, Transaction txn) {
        append(mtr, txn, toRedoState(txn.state()), TransactionStateDeltaState.ROLLED_BACK,
                TransactionStateDeltaReason.ROLLBACK);
    }

    private static void append(MiniTransaction mtr, Transaction txn, TransactionStateDeltaState fromState,
                               TransactionStateDeltaState toState, TransactionStateDeltaReason reason) {
        if (mtr == null || txn == null) {
            throw new DatabaseValidationException("transaction state redo mtr/txn must not be null");
        }
        mtr.appendLogicalRedo(new TransactionStateDeltaRecord(
                        txn.transactionId(), fromState, toState, txn.transactionNo(), reason),
                MtrRedoCategory.TRX_STATE, "append transaction state logical redo");
    }

    private static TransactionStateDeltaState toRedoState(TransactionState state) {
        if (state == null) {
            throw new DatabaseValidationException("transaction state must not be null");
        }
        return switch (state) {
            case ACTIVE -> TransactionStateDeltaState.ACTIVE;
            case COMMITTING -> TransactionStateDeltaState.COMMITTING;
            case COMMITTED -> TransactionStateDeltaState.COMMITTED;
            case ROLLING_BACK -> TransactionStateDeltaState.ROLLING_BACK;
            case ROLLED_BACK -> TransactionStateDeltaState.ROLLED_BACK;
        };
    }
}
