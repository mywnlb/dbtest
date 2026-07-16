package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * 事务状态 logical redo 收集小工具。它位于 trx 包，负责把运行时 {@link TransactionState}
 * 映射成 redo 包的稳定磁盘枚举；redo 包不反向依赖事务状态机。
 */
final class TransactionStateRedoDeltas {

    private TransactionStateRedoDeltas() {
    }

    /**
     * 在 commit MTR 中追加提交状态 redo。该 record 不修改物理页，但会被正式 recovery table 用作终态与
     * transaction id/no 高水位证据；存在 undo slot 时仍必须与 page3/first-page 状态交叉校验，不能单独跳过恢复。
     *
     * @param mtr 当前 commit MTR。
     * @param txn 正在提交或已预留提交号的事务。
     */
    static void appendCommit(MiniTransaction mtr, Transaction txn) {
        append(mtr, txn, toRedoState(txn.state()), TransactionStateDeltaState.COMMITTED,
                txn.transactionNo(), TransactionStateDeltaReason.COMMIT);
    }

    /**
     * 在 finishRollback 前追加回滚完成 redo。调用点必须已经完整走完 undo 链；有 undo 段时该 record 与
     * segment drop + page3 clear 位于同一 finalization MTR，内存 slot 在该 MTR 提交后才释放。恢复期不能仅凭
     * 本 record 跳过 undo/rseg 状态检查。
     *
     * @param mtr 当前短 MTR。
     * @param txn 正处于 ROLLING_BACK 的事务。
     */
    static void appendRollbackComplete(MiniTransaction mtr, Transaction txn) {
        append(mtr, txn, toRedoState(txn.state()), TransactionStateDeltaState.ROLLED_BACK,
                TransactionNo.NONE, TransactionStateDeltaReason.ROLLBACK);
    }

    /**
     * 在 recovery rollback 的原子终结 MTR 中追加终态证据。恢复期没有 live {@link Transaction}，因此显式使用
     * 已由 page3/undo first-page 交叉校验的 creator id；提交号固定 NONE。该 record 与 cache/free/drop segment + owner 转移
     * 同批，下一次 crash 即使 page3 已清也能从 redo 保留 id 高水位和 ROLLED_BACK 终态。
     *
     * @param mtr recovery finalization MTR。
     * @param creatorTransactionId 已核对的崩溃事务 creator id。
     */
    static void appendRecoveredRollback(MiniTransaction mtr, TransactionId creatorTransactionId) {
        if (mtr == null || creatorTransactionId == null || creatorTransactionId.isNone()) {
            throw new DatabaseValidationException(
                    "recovery rollback state redo requires mtr and non-NONE creator transaction id");
        }
        mtr.appendLogicalRedo(new TransactionStateDeltaRecord(
                        creatorTransactionId, TransactionStateDeltaState.ACTIVE,
                        TransactionStateDeltaState.ROLLED_BACK, TransactionNo.NONE,
                        TransactionStateDeltaReason.RECOVERY_ROLLBACK),
                MtrRedoCategory.TRX_STATE, "append recovery rollback terminal-state redo");
    }

    private static void append(MiniTransaction mtr, Transaction txn, TransactionStateDeltaState fromState,
                               TransactionStateDeltaState toState, TransactionNo transactionNo,
                               TransactionStateDeltaReason reason) {
        if (mtr == null || txn == null) {
            throw new DatabaseValidationException("transaction state redo mtr/txn must not be null");
        }
        mtr.appendLogicalRedo(new TransactionStateDeltaRecord(
                        txn.transactionId(), fromState, toState, transactionNo, reason),
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
