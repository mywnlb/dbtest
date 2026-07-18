package cn.zhangyis.db.storage.api.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.trx.TransactionState;

/**
 * prepared phase-two 成功结果。
 *
 * @param transactionId 已完成决议的写事务 id
 * @param transactionNo commit 时为真实提交序号，rollback 时为 NONE
 * @param state 只能是 COMMITTED 或 ROLLED_BACK
 * @param durableLsn 已 fsync 覆盖的 terminal redo 边界
 * @param durable 成功返回恒为 true；保留字段便于上层与普通 DML 结果统一处理
 * @param releasedLockCount terminal redo durable 后释放的事务锁数量
 * @param undoRecordsApplied rollback 本次实际反向应用的 undo 数量；commit 为 0
 */
public record PreparedTransactionCompletionResult(
        TransactionId transactionId,
        TransactionNo transactionNo,
        TransactionState state,
        Lsn durableLsn,
        boolean durable,
        int releasedLockCount,
        int undoRecordsApplied) {

    public PreparedTransactionCompletionResult {
        if (transactionId == null || transactionId.isNone() || transactionNo == null
                || state == null || durableLsn == null) {
            throw new DatabaseValidationException("prepared completion result fields must not be null/NONE");
        }
        if (state != TransactionState.COMMITTED && state != TransactionState.ROLLED_BACK) {
            throw new DatabaseValidationException("prepared completion result requires terminal state: " + state);
        }
        if (!durable || releasedLockCount < 0 || undoRecordsApplied < 0) {
            throw new DatabaseValidationException("prepared completion counters/durability are invalid");
        }
        if (state == TransactionState.COMMITTED && transactionNo.isNone()) {
            throw new DatabaseValidationException("prepared commit result requires transaction number");
        }
        if (state == TransactionState.ROLLED_BACK && !transactionNo.isNone()) {
            throw new DatabaseValidationException("prepared rollback result must not carry transaction number");
        }
    }
}
