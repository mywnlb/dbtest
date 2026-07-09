package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * 事务状态 logical redo。该 record 不指向数据页，不参与 pageLSN 幂等判断；它只把 commit/rollback
 * 状态边界写入 redo 顺序，供恢复诊断和后续事务表重建切片消费。
 *
 * <p>当前恢复 handler 对它执行 no-op apply。事务是否需要回滚、history list 如何重建，仍以 undo/rseg
 * 页上的持久状态为权威，避免在 redo replay 阶段执行普通事务状态机。
 *
 * @param transactionId 事务写 id；只读或未写事务可为 NONE。
 * @param fromState     状态变化前的稳定状态。
 * @param toState       状态变化后的稳定状态。
 * @param transactionNo 提交号；rollback 或未提交边界可为 NONE。
 * @param reason        产生该状态变化的原因。
 */
public record TransactionStateDeltaRecord(
        TransactionId transactionId,
        TransactionStateDeltaState fromState,
        TransactionStateDeltaState toState,
        TransactionNo transactionNo,
        TransactionStateDeltaReason reason) implements RedoRecord {

    /** tag(1)+transactionId(8)+fromState(1)+toState(1)+transactionNo(8)+reason(1)。 */
    private static final int BYTES = 20;

    public TransactionStateDeltaRecord {
        if (transactionId == null || fromState == null || toState == null
                || transactionNo == null || reason == null) {
            throw new DatabaseValidationException("transaction state delta fields must not be null");
        }
    }

    @Override
    public int byteLength() {
        return BYTES;
    }
}
