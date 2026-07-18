package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * 事务状态 logical redo。该 record 不指向数据页，不参与 pageLSN 幂等判断；它把 commit/rollback 状态边界
 * 写入 redo 顺序，供正式 recovery table 重建事务终态与 transaction id/no 高水位。
 *
 * <p>恢复 handler 只按 redo 顺序把它交给 {@link TransactionStateDeltaSink}；事务终态由 recovery table 与
 * undo/rseg page3 交叉校验，handler 本身不执行普通事务状态机、undo rollback 或 MVCC 可见性判断。
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

    /** tag(1)+transactionId(8)+fromState(1)+toState(1)+transactionNo(8)+reason(1)。
     *
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
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
