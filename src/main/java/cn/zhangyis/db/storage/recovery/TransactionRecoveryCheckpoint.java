package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * fuzzy checkpoint 对应的事务恢复高水位。
 *
 * <p>两个 counter 都表达“下一次可分配值”，因此必须大于零。恢复允许保守高估并跳号，但绝不能低估后复用
 * 已写入聚簇记录或 undo 页的事务标识。
 *
 * @param checkpointLsn 该基线覆盖到的 redo checkpoint LSN。
 * @param nextTransactionId 下一次可分配写事务 id。
 * @param nextTransactionNo 下一次可分配提交序号。
 */
public record TransactionRecoveryCheckpoint(Lsn checkpointLsn,
                                            TransactionId nextTransactionId,
                                            TransactionNo nextTransactionNo) {

    public TransactionRecoveryCheckpoint {
        if (checkpointLsn == null || nextTransactionId == null || nextTransactionNo == null) {
            throw new DatabaseValidationException("transaction recovery checkpoint fields must not be null");
        }
        if (nextTransactionId.isNone() || nextTransactionNo.isNone()) {
            throw new DatabaseValidationException(
                    "transaction recovery next counters must be positive: nextId=" + nextTransactionId
                            + ", nextNo=" + nextTransactionNo);
        }
    }

    /** 无权威 sidecar 且 redo checkpoint 为零时使用的初始高水位。 */
    public static TransactionRecoveryCheckpoint initial() {
        return new TransactionRecoveryCheckpoint(
                Lsn.of(0), TransactionId.of(1), TransactionNo.of(1));
    }
}
