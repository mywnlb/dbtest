package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * 事务系统两个分配器的原子高水位快照。
 *
 * <p>字段都是“下一次可分配值”而不是最近已分配值；checkpoint 持久化该对象后，恢复可跳过号码但不会复用
 * 崩溃前已对外可见的事务标识。
 *
 * @param nextTransactionId 下一次可分配写事务 id。
 * @param nextTransactionNo 下一次可分配提交序号。
 */
public record TransactionCounterSnapshot(TransactionId nextTransactionId,
                                         TransactionNo nextTransactionNo) {

    public TransactionCounterSnapshot {
        if (nextTransactionId == null || nextTransactionNo == null) {
            throw new DatabaseValidationException("transaction counter snapshot fields must not be null");
        }
        if (nextTransactionId.isNone() || nextTransactionNo.isNone()) {
            throw new DatabaseValidationException(
                    "transaction counter snapshot next values must be positive: nextId=" + nextTransactionId
                            + ", nextNo=" + nextTransactionNo);
        }
    }
}
