package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Storage prepared phase two 已强持久并释放事务锁的公开结果。
 *
 * @param transactionId 已完成的正 storage transaction id
 * @param committed true 为提交，false 为回滚
 * @param transactionNumber 提交时为正 transaction no；回滚时为 0
 * @param durableLsn terminal redo 已 fsync 覆盖的 LSN
 * @param releasedLockCount durable terminal 后释放的锁数量
 * @param undoneRecords rollback 反向应用数量；commit 固定为 0
 */
public record SqlXaCompletionOutcome(long transactionId, boolean committed,
                                     long transactionNumber, long durableLsn,
                                     int releasedLockCount, int undoneRecords) {

    public SqlXaCompletionOutcome {
        if (transactionId <= 0 || durableLsn < 0 || releasedLockCount < 0 || undoneRecords < 0
                || committed != (transactionNumber > 0)
                || committed && undoneRecords != 0) {
            throw new DatabaseValidationException("XA completion outcome fields are inconsistent");
        }
    }
}
