package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 已确认提交结果；transactionNumber=0 表示只读或未首写事务。 */
public record SqlCommitOutcome(long transactionNumber, boolean durable, int releasedLockCount) {
    public SqlCommitOutcome {
        if (transactionNumber < 0 || releasedLockCount < 0) {
            throw new DatabaseValidationException("invalid SQL commit outcome counters");
        }
    }
}
