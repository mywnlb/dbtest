package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 已确认完整回滚结果。 */
public record SqlRollbackOutcome(int undoneRecords, int releasedLockCount) {
    public SqlRollbackOutcome {
        if (undoneRecords < 0 || releasedLockCount < 0) {
            throw new DatabaseValidationException("invalid SQL rollback outcome counters");
        }
    }
}
