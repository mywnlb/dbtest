package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Storage XA phase one 已强持久的公开结果。
 *
 * @param transactionId registry 已在 PREPARING 中绑定的正 transaction id
 * @param durableLsn storage PREPARED redo 已 fsync 覆盖的正 LSN
 */
public record SqlXaPrepareOutcome(long transactionId, long durableLsn) {

    public SqlXaPrepareOutcome {
        if (transactionId <= 0 || durableLsn < 0) {
            throw new DatabaseValidationException("XA prepare outcome is invalid");
        }
    }
}
