package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 单条写语句的确认结果；不代表 transaction 已提交。 */
public record SqlWriteOutcome(long affectedRows, boolean rollbackOnly) {
    public SqlWriteOutcome {
        if (affectedRows < 0) throw new DatabaseValidationException("SQL affected rows must not be negative");
    }
}
