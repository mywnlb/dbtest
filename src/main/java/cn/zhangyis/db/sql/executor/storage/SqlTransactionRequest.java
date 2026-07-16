package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 开启 SQL transaction 的稳定请求；不出现 storage 枚举。 */
public record SqlTransactionRequest(SqlIsolationLevel isolationLevel, boolean readOnly, boolean autocommit) {
    public SqlTransactionRequest {
        if (isolationLevel == null) throw new DatabaseValidationException("SQL isolation level must not be null");
    }
}
