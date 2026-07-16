package cn.zhangyis.db.sql.executor.storage.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 单条 SQL 的共享绝对 deadline 已耗尽；调用方可回滚当前 statement/transaction 后继续。 */
public final class SqlStatementTimeoutException extends DatabaseRuntimeException {
    public SqlStatementTimeoutException(String message) {
        super(message);
    }
}
