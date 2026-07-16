package cn.zhangyis.db.sql.executor.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** SQL 执行/结果映射失败。 */
public class SqlExecutionException extends DatabaseRuntimeException {
    public SqlExecutionException(String message) { super(message); }
    public SqlExecutionException(String message, Throwable cause) { super(message, cause); }
}
