package cn.zhangyis.db.sql.executor.storage.exception;

/** handle 来源错误、并发占用或 transaction 已终结。 */
public final class SqlTransactionStateException extends SqlStorageException {
    public SqlTransactionStateException(String message) { super(message); }
    public SqlTransactionStateException(String message, Throwable cause) { super(message, cause); }
}
