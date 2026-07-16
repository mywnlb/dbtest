package cn.zhangyis.db.sql.executor.storage.exception;

/** 写语句失败且 statement rollback 未能确认；真实事务已 rollback-only。 */
public final class SqlStatementRollbackException extends SqlStorageException {
    private final boolean rollbackOnly;
    public SqlStatementRollbackException(String message, boolean rollbackOnly, Throwable cause) {
        super(message, cause);
        this.rollbackOnly = rollbackOnly;
    }
    public boolean rollbackOnly() { return rollbackOnly; }
}
