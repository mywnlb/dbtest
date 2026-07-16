package cn.zhangyis.db.sql.executor;

/** 一条 SQL 返回时与用户相关的事务状态，不携带内部事务 id 或 undo 状态。 */
public record TransactionStatus(boolean autocommit, boolean transactionActive, boolean rollbackOnly) {
    public TransactionStatus {
        if (!transactionActive && rollbackOnly) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "rollback-only requires an active transaction");
        }
    }

    public static TransactionStatus idle(boolean autocommit) {
        return new TransactionStatus(autocommit, false, false);
    }
}
