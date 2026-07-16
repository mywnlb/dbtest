package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** BEGIN/COMMIT/ROLLBACK/SET 等命令结果。 */
public record CommandResult(TransactionStatus transactionStatus) implements SqlExecutionResult {
    public CommandResult {
        if (transactionStatus == null) throw new DatabaseValidationException("command status must not be null");
    }
}
