package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** DML affected-row 结果。 */
public record UpdateResult(long affectedRows, TransactionStatus transactionStatus)
        implements SqlExecutionResult {
    public UpdateResult {
        if (affectedRows < 0 || transactionStatus == null) {
            throw new DatabaseValidationException("invalid update result");
        }
    }
}
