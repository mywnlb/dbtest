package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** BEGIN/COMMIT/ROLLBACK/SET 等命令结果。
 *
 * @param transactionStatus 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
 */
public record CommandResult(TransactionStatus transactionStatus) implements SqlExecutionResult {
    public CommandResult {
        if (transactionStatus == null) throw new DatabaseValidationException("command status must not be null");
    }
}
