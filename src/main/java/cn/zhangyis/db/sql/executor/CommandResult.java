package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/** BEGIN/COMMIT/ROLLBACK/SET 等命令结果。
 *
 * @param transactionStatus 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
 * @param warnings 成功语句产生的不可变 warning；普通命令为空
 */
public record CommandResult(
        TransactionStatus transactionStatus,
        List<SqlWarning> warnings) implements SqlExecutionResult {
    public CommandResult {
        if (transactionStatus == null || warnings == null
                || warnings.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "command status/warnings must not be null");
        }
        warnings = List.copyOf(warnings);
    }

    /** 普通无 warning 命令的兼容构造器。 */
    public CommandResult(TransactionStatus transactionStatus) {
        this(transactionStatus, List.of());
    }
}
