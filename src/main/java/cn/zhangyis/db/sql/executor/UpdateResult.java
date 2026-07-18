package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** DML affected-row 结果。
 *
 * @param affectedRows 本次 SQL 写操作实际影响的行数；必须非负，并与事务内已经成功应用的存储变更一致
 * @param transactionStatus 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
 */
public record UpdateResult(long affectedRows, TransactionStatus transactionStatus)
        implements SqlExecutionResult {
    public UpdateResult {
        if (affectedRows < 0 || transactionStatus == null) {
            throw new DatabaseValidationException("invalid update result");
        }
    }
}
