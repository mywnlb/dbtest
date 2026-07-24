package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.math.BigInteger;
import java.util.Optional;

/** DML affected-row 结果。
 *
 * @param affectedRows 本次 SQL 写操作实际影响的行数；必须非负，并与事务内已经成功应用的存储变更一致
 * @param transactionStatus 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
 * @param firstGeneratedKey 批量 INSERT 中第一个由引擎生成的键；其它写操作为空
 */
public record UpdateResult(
        long affectedRows, TransactionStatus transactionStatus,
        Optional<BigInteger> firstGeneratedKey)
        implements SqlExecutionResult {
    public UpdateResult {
        if (affectedRows < 0 || transactionStatus == null
                || firstGeneratedKey == null
                || firstGeneratedKey.isPresent()
                && firstGeneratedKey.orElseThrow().signum() <= 0) {
            throw new DatabaseValidationException("invalid update result");
        }
    }

    /**
     * 保留非生成写操作的 v1 构造形状。
     *
     * @param affectedRows 实际影响行数
     * @param transactionStatus 语句后的事务状态
     */
    public UpdateResult(long affectedRows, TransactionStatus transactionStatus) {
        this(affectedRows, transactionStatus, Optional.empty());
    }
}
