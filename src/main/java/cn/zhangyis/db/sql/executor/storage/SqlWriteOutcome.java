package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.math.BigInteger;
import java.util.Optional;

/** 单条写语句的确认结果；不代表 transaction 已提交。
 *
 * @param affectedRows 本次 SQL 写操作实际影响的行数；必须非负，并与事务内已经成功应用的存储变更一致
 * @param rollbackOnly 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 * @param firstGeneratedKey 批量 INSERT 的第一个实际生成键；没有生成值或非 INSERT 时为空
 */
public record SqlWriteOutcome(
        long affectedRows, boolean rollbackOnly,
        Optional<BigInteger> firstGeneratedKey) {
    public SqlWriteOutcome {
        if (affectedRows < 0 || firstGeneratedKey == null
                || firstGeneratedKey.isPresent()
                && firstGeneratedKey.orElseThrow().signum() <= 0) {
            throw new DatabaseValidationException("SQL write outcome fields are invalid");
        }
    }

    /**
     * 保留非生成写操作的 v1 构造形状。
     *
     * @param affectedRows 实际影响行数
     * @param rollbackOnly 事务是否只允许回滚
     */
    public SqlWriteOutcome(long affectedRows, boolean rollbackOnly) {
        this(affectedRows, rollbackOnly, Optional.empty());
    }
}
