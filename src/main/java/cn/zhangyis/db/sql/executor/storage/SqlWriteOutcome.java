package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 单条写语句的确认结果；不代表 transaction 已提交。
 *
 * @param affectedRows 本次 SQL 写操作实际影响的行数；必须非负，并与事务内已经成功应用的存储变更一致
 * @param rollbackOnly 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 */
public record SqlWriteOutcome(long affectedRows, boolean rollbackOnly) {
    public SqlWriteOutcome {
        if (affectedRows < 0) throw new DatabaseValidationException("SQL affected rows must not be negative");
    }
}
