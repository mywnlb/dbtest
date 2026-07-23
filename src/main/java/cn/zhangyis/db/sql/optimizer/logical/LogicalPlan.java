package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 单条关系 SQL 的不可变逻辑计划根。
 *
 * @param root project 或 table-modify 根节点
 */
public record LogicalPlan(RelNode root) {
    /**
     * 冻结单条语句的逻辑根。
     *
     * @throws DatabaseValidationException 根缺失时抛出；不会发布空逻辑计划
     */
    public LogicalPlan {
        if (root == null) {
            throw new DatabaseValidationException("logical plan root must not be null");
        }
    }
}
