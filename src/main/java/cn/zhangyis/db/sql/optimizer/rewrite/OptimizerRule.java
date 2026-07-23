package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;

/**
 * 单条确定性 logical rewrite。规则不得访问存储、事务、可变 DD repository 或共享缓存。
 */
public interface OptimizerRule {

    /**
     * 返回用于诊断和唯一性校验的稳定规则名。
     *
     * @return 非空白且在同一 RuleProgram 中唯一的名称
     */
    String name();

    /**
     * 对完整逻辑计划尝试一次等价改写。
     *
     * @param plan 当前不可变逻辑计划
     * @return 明确区分 changed/unchanged 的结果；不得返回 {@code null}
     */
    RuleResult apply(LogicalPlan plan);
}
