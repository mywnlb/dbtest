package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;

/**
 * 单条规则的一次应用结果。
 *
 * @param changed 规则是否声明产生结构变化
 * @param plan 本次应用后的不可变逻辑计划
 */
public record RuleResult(boolean changed, LogicalPlan plan) {
    public RuleResult {
        if (plan == null) {
            throw new DatabaseValidationException(
                    "optimizer rule result plan must not be null");
        }
    }

    /**
     * 构造未变化结果。
     *
     * @param plan 原逻辑计划
     * @return changed=false 且保持原引用的结果
     */
    public static RuleResult unchanged(LogicalPlan plan) {
        return new RuleResult(false, plan);
    }

    /**
     * 构造声明已变化的结果；结构是否真的变化由 RuleProgram 相对输入统一验证。
     *
     * @param plan 规则生成的候选逻辑计划
     * @return changed=true 的结果
     */
    public static RuleResult transformed(LogicalPlan plan) {
        return new RuleResult(true, plan);
    }
}
