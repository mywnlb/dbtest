package cn.zhangyis.db.sql.optimizer.rewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * 有界、确定且无共享状态的 logical rule program。
 */
public final class RuleProgram {
    /** 固定声明顺序的规则快照。 */
    private final List<OptimizerRule> rules;
    /** 单次 optimize 允许的最大完整 pass，防止错误规则无界占用 Session。 */
    private final int maxPasses;

    /**
     * 创建规则程序并校验稳定名称和迭代预算。
     *
     * @param rules 按执行顺序排列的无状态规则；名称必须非空且唯一
     * @param maxPasses 最大完整迭代轮数；必须为正
     * @throws DatabaseValidationException 规则、名称或预算非法时抛出
     */
    public RuleProgram(List<OptimizerRule> rules, int maxPasses) {
        if (rules == null || rules.stream().anyMatch(java.util.Objects::isNull)
                || maxPasses <= 0) {
            throw new DatabaseValidationException(
                    "invalid optimizer rule program");
        }
        LinkedHashSet<String> names = new LinkedHashSet<>();
        for (OptimizerRule rule : rules) {
            if (rule.name() == null || rule.name().isBlank()
                    || !names.add(rule.name())) {
                throw new DatabaseValidationException(
                        "optimizer rule names must be non-blank and unique");
            }
        }
        this.rules = List.copyOf(rules);
        this.maxPasses = maxPasses;
    }

    /**
     * 创建 M3 默认等价改写程序。
     *
     * @return AND/OR 展平、comparison 规范化、三值折叠的 16-pass 有界程序
     */
    public static RuleProgram standard() {
        return new RuleProgram(List.of(
                new FlattenConjunctionRule(),
                new FlattenDisjunctionRule(),
                new CanonicalizeComparisonRule(),
                new FoldTruthRule()), 16);
    }

    /**
     * 执行规则直到固定点，并拒绝伪变化、结构循环和不收敛。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验输入并把初始结构加入本次调用私有的 seen 集。</li>
     *     <li>按声明顺序应用规则；changed 结果必须与该规则输入结构不同。</li>
     *     <li>完整 pass 无变化时返回当前引用；重复结构说明规则循环并立即失败。</li>
     *     <li>达到最大 pass 仍变化时 fail-closed，不向物理计划或 Session 发布部分结果。</li>
     * </ol>
     *
     * @param plan Converter 产生的不可变逻辑计划
     * @return 固定点逻辑计划；输入已是固定点时返回同一对象引用
     * @throws SqlOptimizationException 规则协议损坏、循环或不收敛时抛出
     */
    public LogicalPlan rewrite(LogicalPlan plan) {
        // 1、规则失败必须早于任何物理计划或执行资源。
        if (plan == null) {
            throw new SqlOptimizationException(
                    "logical plan for rule program must not be null");
        }
        HashSet<LogicalPlan> seen = new HashSet<>();
        seen.add(plan);
        LogicalPlan current = plan;
        for (int pass = 1; pass <= maxPasses; pass++) {
            // 2、每条规则只观察上一条规则的完整不可变输出。
            boolean changed = false;
            for (OptimizerRule rule : rules) {
                LogicalPlan before = current;
                RuleResult result = rule.apply(before);
                if (result == null) {
                    throw new SqlOptimizationException(
                            "optimizer rule returned null: " + rule.name());
                }
                if (!result.changed()) {
                    if (!result.plan().equals(before)) {
                        throw new SqlOptimizationException(
                                "unchanged optimizer rule altered plan: "
                                        + rule.name());
                    }
                    continue;
                }
                if (result.plan().equals(before)) {
                    throw new SqlOptimizationException(
                            "optimizer rule reported false change: "
                                    + rule.name());
                }
                current = result.plan();
                changed = true;
            }
            // 3、固定点返回当前引用；循环不能等待 pass budget 才被发现。
            if (!changed) {
                return current;
            }
            if (!seen.add(current)) {
                throw new SqlOptimizationException(
                        "optimizer rule cycle detected after pass " + pass);
            }
        }
        // 4、未知规则组合不能无界执行或发布中间计划。
        throw new SqlOptimizationException(
                "optimizer rules did not converge within "
                        + maxPasses + " passes");
    }
}
