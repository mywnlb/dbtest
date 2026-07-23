package cn.zhangyis.db.sql.optimizer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPlan;
import cn.zhangyis.db.sql.optimizer.rewrite.RuleProgram;

/**
 * M3 确定性优化编排器。它先把逻辑计划改写到规则固定点，再委托访问路径选择器生成物理计划；
 * 两个协作者均无共享可变状态，可由多个 Session 复用。
 */
public final class HeuristicQueryOptimizer implements QueryOptimizer {
    /** 仅执行 SQL 三值等价 logical rewrite 的有界规则程序。 */
    private final RuleProgram ruleProgram;
    /** 从规则固定点选择 M1-compatible 访问路径的策略。 */
    private final HeuristicAccessPathSelector accessPathSelector;

    /**
     * 创建使用 M3 默认规则和启发式访问路径的优化器。
     */
    public HeuristicQueryOptimizer() {
        this(RuleProgram.standard(), new HeuristicAccessPathSelector());
    }

    /**
     * 创建显式组合的优化器，供测试验证规则失败与后续策略替换。
     *
     * @param ruleProgram 有界、确定的 logical rewrite 程序
     * @param accessPathSelector 规则固定点之后的物理访问策略
     * @throws DatabaseValidationException 任一协作者缺失时抛出
     */
    public HeuristicQueryOptimizer(
            RuleProgram ruleProgram,
            HeuristicAccessPathSelector accessPathSelector) {
        if (ruleProgram == null || accessPathSelector == null) {
            throw new DatabaseValidationException(
                    "optimizer collaborators must not be null");
        }
        this.ruleProgram = ruleProgram;
        this.accessPathSelector = accessPathSelector;
    }

    /**
     * 先完成等价改写，再选择唯一物理访问路径。
     *
     * <ol>
     *     <li>在本次调用私有状态中运行规则到固定点；失败不发布部分 logical plan。</li>
     *     <li>把固定点交给访问路径选择器；该阶段不创建事务、锁、ReadView 或 storage cursor。</li>
     * </ol>
     *
     * @param logicalPlan Converter 产生的不可变逻辑计划
     * @return 保持 SQL 三值语义和 exact metadata version 的物理计划
     * @throws SqlOptimizationException 规则不收敛或无法形成安全访问路径时抛出
     */
    @Override
    public PhysicalPlan optimize(LogicalPlan logicalPlan) {
        // 1、RuleProgram 负责输入校验、固定点和循环检测。
        LogicalPlan rewritten = ruleProgram.rewrite(logicalPlan);
        // 2、Selector 只消费完整固定点，不观察或发布中间规则状态。
        return accessPathSelector.select(rewritten);
    }
}
