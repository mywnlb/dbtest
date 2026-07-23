package cn.zhangyis.db.sql.optimizer;

import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPlan;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;

/**
 * 从关系逻辑树选择可执行物理访问路径的稳定边界。实现可从 M1 启发式规则演进为 memo/cost
 * optimizer，调用方无需依赖具体搜索算法。
 */
public interface QueryOptimizer {

    /**
     * 在不创建事务或存储游标的前提下选择物理计划。
     *
     * @param logicalPlan Compiler 的 logical converter 产生的不可变逻辑计划
     * @return 保持相同 SQL 语义和 exact metadata version 的物理计划
     * @throws SqlOptimizationException 逻辑形状、DD 索引引用或受支持访问路径无法形成安全计划时抛出
     */
    PhysicalPlan optimize(LogicalPlan logicalPlan);
}
