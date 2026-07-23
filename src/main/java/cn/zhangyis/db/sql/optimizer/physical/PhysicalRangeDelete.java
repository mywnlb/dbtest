package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;

/**
 * 先锁定并物化全部聚簇 identity、再在同一 statement guard 内逐点 delete-mark 的范围 DELETE。
 *
 * @param table exact DD table version
 * @param accessIndexId 候选扫描索引稳定 id
 * @param indexRange 候选物理范围
 * @param predicates 最终完整 boolean residual
 * @param empty 已证明无匹配
 */
public record PhysicalRangeDelete(
        TableDefinition table, long accessIndexId, IndexRange indexRange,
        PredicateSet predicates, boolean empty) implements PhysicalPlan {
    /**
     * 交叉校验并冻结候选范围与完整 residual。
     *
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException
     *         索引、endpoint、predicate 或 table 不一致时抛出
     */
    public PhysicalRangeDelete {
        PhysicalPlanValidation.validateRange(table, accessIndexId, indexRange, predicates);
    }
}
