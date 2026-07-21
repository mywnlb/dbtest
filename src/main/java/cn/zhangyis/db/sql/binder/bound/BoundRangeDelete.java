package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.dd.domain.TableDefinition;

import java.util.List;

/**
 * 先锁定并物化全部聚簇 identity、再逐点 delete-mark 的范围 DELETE 计划。
 *
 * @param table exact DD version
 * @param accessIndexId 候选扫描索引
 * @param indexRange 候选物理范围
 * @param predicates 最终 residual conjunction
 * @param empty 已证明无匹配
 */
public record BoundRangeDelete(TableDefinition table, long accessIndexId,
                               BoundIndexRange indexRange, List<BoundRowPredicate> predicates,
                               boolean empty) implements BoundStatement {
    public BoundRangeDelete {
        BoundRangePlanValidation.validate(table, accessIndexId, indexRange, predicates);
        predicates = List.copyOf(predicates);
    }
}
