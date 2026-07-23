package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.optimizer.logical.PredicateSet;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 先锁定并物化全部聚簇 identity、再在同一 statement guard 内逐点修改的范围 UPDATE。
 *
 * @param table exact DD table version
 * @param assignmentOrdinals 严格升序且不含聚簇 key 的赋值列
 * @param assignmentValues 与 ordinal 一一对应的 typed values
 * @param accessIndexId 候选扫描索引稳定 id
 * @param indexRange 候选物理范围
 * @param predicates 最终完整 boolean residual
 * @param empty 已证明无匹配
 */
public record PhysicalRangeUpdate(
        TableDefinition table, List<Integer> assignmentOrdinals,
        List<SqlValue> assignmentValues, long accessIndexId, IndexRange indexRange,
        PredicateSet predicates, boolean empty) implements PhysicalPlan {
    /**
     * 校验并冻结原子范围修改的候选访问、完整 residual 与非主键 patch。
     *
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException
     *         range、predicate、assignment 或 table/index identity 无效时抛出
     */
    public PhysicalRangeUpdate {
        PhysicalPlanValidation.validateRange(table, accessIndexId, indexRange, predicates);
        PhysicalPlanValidation.validateAssignments(
                table, assignmentOrdinals, assignmentValues, "range UPDATE");
        assignmentOrdinals = List.copyOf(assignmentOrdinals);
        assignmentValues = List.copyOf(assignmentValues);
    }
}
