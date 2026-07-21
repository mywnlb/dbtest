package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;

import java.util.HashSet;
import java.util.List;

/**
 * 先锁定并物化全部聚簇 identity、再逐点修改的范围 UPDATE 计划。
 *
 * @param table exact DD version
 * @param assignmentOrdinals 升序且不含聚簇 key 的赋值列
 * @param assignmentValues 与 ordinal 一一对应的 typed values
 * @param accessIndexId 候选扫描索引
 * @param indexRange 候选物理范围
 * @param predicates 最终 residual conjunction
 * @param empty 已证明无匹配
 */
public record BoundRangeUpdate(TableDefinition table, List<Integer> assignmentOrdinals,
                               List<SqlValue> assignmentValues, long accessIndexId,
                               BoundIndexRange indexRange, List<BoundRowPredicate> predicates,
                               boolean empty) implements BoundStatement {
    public BoundRangeUpdate {
        BoundRangePlanValidation.validate(table, accessIndexId, indexRange, predicates);
        if (assignmentOrdinals == null || assignmentValues == null || assignmentOrdinals.isEmpty()
                || assignmentOrdinals.size() != assignmentValues.size()
                || assignmentValues.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("invalid bound range UPDATE assignments");
        }
        HashSet<Integer> unique = new HashSet<>();
        HashSet<Long> primaryColumnIds = new HashSet<>();
        table.primaryIndex().keyParts().forEach(part -> primaryColumnIds.add(part.columnId()));
        int previous = -1;
        for (Integer ordinal : assignmentOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size()
                    || ordinal <= previous || !unique.add(ordinal)) {
                throw new DatabaseValidationException("range UPDATE ordinals must be unique and ascending");
            }
            if (primaryColumnIds.contains(table.columns().get(ordinal).columnId())) {
                throw new DatabaseValidationException(
                        "range UPDATE cannot assign a clustered primary-key column");
            }
            previous = ordinal;
        }
        assignmentOrdinals = List.copyOf(assignmentOrdinals);
        assignmentValues = List.copyOf(assignmentValues);
        predicates = List.copyOf(predicates);
    }
}
