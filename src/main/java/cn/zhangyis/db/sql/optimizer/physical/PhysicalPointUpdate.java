package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 完整聚簇主键定位的点 UPDATE。
 *
 * @param table exact DD table version
 * @param assignmentOrdinals 严格升序且不含聚簇 key 的赋值列
 * @param assignmentValues 与 ordinal 一一对应的 typed values
 * @param primaryKeyValues 按聚簇 index key-part 顺序排列的完整定位值
 */
public record PhysicalPointUpdate(
        TableDefinition table, List<Integer> assignmentOrdinals,
        List<SqlValue> assignmentValues, List<SqlValue> primaryKeyValues)
        implements PhysicalPlan {
    /**
     * 校验 point identity 与非主键 patch，并复制全部有序集合。
     *
     * @throws DatabaseValidationException 主键、assignment 或 table 不满足 point UPDATE 契约时抛出
     */
    public PhysicalPointUpdate {
        if (table == null || assignmentOrdinals == null || assignmentValues == null
                || primaryKeyValues == null || assignmentOrdinals.isEmpty()
                || assignmentOrdinals.size() != assignmentValues.size()
                || assignmentValues.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("invalid physical point UPDATE fields");
        }
        PhysicalPlanValidation.validatePrimaryKey(table, primaryKeyValues, "UPDATE");
        PhysicalPlanValidation.validateAssignments(
                table, assignmentOrdinals, assignmentValues, "point UPDATE");
        assignmentOrdinals = List.copyOf(assignmentOrdinals);
        assignmentValues = List.copyOf(assignmentValues);
        primaryKeyValues = List.copyOf(primaryKeyValues);
    }
}
