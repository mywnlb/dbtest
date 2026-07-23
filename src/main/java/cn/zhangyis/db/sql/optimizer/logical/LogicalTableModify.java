package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 逻辑表修改根节点。M1 中 UPDATE/DELETE 的输入是 filter(scan)，INSERT 的输入是 values；是否使用 point
 * 或原子 range mutation 由 Optimizer 决定。
 *
 * @param table exact table version
 * @param kind SQL 修改种类
 * @param input values 或带 predicate 的 scan
 * @param assignmentOrdinals UPDATE 的有序列 ordinal，其它种类为空
 * @param assignmentValues UPDATE typed values，其它种类为空
 */
public record LogicalTableModify(TableDefinition table, ModificationKind kind, RelNode input,
                                 List<Integer> assignmentOrdinals,
                                 List<SqlValue> assignmentValues)
        implements RelNode {
    /**
     * 校验 modify/source 的 exact table identity 及 UPDATE assignment 容器形状。
     *
     * @throws DatabaseValidationException table/source 不同版、字段缺失或 assignment 与 kind 不一致时抛出
     */
    public LogicalTableModify {
        if (table == null || kind == null || input == null || assignmentOrdinals == null
                || assignmentValues == null || !input.table().equals(table)
                || assignmentOrdinals.size() != assignmentValues.size()
                || assignmentValues.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("invalid logical table modify fields");
        }
        if ((kind == ModificationKind.UPDATE) != !assignmentOrdinals.isEmpty()) {
            throw new DatabaseValidationException(
                    "logical UPDATE requires assignments and INSERT/DELETE forbid them");
        }
        assignmentOrdinals = List.copyOf(assignmentOrdinals);
        assignmentValues = List.copyOf(assignmentValues);
    }
}
