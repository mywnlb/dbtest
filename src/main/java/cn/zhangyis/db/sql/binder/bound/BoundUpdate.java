package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;
import java.util.HashSet;
import java.util.List;

/** 主键点 UPDATE；赋值按 table ordinal、定位值按聚簇 index part 顺序冻结。 */
public record BoundUpdate(TableDefinition table, List<Integer> assignmentOrdinals,
                          List<SqlValue> assignmentValues,
                          List<SqlValue> primaryKeyValues) implements BoundStatement {
    public BoundUpdate {
        if (table == null || assignmentOrdinals == null || assignmentValues == null || primaryKeyValues == null
                || assignmentOrdinals.isEmpty() || assignmentOrdinals.size() != assignmentValues.size()
                || assignmentValues.stream().anyMatch(java.util.Objects::isNull)
                || primaryKeyValues.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("invalid bound UPDATE fields");
        }
        BoundPrimaryKeyValidation.validate(table, primaryKeyValues, "UPDATE");
        HashSet<Integer> unique = new HashSet<>();
        HashSet<Integer> primaryOrdinals = table.primaryIndex().keyParts().stream()
                .map(part -> table.columns().stream()
                        .filter(column -> column.columnId() == part.columnId()).findFirst().orElseThrow().ordinal())
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
        int previous = -1;
        for (Integer ordinal : assignmentOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size()
                    || ordinal <= previous || !unique.add(ordinal) || primaryOrdinals.contains(ordinal)) {
                throw new DatabaseValidationException("bound UPDATE assignments must have unique ascending ordinals");
            }
            previous = ordinal;
        }
        assignmentOrdinals = List.copyOf(assignmentOrdinals);
        assignmentValues = List.copyOf(assignmentValues);
        primaryKeyValues = List.copyOf(primaryKeyValues);
    }
}
