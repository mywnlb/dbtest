package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.BoundLimit;
import cn.zhangyis.db.sql.binder.bound.BoundSortKey;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

/**
 * 保持用户列顺序的逻辑投影。
 *
 * @param input 同一 exact table version 的输入关系
 * @param projectionOrdinals 公开结果的 table column ordinal
 * @param orderBy 投影前应用的排序键，因此允许引用未公开列
 * @param limit 排序后应用的 offset/count
 */
public record LogicalProject(
        RelNode input, List<Integer> projectionOrdinals,
        List<BoundSortKey> orderBy, Optional<BoundLimit> limit)
        implements RelNode {
    /**
     * 校验并冻结用户可观察的投影顺序。
     *
     * @throws DatabaseValidationException input/投影缺失、ordinal 重复或越界时抛出
     */
    public LogicalProject {
        if (input == null || projectionOrdinals == null || projectionOrdinals.isEmpty()
                || orderBy == null || limit == null) {
            throw new DatabaseValidationException("logical project fields must not be null or empty");
        }
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projectionOrdinals) {
            if (ordinal == null || ordinal < 0
                    || ordinal >= input.tables().stream()
                    .mapToInt(table -> table.columns().size()).sum()
                    || !unique.add(ordinal)) {
                throw new DatabaseValidationException(
                        "logical project ordinal is invalid or duplicate");
            }
        }
        for (BoundSortKey key : orderBy) {
            if (key == null || key.columnOrdinal() >= input.tables().stream()
                    .mapToInt(table -> table.columns().size()).sum()
                    || flattenedColumnId(
                    input.tables(), key.columnOrdinal()) != key.columnId()) {
                throw new DatabaseValidationException(
                        "logical sort key does not belong to input table");
            }
        }
        projectionOrdinals = List.copyOf(projectionOrdinals);
        orderBy = List.copyOf(orderBy);
    }

    /**
     * 保留排序能力引入前的构造形状。
     */
    public LogicalProject(RelNode input, List<Integer> projectionOrdinals) {
        this(input, projectionOrdinals, List.of(), Optional.empty());
    }

    @Override
    public List<TableDefinition> tables() {
        return input.tables();
    }

    private static long flattenedColumnId(
            List<TableDefinition> tables, int ordinal) {
        int remaining = ordinal;
        for (TableDefinition table : tables) {
            if (remaining < table.columns().size()) {
                return table.columns().get(remaining).columnId();
            }
            remaining -= table.columns().size();
        }
        throw new DatabaseValidationException(
                "logical flattened column ordinal exceeds relation schema");
    }
}
