package cn.zhangyis.db.sql.optimizer.logical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;

import java.util.HashSet;
import java.util.List;

/**
 * 保持用户列顺序的逻辑投影。
 *
 * @param input 同一 exact table version 的输入关系
 * @param projectionOrdinals 公开结果的 table column ordinal
 */
public record LogicalProject(RelNode input, List<Integer> projectionOrdinals)
        implements RelNode {
    /**
     * 校验并冻结用户可观察的投影顺序。
     *
     * @throws DatabaseValidationException input/投影缺失、ordinal 重复或越界时抛出
     */
    public LogicalProject {
        if (input == null || projectionOrdinals == null || projectionOrdinals.isEmpty()) {
            throw new DatabaseValidationException("logical project fields must not be null or empty");
        }
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projectionOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= input.table().columns().size()
                    || !unique.add(ordinal)) {
                throw new DatabaseValidationException(
                        "logical project ordinal is invalid or duplicate");
            }
        }
        projectionOrdinals = List.copyOf(projectionOrdinals);
    }

    @Override
    public TableDefinition table() {
        return input.table();
    }
}
