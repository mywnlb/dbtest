package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;

import java.util.HashSet;
import java.util.List;

/** 完整聚簇主键点查；keyValues 按 primary key part 顺序，投影保留用户请求顺序。 */
public record BoundPrimaryPointSelect(TableDefinition table, List<Integer> projectionOrdinals,
                                      List<SqlValue> keyValues) implements BoundStatement {
    public BoundPrimaryPointSelect {
        if (table == null || projectionOrdinals == null || projectionOrdinals.isEmpty() || keyValues == null
                || keyValues.size() != table.primaryIndex().keyParts().size()
                || keyValues.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("invalid bound primary-point SELECT");
        }
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projectionOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size() || !unique.add(ordinal)) {
                throw new DatabaseValidationException("bound SELECT projection ordinal is invalid/duplicate");
            }
        }
        projectionOrdinals = List.copyOf(projectionOrdinals);
        keyValues = List.copyOf(keyValues);
    }
}
