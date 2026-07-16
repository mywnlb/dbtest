package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;

import java.util.List;

/** 完整聚簇行 INSERT；values 始终按 exact DD column ordinal 排列。 */
public record BoundClusteredInsert(TableDefinition table, List<SqlValue> values) implements BoundStatement {
    public BoundClusteredInsert {
        if (table == null || values == null || values.size() != table.columns().size()
                || values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("bound INSERT must contain one value per table column");
        }
        values = List.copyOf(values);
    }
}
