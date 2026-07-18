package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;
import java.util.List;

/** 主键点 DELETE；定位值按聚簇 index part 顺序冻结。 */
public record BoundDelete(TableDefinition table, List<SqlValue> primaryKeyValues) implements BoundStatement {
    public BoundDelete {
        BoundPrimaryKeyValidation.validate(table, primaryKeyValues, "DELETE");
        primaryKeyValues = List.copyOf(primaryKeyValues);
    }
}
