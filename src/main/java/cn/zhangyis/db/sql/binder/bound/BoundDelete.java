package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;
import java.util.List;

/** 主键点 DELETE；定位值按聚簇 index part 顺序冻结。
 *
 * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param primaryKeyValues 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
public record BoundDelete(TableDefinition table, List<SqlValue> primaryKeyValues) implements BoundStatement {
    public BoundDelete {
        BoundPrimaryKeyValidation.validate(table, primaryKeyValues, "DELETE");
        primaryKeyValues = List.copyOf(primaryKeyValues);
    }
}
