package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/** 按 ResultColumn 顺序排列的不可变公开行。
 *
 * @param values 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
 */
public record SqlRow(List<SqlValue> values) {
    public SqlRow {
        if (values == null || values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("SQL row values must not be null");
        }
        values = List.copyOf(values);
    }
}
