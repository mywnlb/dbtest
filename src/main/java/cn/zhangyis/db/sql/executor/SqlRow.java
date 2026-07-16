package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/** 按 ResultColumn 顺序排列的不可变公开行。 */
public record SqlRow(List<SqlValue> values) {
    public SqlRow {
        if (values == null || values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("SQL row values must not be null");
        }
        values = List.copyOf(values);
    }
}
