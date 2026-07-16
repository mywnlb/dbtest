package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 表聚合内的稳定列定义；columnId 不随 ordinal 调整而复用。 */
public record ColumnDefinition(long columnId, ObjectName name, ColumnTypeDefinition type, int ordinal) {
    public ColumnDefinition {
        if (columnId <= 0 || ordinal < 0) {
            throw new DatabaseValidationException("column id must be positive and ordinal non-negative");
        }
        if (name == null || type == null) {
            throw new DatabaseValidationException("column name/type must not be null");
        }
    }
}
