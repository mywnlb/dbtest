package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.type.ColumnValue;

/** SQL/DD 已类型化的列赋值；物理 external reference 不能由上层注入。 */
public record TableColumnAssignment(int ordinal, ColumnValue value) {
    public TableColumnAssignment {
        if (ordinal < 0 || value == null || value instanceof ColumnValue.ExternalValue) {
            throw new DatabaseValidationException("table column assignment ordinal/value is invalid");
        }
    }
}
