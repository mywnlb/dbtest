package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.type.ColumnValue;

/** SQL/DD 已类型化的列赋值；物理 external reference 不能由上层注入。
 *
 * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
 * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
 */
public record TableColumnAssignment(int ordinal, ColumnValue value) {
    public TableColumnAssignment {
        if (ordinal < 0 || value == null || value instanceof ColumnValue.ExternalValue) {
            throw new DatabaseValidationException("table column assignment ordinal/value is invalid");
        }
    }
}
