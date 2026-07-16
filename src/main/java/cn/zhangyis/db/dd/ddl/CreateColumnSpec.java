package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.ObjectName;

/** CREATE TABLE 中尚未分配 table-local columnId/ordinal 的列声明。 */
public record CreateColumnSpec(ObjectName name, ColumnTypeDefinition type) {
    public CreateColumnSpec {
        if (name == null || type == null) {
            throw new DatabaseValidationException("create column name/type must not be null");
        }
    }
}
