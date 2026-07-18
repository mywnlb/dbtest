package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.ObjectName;

/** CREATE TABLE 中尚未分配 table-local columnId/ordinal 的列声明。
 *
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param type 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 */
public record CreateColumnSpec(ObjectName name, ColumnTypeDefinition type) {
    public CreateColumnSpec {
        if (name == null || type == null) {
            throw new DatabaseValidationException("create column name/type must not be null");
        }
    }
}
