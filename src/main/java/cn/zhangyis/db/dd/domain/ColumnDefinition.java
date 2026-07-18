package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 表聚合内的稳定列定义；columnId 不随 ordinal 调整而复用。
 *
 * @param columnId 参与 {@code 构造} 的原始数值身份 {@code columnId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
 * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param type 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
 */
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
