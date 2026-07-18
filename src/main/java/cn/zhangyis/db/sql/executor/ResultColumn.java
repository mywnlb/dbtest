package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;

/** 查询结果列；声明 DD 类型随结果返回，调用方无需从 Java variant 反推 SQL 类型。
 *
 * @param name 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
 * @param type 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 */
public record ResultColumn(String name, ColumnTypeDefinition type) {
    public ResultColumn {
        if (name == null || name.isBlank() || type == null) {
            throw new DatabaseValidationException("result column name/type must not be null or blank");
        }
    }
}
