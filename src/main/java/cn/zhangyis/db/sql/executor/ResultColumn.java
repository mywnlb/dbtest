package cn.zhangyis.db.sql.executor;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;

/** 查询结果列；声明 DD 类型随结果返回，调用方无需从 Java variant 反推 SQL 类型。 */
public record ResultColumn(String name, ColumnTypeDefinition type) {
    public ResultColumn {
        if (name == null || name.isBlank() || type == null) {
            throw new DatabaseValidationException("result column name/type must not be null or blank");
        }
    }
}
