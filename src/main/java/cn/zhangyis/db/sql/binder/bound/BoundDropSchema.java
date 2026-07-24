package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ObjectName;

/**
 * 不持 DD 资源的 DROP SCHEMA 意图。
 *
 * @param name 规范化 schema 名
 * @param ifExists 缺失时是否返回 warning
 */
public record BoundDropSchema(
        ObjectName name, boolean ifExists) implements BoundStatement {

    public BoundDropSchema {
        if (name == null) {
            throw new DatabaseValidationException(
                    "bound DROP SCHEMA name must not be null");
        }
    }
}
