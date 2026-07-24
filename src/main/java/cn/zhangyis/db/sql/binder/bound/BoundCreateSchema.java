package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ObjectName;

/**
 * 不持 DD 资源的 CREATE SCHEMA 意图。
 *
 * @param name 规范化 schema 名
 * @param ifNotExists 已存在时是否返回 warning
 */
public record BoundCreateSchema(
        ObjectName name, boolean ifNotExists) implements BoundStatement {

    public BoundCreateSchema {
        if (name == null) {
            throw new DatabaseValidationException(
                    "bound CREATE SCHEMA name must not be null");
        }
    }
}
