package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.CreateSecondaryIndexCommand;

/**
 * SQL Binder 已完成 schema 默认值与标识符规范化的 CREATE INDEX 意图。
 *
 * @param command 不含 page/root/segment 的纯 DD 命令；列存在性由持 table MDL X 的 coordinator 重验
 */
public record BoundCreateIndex(CreateSecondaryIndexCommand command) implements BoundStatement {
    public BoundCreateIndex {
        if (command == null) {
            throw new DatabaseValidationException("bound CREATE INDEX command must not be null");
        }
    }
}
