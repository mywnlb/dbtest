package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.AlterTableCommand;

/** 通用 ALTER 的无资源 bound command。 */
public record BoundAlterTable(AlterTableCommand command) implements BoundStatement {
    public BoundAlterTable {
        if (command == null) {
            throw new DatabaseValidationException("bound ALTER TABLE command must not be null");
        }
    }
}
