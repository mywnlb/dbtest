package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.CreateTableCommand;

/**
 * SQL Binder 已完成限定名、类型、default 与索引结构校验的 CREATE TABLE 意图。
 *
 * @param command 不持 DD pin、MDL、物理路径或已分配 identity 的原子建表命令；目标 schema/table
 *                状态由 implicit commit 后的 DD coordinator 在 schema IX/table X 下重验
 * @param ifNotExists 表已存在时是否转换为 warning
 */
public record BoundCreateTable(
        CreateTableCommand command, boolean ifNotExists)
        implements BoundStatement {
    public BoundCreateTable {
        if (command == null) {
            throw new DatabaseValidationException(
                    "bound CREATE TABLE command must not be null");
        }
    }

    /** 保留基础 CREATE TABLE 调用形状，默认重复表为错误。 */
    public BoundCreateTable(CreateTableCommand command) {
        this(command, false);
    }
}
