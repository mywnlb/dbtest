package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.DropSecondaryIndexCommand;

/**
 * Binder 已完成 catalog/schema/table/index 名规范化的 DROP INDEX 意图。
 *
 * @param command 不含 DD pin、page、root 或 segment 的纯 DDL command；目标存在性在 implicit commit 后由 DD 重验
 */
public record BoundDropIndex(DropSecondaryIndexCommand command) implements BoundStatement {

    /**
     * 冻结可交给 Session DDL gateway 的命令。
     *
     * @throws DatabaseValidationException command 缺失时抛出，且不触发 implicit commit
     */
    public BoundDropIndex {
        if (command == null) {
            throw new DatabaseValidationException("bound DROP INDEX command must not be null");
        }
    }
}
