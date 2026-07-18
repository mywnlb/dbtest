package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.QualifiedTableName;

/**
 * 独立 CREATE INDEX 与 ALTER TABLE ADD INDEX 共享的不可变 DD 命令。
 *
 * @param table 待修改的完整 schema/table 名称
 * @param index 只允许非聚簇二级索引的名称、唯一性和 key parts
 */
public record CreateSecondaryIndexCommand(QualifiedTableName table, CreateIndexSpec index) {

    public CreateSecondaryIndexCommand {
        if (table == null || index == null || index.clustered()) {
            throw new DatabaseValidationException(
                    "CREATE INDEX requires a table and a non-clustered index definition");
        }
    }
}
