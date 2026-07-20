package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;

/**
 * 两种 DROP INDEX SQL 语法共享的不可变 DD 命令。
 *
 * @param table 已按 Session current schema 规范化的完整表名
 * @param indexName 待删除二级索引名；DD coordinator 在 table MDL X 下重新解析为稳定 IndexId
 */
public record DropSecondaryIndexCommand(QualifiedTableName table, ObjectName indexName) {

    /**
     * 冻结逻辑 DROP 目标，不携带 root、segment 或 page 等存储内部身份。
     *
     * @throws DatabaseValidationException 表名或索引名缺失时抛出，且不产生 DDL 副作用
     */
    public DropSecondaryIndexCommand {
        if (table == null || indexName == null) {
            throw new DatabaseValidationException("DROP INDEX requires table and index name");
        }
    }
}
