package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.QualifiedTableName;

import java.util.HashSet;
import java.util.List;

/**
 * 保持一条 SQL 原子边界的 DROP TABLE 目标集合。
 *
 * @param tables 已补全 schema 且保持用户顺序的目标
 * @param ifExists 缺失目标是否转换为 warning
 */
public record BoundDropTables(
        List<QualifiedTableName> tables, boolean ifExists)
        implements BoundStatement {

    public BoundDropTables {
        if (tables == null || tables.isEmpty()
                || tables.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "bound DROP TABLE requires targets");
        }
        HashSet<String> names = new HashSet<>();
        for (QualifiedTableName table : tables) {
            if (!names.add(table.canonicalKey())) {
                throw new DatabaseValidationException(
                        "DROP TABLE repeats target: " + table.canonicalKey());
            }
        }
        tables = List.copyOf(tables);
    }
}
