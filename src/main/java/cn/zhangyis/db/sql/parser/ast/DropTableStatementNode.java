package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 一个原子 DROP TABLE 语句的目标列表。
 *
 * @param tables 保持用户顺序的一个或多个限定表名
 * @param ifExists 缺失目标是否转换为 warning
 */
public record DropTableStatementNode(
        List<QualifiedNameNode> tables, boolean ifExists)
        implements StatementNode {

    public DropTableStatementNode {
        if (tables == null || tables.isEmpty()
                || tables.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "DROP TABLE requires non-empty targets");
        }
        tables = List.copyOf(tables);
    }
}
