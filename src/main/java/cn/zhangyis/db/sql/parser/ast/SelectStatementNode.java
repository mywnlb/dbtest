package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;

/**
 * SELECT AST。condition 保留完整 boolean 语法树；lockingClause 仅表达尾部语法，
 * Parser 不猜测索引路径或 SQL 三值逻辑。
 *
 * @param star            是否为星号投影。
 * @param projections     显式投影列；star 为 true 时必须为空。
 * @param table           目标限定表名。
 * @param condition       非空 WHERE boolean 表达式。
 * @param lockingClause   普通一致性读或 FOR SHARE/FOR UPDATE。
 */
public record SelectStatementNode(boolean star, List<IdentifierNode> projections, QualifiedNameNode table,
                                  BooleanExpressionNode condition,
                                  SelectLockingClause lockingClause) implements StatementNode {
    public SelectStatementNode {
        if (projections == null || table == null || condition == null
                || star != projections.isEmpty() || lockingClause == null) {
            throw new DatabaseValidationException("invalid SELECT AST shape");
        }
        projections = List.copyOf(projections);
    }
}
