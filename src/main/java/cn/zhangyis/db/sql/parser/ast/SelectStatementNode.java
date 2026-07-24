package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;
import java.util.Optional;

/**
 * SELECT AST。condition 保留完整 boolean 语法树；lockingClause 仅表达尾部语法，
 * Parser 不猜测索引路径或 SQL 三值逻辑。
 *
 * @param star            是否为星号投影。
 * @param projections     显式投影列；star 为 true 时必须为空。
 * @param table           目标限定表名。
 * @param tableAlias      FROM 左表的可选显式或隐式别名。
 * @param join            可选二表等值 INNER JOIN；空表示单表 SELECT。
 * @param condition       非空 WHERE boolean 表达式。
 * @param orderBy         保持用户优先级顺序的排序项；空列表表示没有顺序承诺。
 * @param limit           规范化后的 offset/count；空表示不限制。
 * @param lockingClause   普通一致性读或 FOR SHARE/FOR UPDATE。
 */
public record SelectStatementNode(boolean star, List<ColumnReferenceNode> projections,
                                  QualifiedNameNode table,
                                  Optional<IdentifierNode> tableAlias,
                                  Optional<InnerJoinClauseNode> join,
                                  BooleanExpressionNode condition,
                                  List<OrderByItemNode> orderBy,
                                  Optional<LimitClauseNode> limit,
                                  SelectLockingClause lockingClause) implements StatementNode {
    public SelectStatementNode {
        if (projections == null || table == null || tableAlias == null || join == null
                || condition == null || orderBy == null
                || limit == null
                || star != projections.isEmpty() || lockingClause == null) {
            throw new DatabaseValidationException("invalid SELECT AST shape");
        }
        projections = List.copyOf(projections);
        orderBy = List.copyOf(orderBy);
    }

    /**
     * 保留 ORDER BY/LIMIT 引入前的调用形状，旧测试与内部改写明确得到“无排序、无限制”语义。
     */
    public SelectStatementNode(
            boolean star, List<ColumnReferenceNode> projections,
            QualifiedNameNode table, BooleanExpressionNode condition,
            List<OrderByItemNode> orderBy,
            Optional<LimitClauseNode> limit,
            SelectLockingClause lockingClause) {
        this(star, projections, table, Optional.empty(), Optional.empty(),
                condition, orderBy, limit, lockingClause);
    }

    /**
     * 保留 JOIN 引入前且 ORDER BY/LIMIT 已存在的调用形状。
     */
    public SelectStatementNode(
            boolean star, List<ColumnReferenceNode> projections,
            QualifiedNameNode table, BooleanExpressionNode condition,
            SelectLockingClause lockingClause) {
        this(star, projections, table, Optional.empty(), Optional.empty(), condition,
                List.of(), Optional.empty(), lockingClause);
    }
}
