package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import java.util.List;

/**
 * v1 SELECT AST。谓词仍限定为 AND 连接的等值比较；lockingClause 仅表达尾部语法，不在 Parser 猜测索引路径。
 *
 * @param star            是否为星号投影。
 * @param projections     显式投影列；star 为 true 时必须为空。
 * @param table           目标限定表名。
 * @param predicates      非空 AND 等值谓词。
 * @param lockingClause   普通一致性读或 FOR SHARE/FOR UPDATE。
 */
public record SelectStatementNode(boolean star, List<IdentifierNode> projections, QualifiedNameNode table,
                                  List<EqualityPredicateNode> predicates,
                                  SelectLockingClause lockingClause) implements StatementNode {
    public SelectStatementNode {
        if (projections == null || table == null || predicates == null || predicates.isEmpty()
                || star != projections.isEmpty() || lockingClause == null) {
            throw new DatabaseValidationException("invalid SELECT AST shape");
        }
        projections = List.copyOf(projections); predicates = List.copyOf(predicates);
    }

    /**
     * 保留既有 AST 构造兼容性；未显式给出 clause 的调用方表示普通一致性读。
     */
    public SelectStatementNode(boolean star, List<IdentifierNode> projections, QualifiedNameNode table,
                               List<EqualityPredicateNode> predicates) {
        this(star, projections, table, predicates, SelectLockingClause.NONE);
    }
}
