package cn.zhangyis.db.sql.parser.ast;

/**
 * WHERE boolean tree 中的单个不可变原子谓词节点。
 *
 * <p>Parser 只保留操作符、字面量和源位置，不在语法层推导列类型、索引边界或 SQL NULL
 * 真值；这些职责分别属于 Binder、Optimizer 和 Executor。AND/OR/NOT 由上层 boolean
 * 节点表达，当前原子集合包含 comparison、BETWEEN 与 null-test。</p>
 */
public sealed interface PredicateNode extends BooleanExpressionNode
        permits EqualityPredicateNode, ComparisonPredicateNode,
        BetweenPredicateNode, NullTestPredicateNode {

    /**
     * 返回谓词左侧的未绑定列名。
     *
     * @return 当前语句源文本中的列标识符；始终非 {@code null}，尚未关联 DD column id
     */
    IdentifierNode column();

    /**
     * 原子谓词的诊断起点就是左侧列 token；操作符和 literal 的位置由各自节点保留。
     *
     * @return 左侧列标识符的稳定源位置
     */
    @Override
    default cn.zhangyis.db.sql.parser.SourcePosition position() {
        return column().position();
    }
}
