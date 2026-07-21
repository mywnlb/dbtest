package cn.zhangyis.db.sql.parser.ast;

/**
 * WHERE conjunction 中的单个不可变谓词节点。
 *
 * <p>Parser 只保留操作符、字面量和源位置，不在语法层推导列类型、索引边界或 SQL NULL
 * 真值；这些职责分别属于 Binder 和 Executor。当前封闭集合故意只包含 comparison 与
 * BETWEEN，避免未实现的 OR/IN/LIKE 被误认为已有执行语义。</p>
 */
public sealed interface PredicateNode permits EqualityPredicateNode, ComparisonPredicateNode,
        BetweenPredicateNode {

    /**
     * 返回谓词左侧的未绑定列名。
     *
     * @return 当前语句源文本中的列标识符；始终非 {@code null}，尚未关联 DD column id
     */
    IdentifierNode column();
}
