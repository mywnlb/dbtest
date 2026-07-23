package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.sql.parser.SourcePosition;

/**
 * WHERE 子句的不可变布尔语法树根。
 *
 * <p>Parser 只表达用户书写的优先级、组合关系和源位置，不解析列身份、DD 类型、
 * SQL 三值结果或索引范围。Binder 必须递归消费该封闭集合，新增节点时不能通过默认
 * 分支静默忽略。</p>
 */
public sealed interface BooleanExpressionNode permits PredicateNode,
        ConjunctionExpressionNode, DisjunctionExpressionNode,
        NegationExpressionNode {

    /**
     * 返回当前表达式第一个有语义 token 的源起始位置。
     *
     * @return 属于同一 SQL 文本的稳定位置；始终非 {@code null}
     */
    SourcePosition position();
}
