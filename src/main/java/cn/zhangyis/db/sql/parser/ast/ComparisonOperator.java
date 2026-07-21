package cn.zhangyis.db.sql.parser.ast;

/**
 * SQL 二元比较操作符。等值继续由 {@link EqualityPredicateNode} 表达以保持既有 AST
 * 兼容性，其余四个操作符由 {@link ComparisonPredicateNode} 携带。
 */
public enum ComparisonOperator {
    /** 严格小于，形成开放上界。 */
    LESS_THAN,
    /** 小于等于，形成闭合上界。 */
    LESS_THAN_OR_EQUAL,
    /** 严格大于，形成开放下界。 */
    GREATER_THAN,
    /** 大于等于，形成闭合下界。 */
    GREATER_THAN_OR_EQUAL
}
