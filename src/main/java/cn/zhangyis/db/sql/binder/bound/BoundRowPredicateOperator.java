package cn.zhangyis.db.sql.binder.bound;

/**
 * Binder 已完成类型转换的行谓词操作符。该枚举只服务 SQL residual evaluation，
 * storage B+Tree 边界由 {@link BoundIndexRange} 单独表达。
 */
public enum BoundRowPredicateOperator {
    /** SQL 等值比较。 */
    EQUAL,
    /** 严格小于。 */
    LESS_THAN,
    /** 小于等于。 */
    LESS_THAN_OR_EQUAL,
    /** 严格大于。 */
    GREATER_THAN,
    /** 大于等于。 */
    GREATER_THAN_OR_EQUAL
}
