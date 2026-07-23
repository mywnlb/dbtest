package cn.zhangyis.db.sql.expression;

/**
 * Bound comparison 操作符。该枚举属于语义表达式；物理 range 的开放/闭合方向由独立
 * {@code RangeEndpoint} 表达。
 */
public enum BoundComparisonOperator {
    /** SQL 等值比较。 */
    EQUAL,
    /** 严格小于。 */
    LESS_THAN,
    /** 小于等于。 */
    LESS_THAN_OR_EQUAL,
    /** 严格大于。 */
    GREATER_THAN,
    /** 大于等于。 */
    GREATER_THAN_OR_EQUAL;

    /**
     * 在交换 comparison 左右操作数时返回语义等价操作符。
     *
     * @return EQUAL 不变，小于与大于方向互换并保留开放/闭合属性
     */
    public BoundComparisonOperator reversed() {
        return switch (this) {
            case EQUAL -> EQUAL;
            case LESS_THAN -> GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
            case GREATER_THAN -> LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
        };
    }
}
