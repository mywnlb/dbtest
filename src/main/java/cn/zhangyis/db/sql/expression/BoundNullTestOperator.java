package cn.zhangyis.db.sql.expression;

/**
 * Binder 输出的 typed NULL 检查操作符。
 */
public enum BoundNullTestOperator {
    /** operand 为 SQL NULL 时 TRUE。 */
    IS_NULL,
    /** operand 不为 SQL NULL 时 TRUE。 */
    IS_NOT_NULL
}
