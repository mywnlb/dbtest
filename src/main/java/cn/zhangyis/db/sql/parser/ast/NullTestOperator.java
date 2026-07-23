package cn.zhangyis.db.sql.parser.ast;

/**
 * Parser 层显式保留的 NULL 检查操作符。
 *
 * <p>使用枚举而非 boolean，避免 Binder 或诊断层把 negated 标志与 SQL NOT 混淆。</p>
 */
public enum NullTestOperator {
    /** {@code column IS NULL}。 */
    IS_NULL,
    /** {@code column IS NOT NULL}。 */
    IS_NOT_NULL
}
