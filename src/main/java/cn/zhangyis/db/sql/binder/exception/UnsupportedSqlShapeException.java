package cn.zhangyis.db.sql.binder.exception;

/** 语法合法但超出 primary-point v1 确定性执行范围。 */
public final class UnsupportedSqlShapeException extends SqlBindingException {
    /**
     * 创建 {@code UnsupportedSqlShapeException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UnsupportedSqlShapeException(String message) { super(message); }

    /**
     * 创建保留根因的 unsupported-shape 异常，供 enum/type 等标准库解析失败转换为 SQL 领域错误。
     *
     * @param message 包含不受支持语法形状的诊断
     * @param cause 原始解析或范围异常；不得丢失
     */
    public UnsupportedSqlShapeException(String message, Throwable cause) {
        super(message, cause);
    }
}
