package cn.zhangyis.db.sql.binder.exception;

/** SQL 标识符在 exact TableDefinition 中不存在。 */
public final class UnknownColumnException extends SqlBindingException {
    /**
     * 创建 {@code UnknownColumnException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UnknownColumnException(String message) { super(message); }
}
