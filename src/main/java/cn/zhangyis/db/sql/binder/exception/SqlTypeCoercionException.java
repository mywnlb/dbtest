package cn.zhangyis.db.sql.binder.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** SQL literal 不能无损转换为目标 DD 类型。 */
public final class SqlTypeCoercionException extends DatabaseRuntimeException {
    /**
     * 创建 {@code SqlTypeCoercionException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public SqlTypeCoercionException(String message) { super(message); }
    /**
     * 创建 {@code SqlTypeCoercionException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public SqlTypeCoercionException(String message, Throwable cause) { super(message, cause); }
}
