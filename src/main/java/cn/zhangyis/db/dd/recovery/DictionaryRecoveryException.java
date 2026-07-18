package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/** DD/物理绑定不一致或 DDL 恢复无法安全续作时的 fail-closed 异常。 */
public final class DictionaryRecoveryException extends DatabaseFatalException {
    /**
     * 创建 {@code DictionaryRecoveryException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public DictionaryRecoveryException(String message) {
        super(message);
    }

    /**
     * 创建 {@code DictionaryRecoveryException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public DictionaryRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
