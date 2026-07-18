package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 事务恢复证据缺失、互相冲突或 sidecar IO 失败时的致命异常。
 *
 * <p>这类错误意味着无法证明事务 id/no 不会复用，或无法确定崩溃事务应提交还是回滚；启动必须 fail closed。
 */
public class TransactionRecoveryException extends DatabaseFatalException {

    /** 创建只带事务恢复诊断上下文的致命异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public TransactionRecoveryException(String message) {
        super(message);
    }

    /** 创建并保留底层 IO/编码根因的致命异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public TransactionRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
