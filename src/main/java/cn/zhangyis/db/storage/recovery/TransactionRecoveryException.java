package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 事务恢复证据缺失、互相冲突或 sidecar IO 失败时的致命异常。
 *
 * <p>这类错误意味着无法证明事务 id/no 不会复用，或无法确定崩溃事务应提交还是回滚；启动必须 fail closed。
 */
public class TransactionRecoveryException extends DatabaseFatalException {

    /** 创建只带事务恢复诊断上下文的致命异常。 */
    public TransactionRecoveryException(String message) {
        super(message);
    }

    /** 创建并保留底层 IO/编码根因的致命异常。 */
    public TransactionRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
