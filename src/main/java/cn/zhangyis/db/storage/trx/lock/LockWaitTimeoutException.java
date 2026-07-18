package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 事务锁等待超时。LockManager 已在抛出前清理等待队列和 wait-for graph，调用方可选择回滚事务、
 * 释放已持有锁或把错误返回给 SQL/session 层。
 */
public class LockWaitTimeoutException extends DatabaseRuntimeException {

    /**
     * 创建 {@code LockWaitTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public LockWaitTimeoutException(String message) {
        super(message);
    }

    /**
     * 创建 {@code LockWaitTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public LockWaitTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
