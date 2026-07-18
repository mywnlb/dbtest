package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * row-lock wait-for graph 检测到环。当前切片采用“当前等待请求作为 victim”的简化策略：
 * LockManager 只移除该等待请求并抛异常，不自动驱动 TransactionManager rollback。
 */
public class DeadlockDetectedException extends DatabaseRuntimeException {

    /**
     * 创建 {@code DeadlockDetectedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public DeadlockDetectedException(String message) {
        super(message);
    }

    /**
     * 创建 {@code DeadlockDetectedException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public DeadlockDetectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
