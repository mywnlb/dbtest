package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * redo capacity 前台限流等待超时。
 *
 * <p>该异常表示 checkpoint/reclaim boundary 在限定时间内没有前进到安全区间。调用方可在上层回滚当前操作、
 * 报告容量压力或稍后重试；底层 redo 文件环仍保持 fail-closed，不允许覆盖恢复仍需要的日志区间。
 */
public final class RedoCapacityThrottleTimeoutException extends DatabaseRuntimeException {

    /**
     * 创建 {@code RedoCapacityThrottleTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public RedoCapacityThrottleTimeoutException(String message) {
        super(message);
    }

    /**
     * 创建 {@code RedoCapacityThrottleTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public RedoCapacityThrottleTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
