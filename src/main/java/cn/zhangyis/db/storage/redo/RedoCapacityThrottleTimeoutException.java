package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * redo capacity 前台限流等待超时。
 *
 * <p>该异常表示 checkpoint/reclaim boundary 在限定时间内没有前进到安全区间。调用方可在上层回滚当前操作、
 * 报告容量压力或稍后重试；底层 redo 文件环仍保持 fail-closed，不允许覆盖恢复仍需要的日志区间。
 */
public final class RedoCapacityThrottleTimeoutException extends DatabaseRuntimeException {

    public RedoCapacityThrottleTimeoutException(String message) {
        super(message);
    }

    public RedoCapacityThrottleTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
