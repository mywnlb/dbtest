package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Shadow final X下ReadView/history未在预算内收敛。异常只允许在FORWARD_ONLY之前产生；coordinator必须先
 * 恢复CAPTURING或进入durable abort，不能留下无法解释的SEALED半状态。
 */
public final class OnlineAlterFinalizationTimeoutException extends DatabaseRuntimeException {

    /**
     * 创建无底层根因的final barrier超时。
     *
     * @param message 包含DDL、表及未收敛屏障的诊断消息；不得用于暗示已跨过FORWARD_ONLY
     */
    public OnlineAlterFinalizationTimeoutException(String message) {
        super(message);
    }

    /**
     * 创建保留ReadView、purge或deadline根因的final barrier超时。
     *
     * @param message 包含DDL、表及未收敛屏障的诊断消息
     * @param cause 触发回退CAPTURING/abort的原始领域异常；调用方可据此诊断但不得继续发布target
     */
    public OnlineAlterFinalizationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
