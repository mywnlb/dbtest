package cn.zhangyis.db.engine.xa;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Server XA 目录、branch 状态或 phase 编排失败。该异常表示调用方可依据持久 registry
 * 重试同一决议、使用离线 maintenance 裁决，或关闭实例后重新恢复。
 */
public final class XaException extends DatabaseRuntimeException {

    /**
     * 创建只含 XA 领域消息的异常。
     *
     * @param message XID、registry 状态或 phase 边界的诊断信息
     */
    public XaException(String message) {
        super(message);
    }

    /**
     * 创建保留 IO、storage 或状态机根因的 XA 异常。
     *
     * @param message XID、registry 状态或 phase 边界的诊断信息
     * @param cause 原始失败；不得丢弃
     */
    public XaException(String message, Throwable cause) {
        super(message, cause);
    }
}
