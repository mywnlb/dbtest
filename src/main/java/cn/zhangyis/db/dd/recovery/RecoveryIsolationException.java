package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/** 强制恢复无法把管理员 SpaceId 唯一、稳定地归属到 committed DD 对象时的 fail-closed 异常。 */
public final class RecoveryIsolationException extends DatabaseFatalException {

    /**
     * 创建带完整对象/空间诊断的隔离异常。
     *
     * @param message 无法证明隔离安全性的具体原因；不得用模糊占位文本替代
     */
    public RecoveryIsolationException(String message) {
        super(message);
    }

    /**
     * 包装 catalog、control 或路径检查的底层失败并保留根因。
     *
     * @param message 隔离阶段及目标对象上下文
     * @param cause 原始持久化或校验失败
     */
    public RecoveryIsolationException(String message, Throwable cause) {
        super(message, cause);
    }
}
