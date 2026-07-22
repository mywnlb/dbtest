package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** coordinator观察到已接受取消并进入正常回滚路径；不表示实例fatal。 */
public final class OnlineDdlCancellationException extends DatabaseRuntimeException {
    /** @param message 包含DDL identity与安全检查点的诊断 */
    public OnlineDdlCancellationException(String message) {
        super(message);
    }

    /**
     * @param message 包含 DDL identity 与安全检查点的诊断
     * @param cause 唤醒 final MDL/gate 等待时保留的底层领域异常
     */
    public OnlineDdlCancellationException(String message, Throwable cause) {
        super(message, cause);
    }
}
