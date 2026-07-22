package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** Online DDL control/terminal有界等待超时；durable结果未被伪造，调用方可重新查询。 */
public final class OnlineDdlControlTimeoutException extends DatabaseRuntimeException {

    /**
     * @param message 包含DDL identity和等待边界的诊断消息
     */
    public OnlineDdlControlTimeoutException(String message) {
        super(message);
    }

    /**
     * @param message 包含DDL identity和等待边界的诊断消息
     * @param cause 中断等原始等待失败
     */
    public OnlineDdlControlTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
