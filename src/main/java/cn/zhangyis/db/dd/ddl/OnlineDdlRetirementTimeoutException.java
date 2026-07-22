package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** target DD已经发布但旧物理资源尚未越过retirement fence；只能保留descriptor并由前滚恢复续作。 */
public final class OnlineDdlRetirementTimeoutException extends DatabaseRuntimeException {

    /** @param message 包含DDL/table/index或source version的前滚诊断 */
    public OnlineDdlRetirementTimeoutException(String message) {
        super(message);
    }

    /**
     * @param message 包含DDL/table/index或source version的前滚诊断
     * @param cause history/pin等待报告的原始领域异常
     */
    public OnlineDdlRetirementTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
