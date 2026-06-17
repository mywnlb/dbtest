package cn.zhangyis.db.common.exception;

/**
 * 数据库致命异常。表示系统继续运行的安全性已被破坏，例如不可恢复的数据损坏或恢复流程无法继续。
 */
public class DatabaseFatalException extends DatabaseRuntimeException {

    /**
     * 创建只包含致命错误消息的异常。
     *
     * @param message 面向调用方和日志诊断的致命错误描述。
     */
    public DatabaseFatalException(String message) {
        super(message);
    }

    /**
     * 创建保留底层根因的致命异常，避免恢复和损坏诊断时丢失原始异常。
     *
     * @param message 面向调用方和日志诊断的致命错误描述。
     * @param cause 触发致命错误的原始异常。
     */
    public DatabaseFatalException(String message, Throwable cause) {
        super(message, cause);
    }
}
