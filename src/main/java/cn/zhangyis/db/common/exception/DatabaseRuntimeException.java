package cn.zhangyis.db.common.exception;

/**
 * 数据库运行时异常基类。表示调用方仍可通过重试、回滚、关闭资源或报告错误继续运行的领域错误。
 */
public class DatabaseRuntimeException extends RuntimeException {

    /**
     * 创建只包含领域错误消息的运行时异常。
     *
     * @param message 面向调用方和日志诊断的领域错误描述。
     */
    public DatabaseRuntimeException(String message) {
        super(message);
    }

    /**
     * 创建保留底层根因的运行时异常，包装 IO、状态机或第三方库异常时必须使用该构造器。
     *
     * @param message 面向调用方和日志诊断的领域错误描述。
     * @param cause 触发该领域错误的原始异常。
     */
    public DatabaseRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
