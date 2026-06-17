package cn.zhangyis.db.common.exception;

/**
 * 数据库领域参数校验异常。用于替代生产代码中的 IllegalArgumentException，统一非法配置和值对象边界错误。
 */
public class DatabaseValidationException extends DatabaseRuntimeException {

    /**
     * 创建领域参数校验异常。
     *
     * @param message 描述非法参数及其违反的数据库领域约束。
     */
    public DatabaseValidationException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的领域参数校验异常。
     *
     * @param message 描述非法参数及其违反的数据库领域约束。
     * @param cause 触发校验失败的底层异常。
     */
    public DatabaseValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
