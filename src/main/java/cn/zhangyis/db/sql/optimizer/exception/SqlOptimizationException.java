package cn.zhangyis.db.sql.optimizer.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 逻辑计划无法在当前规则集合中转换为安全物理计划时抛出的领域异常。
 */
public final class SqlOptimizationException extends DatabaseRuntimeException {

    /**
     * 创建带诊断上下文的优化异常。
     *
     * @param message 失败的关系形状、元数据或访问路径约束
     */
    public SqlOptimizationException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的优化异常。
     *
     * @param message 失败的关系形状、元数据或访问路径约束
     * @param cause 原始异常；不得丢失，以便调用方定位损坏元数据或规则错误
     */
    public SqlOptimizationException(String message, Throwable cause) {
        super(message, cause);
    }
}
