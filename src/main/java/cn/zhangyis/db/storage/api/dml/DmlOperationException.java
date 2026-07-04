package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Storage DML 编排异常。表示 facade 在事务、undo、B+Tree、redo durability 或 recovery gate 编排中发现
 * 可恢复运行时错误；调用方可选择回滚事务、关闭 session 或向 SQL 层报告失败。
 */
public class DmlOperationException extends DatabaseRuntimeException {

    /**
     * 创建只包含领域消息的 DML 编排异常。
     *
     * @param message 面向调用方和日志诊断的错误描述。
     */
    public DmlOperationException(String message) {
        super(message);
    }

    /**
     * 创建保留底层根因的 DML 编排异常。
     *
     * @param message 面向调用方和日志诊断的错误描述。
     * @param cause   触发失败的底层异常。
     */
    public DmlOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
