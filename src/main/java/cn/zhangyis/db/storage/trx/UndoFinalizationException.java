package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * undo segment finalization 已进入物理 FSP/page3 写阶段后发生的致命错误。MTR rollback 不撤销 buffer 字节，
 * 因而调用方不得在同一进程重试；应停止相关 worker/请求并通过 crash recovery 重新建立权威状态。
 */
public final class UndoFinalizationException extends DatabaseFatalException {

    /** 创建只包含 fail-stop 诊断的异常。 */
    public UndoFinalizationException(String message) {
        super(message);
    }

    /** 创建并保留导致 finalization 结果不确定的根因。 */
    public UndoFinalizationException(String message, Throwable cause) {
        super(message, cause);
    }
}
