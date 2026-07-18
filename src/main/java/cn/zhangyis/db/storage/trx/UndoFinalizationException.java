package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * undo segment finalization 已进入物理 FSP/page3 写阶段后发生的致命错误。MTR rollback 不撤销 buffer 字节，
 * 因而调用方不得在同一进程重试；应停止相关 worker/请求并通过 crash recovery 重新建立权威状态。
 */
public final class UndoFinalizationException extends DatabaseFatalException {

    /** 创建只包含 fail-stop 诊断的异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UndoFinalizationException(String message) {
        super(message);
    }

    /** 创建并保留导致 finalization 结果不确定的根因。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public UndoFinalizationException(String message, Throwable cause) {
        super(message, cause);
    }
}
