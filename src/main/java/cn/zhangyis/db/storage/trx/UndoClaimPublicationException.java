package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * undo segment 已完成物理创建并绑定运行期 slot，但 page3 owner 或事务 {@link UndoContext} 发布失败。此时 MTR
 * rollback 不会撤销 FSP/undo buffer 字节，slot 又必须保持 ACTIVE 防止误复用，因此调用方不得在同一进程重试；
 * 应停止当前请求/worker，并由 crash recovery 依据 redo 与 page3 权威状态重新建立目录。
 */
public final class UndoClaimPublicationException extends DatabaseFatalException {

    /** 创建只包含 fail-stop 发布诊断的异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UndoClaimPublicationException(String message) {
        super(message);
    }

    /** 创建并保留持久 claim 或 context 发布失败的根因。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public UndoClaimPublicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
