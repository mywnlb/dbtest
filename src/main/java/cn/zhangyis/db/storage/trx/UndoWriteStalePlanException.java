package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** Undo 规划与执行之间事务链或持久尾页发生变化；异常发生在首次物理修改前，可由上层重新规划。 */
public final class UndoWriteStalePlanException extends DatabaseRuntimeException {

    /** 创建可通过重新规划解决的 stale 异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UndoWriteStalePlanException(String message) {
        super(message);
    }

    /** 创建保留持久快照读取根因的 stale 异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public UndoWriteStalePlanException(String message, Throwable cause) {
        super(message, cause);
    }
}
