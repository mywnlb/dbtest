package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * page3 rollback-segment slot 已有持久 owner，无法按预期认领空槽。该异常表示运行期槽投影与磁盘权威状态冲突；
 * 首写编排必须在分配 undo segment 前结束 RESERVED 租约，不能覆盖旧 owner 或留下新 orphan segment。
 */
public final class UndoSlotOwnershipConflictException extends DatabaseRuntimeException {

    /** 创建带领域上下文的 owner 冲突。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UndoSlotOwnershipConflictException(String message) {
        super(message);
    }

    /** 包装读取/校验持久 owner 时的底层根因。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public UndoSlotOwnershipConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
