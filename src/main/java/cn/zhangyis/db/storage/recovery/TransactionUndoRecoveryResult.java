package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务 undo 恢复阶段摘要。该值对象只描述恢复阶段实际处理的持久 undo/rseg 事实，用于启动诊断与阶段报告；
 * 具体 undo 记录条数仍由 RollbackService / PurgeCoordinator 各自负责。
 *
 * @param restoredSlots 从 rollback segment header page3 恢复出的占用 slot 数量。
 * @param rolledBackActiveSlots 已执行 recovery rollback 并释放内存 slot 的 ACTIVE 段数量。
 * @param skippedActiveSlots 为 force-recovery 扩展保留的诊断字段；当前生产链不允许跳过 ACTIVE，固定为 0。
 * @param rebuiltHistoryEntries 从 COMMITTED undo header 重建并提交到内存 history list 的条目数量。
 */
public record TransactionUndoRecoveryResult(int restoredSlots,
                                            int rolledBackActiveSlots,
                                            int skippedActiveSlots,
                                            int rebuiltHistoryEntries) {

    public TransactionUndoRecoveryResult {
        if (restoredSlots < 0 || rolledBackActiveSlots < 0
                || skippedActiveSlots < 0 || rebuiltHistoryEntries < 0) {
            throw new DatabaseValidationException("transaction undo recovery counters must not be negative");
        }
        if (rolledBackActiveSlots + skippedActiveSlots > restoredSlots) {
            throw new DatabaseValidationException("active slot counters exceed restored slots: restored="
                    + restoredSlots + ", rolledBack=" + rolledBackActiveSlots + ", skipped=" + skippedActiveSlots);
        }
        if (rolledBackActiveSlots + skippedActiveSlots + rebuiltHistoryEntries > restoredSlots) {
            throw new DatabaseValidationException("transaction undo recovery counters exceed restored slots: restored="
                    + restoredSlots + ", rolledBack=" + rolledBackActiveSlots
                    + ", skipped=" + skippedActiveSlots + ", history=" + rebuiltHistoryEntries);
        }
    }
}
