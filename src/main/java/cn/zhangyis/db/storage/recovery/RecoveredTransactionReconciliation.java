package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * page3 交叉校验结果：final snapshot 恢复 counter，ACTIVE 列表驱动 rollback，COMMITTED 列表驱动 history rebuild。
 * 三者都为不可变值，不持有页 guard 或 undo segment 句柄。
 *
 * @param snapshot 合并 page3 后的事务恢复快照。
 * @param activeSlots 合法 RECOVERED_ACTIVE slot。
 * @param committedSlots 合法 COMMITTED history 输入。
 */
public record RecoveredTransactionReconciliation(RecoveredTransactionSnapshot snapshot,
                                                 List<RecoveredUndoSlotEvidence> activeSlots,
                                                 List<RecoveredUndoSlotEvidence> committedSlots) {

    public RecoveredTransactionReconciliation {
        if (snapshot == null || activeSlots == null || committedSlots == null) {
            throw new DatabaseValidationException("transaction reconciliation fields must not be null");
        }
        activeSlots = List.copyOf(activeSlots);
        committedSlots = List.copyOf(committedSlots);
    }
}
