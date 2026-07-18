package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * page3 交叉校验结果：final snapshot 恢复 counter，ACTIVE 列表驱动 rollback，PREPARED 列表驱动 XA 决议，
 * COMMITTED 列表驱动 history rebuild。所有列表都为不可变值，不持有页 guard 或 undo segment 句柄。
 *
 * @param snapshot 合并 page3 后的事务恢复快照。
 * @param activeSlots 合法 RECOVERED_ACTIVE slot。
 * @param preparedSlots 合法 PREPARED slot；按恢复编排在 gate 关闭期间消费决议。
 * @param committedSlots 合法 COMMITTED history 输入。
 */
public record RecoveredTransactionReconciliation(RecoveredTransactionSnapshot snapshot,
                                                 List<RecoveredUndoSlotEvidence> activeSlots,
                                                 List<RecoveredUndoSlotEvidence> preparedSlots,
                                                 List<RecoveredUndoSlotEvidence> committedSlots) {

    public RecoveredTransactionReconciliation {
        if (snapshot == null || activeSlots == null || preparedSlots == null || committedSlots == null) {
            throw new DatabaseValidationException("transaction reconciliation fields must not be null");
        }
        activeSlots = List.copyOf(activeSlots);
        preparedSlots = List.copyOf(preparedSlots);
        committedSlots = List.copyOf(committedSlots);
    }
}
