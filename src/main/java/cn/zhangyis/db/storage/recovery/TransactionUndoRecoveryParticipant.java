package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;

/**
 * 事务 undo 恢复阶段参与者。CrashRecoveryService 只负责恢复阶段顺序和流量门控，不直接理解 rollback segment、
 * undo page 或 B+Tree；具体扫描 page3、回滚 ACTIVE 段、重建 committed history 的逻辑由引擎组合根提供。
 *
 * <p>调用时机固定在 redo replay、UNDO tablespace truncate 续作和 space file reconcile 之后，OPEN_TRAFFIC 之前。
 * 因此实现可以假设物理页已经恢复到 checkpoint 后的最新状态，且普通用户事务尚未进入存储引擎。
 */
@FunctionalInterface
public interface TransactionUndoRecoveryParticipant {

    /**
     * 执行正式 UNDO_ROLLBACK / RESUME_PURGE 恢复阶段。
     *
     * @param recoveredToLsn redo replay 扫描到的连续恢复边界；实现可用于诊断或校验后续 redo 续写边界。
     * @return 本次恢复阶段摘要；不能返回 null。
     */
    TransactionUndoRecoveryResult recoverAfterRedo(Lsn recoveredToLsn);
}
