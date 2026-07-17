package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;

/**
 * 事务 undo 恢复阶段参与者。CrashRecoveryService 只负责恢复阶段顺序和流量门控，不直接理解 rollback segment、
 * undo page 或 B+Tree；具体扫描 page3、回滚 ACTIVE 段、重建 committed history 和恢复期推进 purge 的逻辑由引擎组合根提供。
 *
 * <p>调用时机固定在 redo replay、UNDO tablespace truncate 续作和 space file reconcile 之后，OPEN_TRAFFIC 之前。
 * 因此实现可以假设物理页已经恢复到 checkpoint 后的最新状态，且普通用户事务尚未进入存储引擎。
 */
@FunctionalInterface
public interface TransactionUndoRecoveryParticipant {

    /**
     * 执行正式 UNDO_ROLLBACK 阶段，并恢复 committed history 的运行时投影。
     *
     * @param recoveredToLsn redo replay 扫描到的连续恢复边界；实现可用于诊断或校验后续 redo 续写边界。
     * @param transactionSnapshot checkpoint 基线与 post-checkpoint redo 合并后的不可变事务证据。
     * @return 本次恢复阶段摘要；不能返回 null。
     */
    TransactionUndoRecoveryResult recoverAfterRedo(
            Lsn recoveredToLsn, RecoveredTransactionSnapshot transactionSnapshot);

    /**
     * 执行正式 RESUME_PURGE 阶段。调用时 committed history 已经恢复、ACTIVE 事务已经回滚，且用户流量仍被关闭；
     * 实现可以推进当前安全 purge boundary，但不得在本方法内开放流量或启动后台 worker。
     *
     * <p>默认实现为兼容不持有 persistent history 的低层恢复参与者。生产组合根一旦恢复出 committed history，必须覆盖本方法
     * 并调用真实 purge 协调器；失败应直接抛出项目异常，由恢复总控保持 fail-closed。</p>
     */
    default void resumePurgeAfterRedo() {
        // 低层无 persistent history 的参与者没有可恢复工作。
    }
}
