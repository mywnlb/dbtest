package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.List;

/**
 * 一次 crash recovery 的不可变结果快照。它用于启动日志、测试断言和后续 metrics 暴露。
 *
 * @param mode 恢复模式。
 * @param state 恢复结束状态。
 * @param checkpointLsn 本次恢复使用的 checkpoint LSN。
 * @param recoveredToLsn redo reader 扫描到的最后完整 LSN。
 * @param repairedPageCount doublewrite 实际修复页数。
 * @param appliedBatchCount 交给 redo dispatcher 的批次数。
 * @param completedStages 已完成阶段顺序。
 */
public record RecoveryReport(RecoveryMode mode,
                             RecoveryState state,
                             Lsn checkpointLsn,
                             Lsn recoveredToLsn,
                             int repairedPageCount,
                             int appliedBatchCount,
                             List<RecoveryStageName> completedStages) {

    public RecoveryReport {
        if (mode == null || state == null || checkpointLsn == null
                || recoveredToLsn == null || completedStages == null) {
            throw new DatabaseValidationException("recovery report fields must not be null");
        }
        if (repairedPageCount < 0 || appliedBatchCount < 0) {
            throw new DatabaseValidationException("recovery report counts must not be negative");
        }
        completedStages = List.copyOf(completedStages);
    }
}
