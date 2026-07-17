package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一次 {@link PurgeCoordinator#runBatch} 的结果统计。
 *
 * @param purgedLogs               本批完成并从 history 摘除的 committed update undo log 数。
 * @param removedClusteredRecords  本批物理移除的 delete-marked 聚簇记录数（stale 跳过的不计入）。
 * @param removedSecondaryEntries  本批物理移除的 delete-marked secondary entry 数（RETAIN/ABSENT 不计入）。
 * @param deferredLogs             因 row guard busy 而保留在 history head 的日志数；单批当前只可能为 0 或 1。
 */
public record PurgeSummary(int purgedLogs, int removedClusteredRecords,
                           int removedSecondaryEntries, int deferredLogs) {

    /**
     * 校验 purge 批次统计不会出现负值；统计只描述已观察结果，不反向驱动 history 状态。
     *
     * @param purgedLogs              已完成 finalization 并从 history 摘除的日志数。
     * @param removedClusteredRecords 已物理移除的聚簇 delete-marked 记录数。
     * @param removedSecondaryEntries 已物理移除的二级 delete-marked entry 数。
     * @param deferredLogs            因 row guard busy 保留在队首的日志数。
     * @throws DatabaseValidationException 任一计数为负时抛出。
     */
    public PurgeSummary {
        if (purgedLogs < 0 || removedClusteredRecords < 0
                || removedSecondaryEntries < 0 || deferredLogs < 0) {
            throw new DatabaseValidationException("purge summary counts must be non-negative");
        }
    }
}
