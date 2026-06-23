package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一次 {@link PurgeCoordinator#runBatch} 的结果统计。
 *
 * @param purgedLogs              本批回收的 committed（update/delete）undo log 数。
 * @param removedRecords          本批物理移除的 delete-marked 聚簇记录数（stale 跳过的不计入）。
 * @param reclaimedInsertSegments 本批回收的纯 insert undo 段数。
 */
public record PurgeSummary(int purgedLogs, int removedRecords, int reclaimedInsertSegments) {

    public PurgeSummary {
        if (purgedLogs < 0 || removedRecords < 0 || reclaimedInsertSegments < 0) {
            throw new DatabaseValidationException("purge summary counts must be non-negative");
        }
    }
}
