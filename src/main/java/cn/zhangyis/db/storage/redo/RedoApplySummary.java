package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * redo apply 批次级诊断摘要。crash recovery 用它把“扫描了多少批次、真正交给 handler 的批次、
 * 因 FORCE_SKIP_CORRUPT_TABLESPACE 被丢弃的物理记录数”写入最终恢复报告。
 *
 * @param scannedBatchCount recovery reader 交给 dispatcher 检查的 batch 数；即使 batch 内记录全被 skip 也计入。
 * @param appliedBatchCount 至少有一条记录被交给任一 handler 重放的 batch 数。
 * @param skippedRecordCount 在访问 PageStore 前按 PageId 过滤掉的 redo record 数。
 */
public record RedoApplySummary(int scannedBatchCount, int appliedBatchCount, int skippedRecordCount) {

    public RedoApplySummary {
        if (scannedBatchCount < 0 || appliedBatchCount < 0 || skippedRecordCount < 0) {
            throw new DatabaseValidationException("redo apply summary counts must be non-negative");
        }
        if (appliedBatchCount > scannedBatchCount) {
            throw new DatabaseValidationException("redo applied batch count must not exceed scanned batch count");
        }
    }

    /** 空摘要，用于没有 redo batch 或阶段被跳过时保持报告字段语义明确。 */
    public static RedoApplySummary empty() {
        return new RedoApplySummary(0, 0, 0);
    }
}
