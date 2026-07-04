package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

/**
 * doublewrite recovery 单页结果快照。
 *
 * @param pageId 被检查的 data page。
 * @param outcome 检查结果。
 */
public record DoublewriteRecoveryResult(PageId pageId, DoublewriteRecoveryOutcome outcome) {

    public DoublewriteRecoveryResult {
        if (pageId == null || outcome == null) {
            throw new DatabaseValidationException("doublewrite recovery result page/outcome must not be null");
        }
    }

    /** @return true 表示本次检查实际写回并 force 了 data file。 */
    public boolean repaired() {
        return outcome == DoublewriteRecoveryOutcome.REPAIRED_FROM_COPY;
    }

    /** @return true 表示 detect-only metadata 发现了损坏页，但没有可写回副本。 */
    public boolean detectedOnly() {
        return outcome == DoublewriteRecoveryOutcome.DETECTED_ONLY;
    }
}
