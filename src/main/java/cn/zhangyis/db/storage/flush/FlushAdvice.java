package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * adaptive flush 策略输出。它只描述本轮应该刷到哪个 LSN、最多刷几页，以及该压力是否可能要求前台同步等待。
 *
 * @param targetLsn flush list 目标 LSN。
 * @param maxPages 本轮最多刷出的页数，0 表示本轮无需刷脏。
 * @param synchronousPressure true 表示 checkpoint age 已达到同步或 hard 水位。
 */
public record FlushAdvice(Lsn targetLsn, int maxPages, boolean synchronousPressure) {

    public FlushAdvice {
        if (targetLsn == null) {
            throw new DatabaseValidationException("flush advice target LSN must not be null");
        }
        if (maxPages < 0) {
            throw new DatabaseValidationException("flush advice max pages must not be negative: " + maxPages);
        }
    }

    /** 本轮是否需要调用 FlushCoordinator 执行刷脏。 */
    public boolean shouldFlush() {
        return maxPages > 0;
    }
}
