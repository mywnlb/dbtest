package cn.zhangyis.db.storage.flush.policy;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoCapacityDecision;
import cn.zhangyis.db.storage.redo.RedoCapacityPressure;

/**
 * 简化 adaptive flush 策略。R2 已经把 checkpoint age 分为 capacity pressure，F2 先把压力稳定映射为批量大小；
 * 后续再引入 redo generation rate、实际 flush rate 和 dirty ratio 时，可替换该策略而不改变 FlushService API。
 */
public final class AdaptiveFlushPolicy {

    /** 压力刚出现时的最小刷页数。 */
    private final int minBatchPages;
    /** hard pressure 下单轮允许刷出的最大页数。 */
    private final int maxBatchPages;

    private AdaptiveFlushPolicy(int minBatchPages, int maxBatchPages) {
        this.minBatchPages = minBatchPages;
        this.maxBatchPages = maxBatchPages;
    }

    /**
     * 创建固定批量策略。
     *
     * @param minBatchPages ASYNC_FLUSH 时的最小 batch。
     * @param maxBatchPages HARD_LIMIT 时的最大 batch。
     * @return adaptive flush policy。
     */
    public static AdaptiveFlushPolicy fixed(int minBatchPages, int maxBatchPages) {
        if (minBatchPages < 1) {
            throw new DatabaseValidationException("adaptive flush min batch must be >= 1: " + minBatchPages);
        }
        if (maxBatchPages < minBatchPages) {
            throw new DatabaseValidationException("adaptive flush max batch must be >= min batch: "
                    + maxBatchPages + " < " + minBatchPages);
        }
        return new AdaptiveFlushPolicy(minBatchPages, maxBatchPages);
    }

    /**
     * 根据 redo capacity decision 生成本轮刷脏建议。
     *
     * @param decision redo capacity pressure 判断。
     * @param requestMaxPages 调用方允许本轮最多刷出的页数。
     * @return flush advice。
     */
    public FlushAdvice plan(RedoCapacityDecision decision, int requestMaxPages) {
        if (decision == null) {
            throw new DatabaseValidationException("redo capacity decision must not be null");
        }
        if (requestMaxPages < 0) {
            throw new DatabaseValidationException("requested max pages must not be negative: " + requestMaxPages);
        }
        int planned = switch (decision.pressure()) {
            case NONE -> 0;
            case ASYNC_FLUSH -> minBatchPages;
            case SYNC_FLUSH -> (minBatchPages + maxBatchPages) / 2;
            case HARD_LIMIT -> maxBatchPages;
        };
        int pages = Math.min(planned, requestMaxPages);
        boolean sync = decision.pressure() == RedoCapacityPressure.SYNC_FLUSH
                || decision.pressure() == RedoCapacityPressure.HARD_LIMIT;
        return new FlushAdvice(decision.targetCheckpointLsn(), pages, sync);
    }
}
