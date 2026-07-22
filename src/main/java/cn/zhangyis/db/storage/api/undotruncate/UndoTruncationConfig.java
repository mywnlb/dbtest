package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;

/**
 * purge 驱动 undo tablespace 自动截断的不可变策略边界。
 *
 * @param enabled 是否允许后台 purge cycle 触发维护；关闭只影响自动调度，不影响恢复续作和显式 truncate
 * @param minReclaimableExtents 物理文件相对持久 initial size 至少增长的 extent 数；候选判断始终使用 page0 真相
 * @param checkInterval 两次后台候选检查的最小间隔；使用单调时钟执行，必须严格为正
 */
public record UndoTruncationConfig(boolean enabled, int minReclaimableExtents, Duration checkInterval) {

    /** 默认回收门槛为一个 extent，使小型教学实例也能观察到完整生命周期。 */
    private static final int DEFAULT_MIN_RECLAIMABLE_EXTENTS = 1;
    /** 默认三十秒冷却，避免空闲实例每个 purge tick 都竞争 lifecycle lease。 */
    private static final Duration DEFAULT_CHECK_INTERVAL = Duration.ofSeconds(30);

    /**
     * 校验阈值和冷却时间；即使当前禁用也保留完整合法策略，后续启用不产生非法状态。
     *
     * @throws DatabaseValidationException extent 非正或冷却为空、零、负数时抛出
     */
    public UndoTruncationConfig {
        if (minReclaimableExtents <= 0) {
            throw new DatabaseValidationException(
                    "undo truncation minReclaimableExtents must be positive: " + minReclaimableExtents);
        }
        if (checkInterval == null || checkInterval.isZero() || checkInterval.isNegative()) {
            throw new DatabaseValidationException("undo truncation checkInterval must be positive");
        }
    }

    /**
     * 返回生产默认策略。
     *
     * @return 默认启用、一个 extent 门槛、三十秒冷却的新不可变配置
     */
    public static UndoTruncationConfig defaults() {
        return new UndoTruncationConfig(true, DEFAULT_MIN_RECLAIMABLE_EXTENTS, DEFAULT_CHECK_INTERVAL);
    }
}
