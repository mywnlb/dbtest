package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;

/**
 * Change Buffer 的不可变资源边界。配置不描述磁盘 identity；SpaceId 0 与固定页属于实例格式，不能由用户改写。
 *
 * @param mode 新 mutation 的允许集合；{@code NONE} 不妨碍合并既有记录
 * @param maxSizePercent Change Buffer 最多占 Buffer Pool 的百分比，合法范围 1..50
 * @param mergeInterval 后台空闲合并 tick 间隔，必须为正
 * @param mergeBatchPages 单轮最多主动载入的不同目标页数，合法范围 1..64
 * @param pageGateTimeout 同目标页 buffer/merge gate 的最大等待时间，必须为正
 * @param stopTimeout 关闭时等待 merge worker 停止的上限，必须为正
 */
public record ChangeBufferConfig(ChangeBufferMode mode, int maxSizePercent,
                                 Duration mergeInterval, int mergeBatchPages,
                                 Duration pageGateTimeout, Duration stopTimeout) {

    /** MySQL 8.0 默认 Change Buffer 上限占 Buffer Pool 的 25%。 */
    public static final int DEFAULT_MAX_SIZE_PERCENT = 25;
    /** MySQL 公开配置允许的最大比例。 */
    public static final int MAX_SIZE_PERCENT = 50;
    /** 单轮后台目标页硬上限；结合每页 64 条上限，把选择快照限制在最多 4096 条 mutation。 */
    public static final int MAX_MERGE_BATCH_PAGES = 64;

    /**
     * 校验完整配置；所有等待必须有界，避免把后台优化变成永久阻塞前台或关闭流程的资源。
     */
    public ChangeBufferConfig {
        if (mode == null || mergeInterval == null || pageGateTimeout == null || stopTimeout == null) {
            throw new DatabaseValidationException("change buffer config fields must not be null");
        }
        if (maxSizePercent < 1 || maxSizePercent > MAX_SIZE_PERCENT) {
            throw new DatabaseValidationException(
                    "change buffer max size percent must be in [1,50]: " + maxSizePercent);
        }
        if (mergeInterval.isZero() || mergeInterval.isNegative()) {
            throw new DatabaseValidationException("change buffer merge interval must be positive: " + mergeInterval);
        }
        if (mergeBatchPages <= 0 || mergeBatchPages > MAX_MERGE_BATCH_PAGES) {
            throw new DatabaseValidationException("change buffer merge batch pages must be in [1,64]: "
                    + mergeBatchPages);
        }
        if (pageGateTimeout.isZero() || pageGateTimeout.isNegative()) {
            throw new DatabaseValidationException("change buffer page gate timeout must be positive: "
                    + pageGateTimeout);
        }
        if (stopTimeout.isZero() || stopTimeout.isNegative()) {
            throw new DatabaseValidationException("change buffer stop timeout must be positive: " + stopTimeout);
        }
    }

    /**
     * 返回新实例的默认策略：功能全开、25% 上限、温和后台批次和全部有界等待。
     *
     * @return 可直接进入 {@code EngineConfig} 的不可变默认值
     */
    public static ChangeBufferConfig defaults() {
        return new ChangeBufferConfig(ChangeBufferMode.ALL, DEFAULT_MAX_SIZE_PERCENT,
                Duration.ofSeconds(1), 16, Duration.ofSeconds(5), Duration.ofSeconds(5));
    }

    /** @return 完全禁止新缓冲、但保留相同资源边界以供 legacy 实例诊断的配置。 */
    public static ChangeBufferConfig disabled() {
        ChangeBufferConfig defaults = defaults();
        return new ChangeBufferConfig(ChangeBufferMode.NONE, defaults.maxSizePercent,
                defaults.mergeInterval, defaults.mergeBatchPages,
                defaults.pageGateTimeout, defaults.stopTimeout);
    }

    /**
     * 按 MySQL {@code innodb_change_buffer_max_size} 的 Buffer Pool 百分比语义计算教学实现的容量上限。
     * 当前每条 pending mutation 保守按一个完整页等价量计费，因此结果是 mutation 数上限而非全局树精确页数；
     * 极小 Buffer Pool 仍允许一个槽位，避免启用模式在合法最小配置下退化为永久 NONE。
     *
     * @param bufferPoolCapacityFrames 当前实例 Buffer Pool 的正 frame 总数
     * @return 至少为 1 的 pending mutation 页等价量上限
     * @throws DatabaseValidationException frame 容量非正时抛出，调用方必须修正引擎配置
     */
    public long maxBufferedPageEquivalents(int bufferPoolCapacityFrames) {
        if (bufferPoolCapacityFrames <= 0) {
            throw new DatabaseValidationException(
                    "change buffer requires a positive buffer pool capacity: " + bufferPoolCapacityFrames);
        }
        return Math.max(1L,
                Math.multiplyExact((long) bufferPoolCapacityFrames, maxSizePercent) / 100L);
    }
}
