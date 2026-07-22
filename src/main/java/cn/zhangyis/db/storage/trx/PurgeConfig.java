package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;

/**
 * 多 worker purge 的不可变资源边界。
 *
 * @param workerCount      后台 purge 执行线程数；只控制完整表级协调器，legacy 单索引路径仍保持串行
 * @param maxInFlightLogs  单批最多建立依赖并保留结果的 history log 数，防止 blocked head 后方无限投机
 * @param batchTimeout     dispatcher 等待本批 worker 全部到达稳定结果的上限；超时后该 pool fail-stop
 */
public record PurgeConfig(int workerCount, int maxInFlightLogs, Duration batchTimeout) {

    /** 教学实现与 MySQL 风格默认并发之间的固定折中。 */
    private static final int DEFAULT_WORKER_COUNT = 4;
    /** 默认只在物理 head 后方保留四倍 worker 数量的有界窗口。 */
    private static final int DEFAULT_MAX_IN_FLIGHT_LOGS = 16;
    /** 默认批次上限；超时不是跳过，而是停止 pool 以避免未知在途写与下一批重叠。 */
    private static final Duration DEFAULT_BATCH_TIMEOUT = Duration.ofSeconds(5);
    /** 防止错误配置在一个教学实例中创建过多平台线程。 */
    private static final int MAX_WORKER_COUNT = 32;
    /** 防止单批依赖图和 completion stage 无界占用内存。 */
    private static final int MAX_IN_FLIGHT_LOGS = 4096;

    /**
     * 校验线程、窗口和等待预算；校验发生在线程池创建之前。
     *
     * @throws DatabaseValidationException 任一资源边界为空、非正或超过固定安全上限时抛出
     */
    public PurgeConfig {
        if (workerCount < 1 || workerCount > MAX_WORKER_COUNT) {
            throw new DatabaseValidationException(
                    "purge workerCount must be in [1," + MAX_WORKER_COUNT + "]: " + workerCount);
        }
        if (maxInFlightLogs < 1 || maxInFlightLogs > MAX_IN_FLIGHT_LOGS) {
            throw new DatabaseValidationException(
                    "purge maxInFlightLogs must be in [1," + MAX_IN_FLIGHT_LOGS + "]: "
                            + maxInFlightLogs);
        }
        if (batchTimeout == null || batchTimeout.isZero() || batchTimeout.isNegative()) {
            throw new DatabaseValidationException("purge batchTimeout must be positive");
        }
    }

    /**
     * 返回生产默认值；每次返回新的不可变 record，不共享运行时状态。
     *
     * @return 四 worker、十六条在途日志和五秒批次上限
     */
    public static PurgeConfig defaults() {
        return new PurgeConfig(DEFAULT_WORKER_COUNT, DEFAULT_MAX_IN_FLIGHT_LOGS, DEFAULT_BATCH_TIMEOUT);
    }
}
