package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexChangeLog;

/**
 * Online DDL 的有界资源配置。
 *
 * @param maxRowLogBytes 单个 online index row-log 文件的最大字节数；包含 header、manifest、candidate、
 *                       force watermark 与 terminal state，必须大于 {@code abortReserveBytes}
 * @param scanBatchRows 每个聚簇 continuation scan 批次最多物化的 live row 数；正值保证页资源能在批次间释放
 * @param abortReserveBytes candidate 不得消费的 terminal 预留；容量耗尽后仍必须先 force 最后 candidate，
 *                          再持久发布 {@code ABORT_REQUIRED}
 */
public record OnlineDdlConfig(long maxRowLogBytes, int scanBatchRows, int abortReserveBytes) {

    /** 教学实现默认允许每条 online build 使用 128 MiB 持久变化日志。 */
    private static final long DEFAULT_MAX_ROW_LOG_BYTES = 128L * 1024 * 1024;
    /** 与既有 blocking ALTER continuation 一致的默认有界扫描批次。 */
    private static final int DEFAULT_SCAN_BATCH_ROWS = 256;
    /** 服务最后一次 force watermark 与 terminal abort frame 的独立空间，普通 candidate 不得占用。 */
    private static final int DEFAULT_ABORT_RESERVE_BYTES = 4 * 1024;

    /**
     * 校验容量组合，保证正常记录和失败终态各有独立空间。
     *
     * @throws DatabaseValidationException 容量、批次或预留不能形成有效有界配置时抛出
     */
    public OnlineDdlConfig {
        if (maxRowLogBytes <= 0 || scanBatchRows <= 0
                || abortReserveBytes < OnlineIndexChangeLog.MIN_TERMINAL_RESERVE_BYTES
                || maxRowLogBytes <= abortReserveBytes) {
            throw new DatabaseValidationException(
                    "online DDL config requires positive scan, terminal reserve of at least "
                            + OnlineIndexChangeLog.MIN_TERMINAL_RESERVE_BYTES
                            + " bytes, and max row log greater than reserve");
        }
    }

    /** @return 新实例使用的稳定默认 Online DDL 配置。 */
    public static OnlineDdlConfig defaults() {
        return new OnlineDdlConfig(DEFAULT_MAX_ROW_LOG_BYTES,
                DEFAULT_SCAN_BATCH_ROWS, DEFAULT_ABORT_RESERVE_BYTES);
    }
}
