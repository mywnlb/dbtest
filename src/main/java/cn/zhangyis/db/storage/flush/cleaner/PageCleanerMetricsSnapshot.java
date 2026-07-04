package cn.zhangyis.db.storage.flush.cleaner;

/**
 * Page cleaner supervisor 汇总的只读 metrics 快照。
 *
 * <p>该快照跨 worker 生命周期保留历史计数：worker 因失败被重建后，supervisor 仍继续累加成功/失败/restart 计数。
 * 第一阶段只服务测试与恢复诊断，不承诺 Performance Schema 兼容格式。
 *
 * @param state supervisor 视角下的当前后台 flush 状态。
 * @param restartCount 已执行的 worker 重启次数。
 * @param successfulCycles 跨 worker 累计成功 flush cycle 数。
 * @param failedCycles 跨 worker 累计失败 cycle 数。
 * @param lastCyclePresent 当前或历史 worker 是否已有成功 cycle。
 * @param lastErrorMessage 最近失败消息；无失败时为空字符串。
 * @param lastStartedAtMillis 最近一次 worker 启动时间，epoch millis；未启动为 0。
 * @param lastStoppedAtMillis 最近一次 supervisor/worker 停止时间，epoch millis；未停止为 0。
 */
public record PageCleanerMetricsSnapshot(PageCleanerState state,
                                         int restartCount,
                                         long successfulCycles,
                                         long failedCycles,
                                         boolean lastCyclePresent,
                                         String lastErrorMessage,
                                         long lastStartedAtMillis,
                                         long lastStoppedAtMillis) {
}
