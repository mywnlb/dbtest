package cn.zhangyis.db.storage.flush.policy;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** Adaptive flush 的静态调优边界；运行时速率由 {@link FlushRuntimeSnapshot} 提供。
 * @param pageSizeBytes 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param basePages 参与 {@code 构造} 的上界或规格值 {@code basePages}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param minBatchPages 参与 {@code 构造} 的上界或规格值 {@code minBatchPages}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param maxBatchPages 参与 {@code 构造} 的上界或规格值 {@code maxBatchPages}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param ioCapacityPagesPerSecond 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param ioCapacityMaxPagesPerSecond 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param idleFlushPercent 控制 {@code 构造} 触发边界的阈值 {@code idleFlushPercent}；必须非负，百分比不得超过 100，计数阈值不得超过所属资源容量
 * @param lruFreeFrameLowWatermarkPercent 控制 {@code 构造} 触发边界的阈值 {@code lruFreeFrameLowWatermarkPercent}；必须非负，百分比不得超过 100，计数阈值不得超过所属资源容量
 */
public record FlushTuning(int pageSizeBytes, int basePages, int minBatchPages, int maxBatchPages,
                          int ioCapacityPagesPerSecond, int ioCapacityMaxPagesPerSecond,
                          int idleFlushPercent, int lruFreeFrameLowWatermarkPercent) {

    public FlushTuning {
        if (pageSizeBytes < 1 || basePages < 0 || minBatchPages < 1 || maxBatchPages < minBatchPages
                || ioCapacityPagesPerSecond < 1 || ioCapacityMaxPagesPerSecond < ioCapacityPagesPerSecond
                || idleFlushPercent < 0 || idleFlushPercent > 100
                || lruFreeFrameLowWatermarkPercent < 0 || lruFreeFrameLowWatermarkPercent > 100) {
            throw new DatabaseValidationException("invalid adaptive flush tuning");
        }
    }

    /**
     * 根据调用参数构造 {@code defaults} 对应的脏页刷盘与 checkpoint领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param pageSizeBytes 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param maxBatchPages 参与 {@code defaults} 的上界或规格值 {@code maxBatchPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return {@code defaults} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     */
    public static FlushTuning defaults(int pageSizeBytes, int maxBatchPages) {
        return new FlushTuning(pageSizeBytes, 1, 1, maxBatchPages,
                Math.max(1, maxBatchPages), Math.max(1, maxBatchPages * 4), 10, 10);
    }
}
