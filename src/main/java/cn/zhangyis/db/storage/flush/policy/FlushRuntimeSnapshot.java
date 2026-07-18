package cn.zhangyis.db.storage.flush.policy;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 一轮策略计算使用的只读速率与 Buffer Pool 压力快照。
 *
 * @param redoBytesGenerated 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
 * @param flushedPages 参与 {@code 构造} 的上界或规格值 {@code flushedPages}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param sampleSeconds 参与 {@code 构造} 的时间量 {@code sampleSeconds}；必须非负，零表示立即检查或尚未累计等待
 * @param dirtyPages 参与 {@code 构造} 的上界或规格值 {@code dirtyPages}；必须非负且不能使容量、页数或编码长度计算溢出
 * @param capacityFrames 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 * @param freeFrames 采样时 buffer pool 可立即分配的 frame 数；必须非负且不得超过实例总容量
 */
public record FlushRuntimeSnapshot(long redoBytesGenerated, int flushedPages, double sampleSeconds,
                                   int dirtyPages, int capacityFrames, int freeFrames) {
    public FlushRuntimeSnapshot {
        if (redoBytesGenerated < 0 || flushedPages < 0 || sampleSeconds < 0
                || dirtyPages < 0 || capacityFrames < 0 || freeFrames < 0 || freeFrames > capacityFrames) {
            throw new DatabaseValidationException("invalid flush runtime snapshot");
        }
    }

    public static FlushRuntimeSnapshot empty() {
        return new FlushRuntimeSnapshot(0, 0, 0, 0, 0, 0);
    }
}
