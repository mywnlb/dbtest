package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Change Buffer 控制面的不可变观测快照。持久 pending/tree 数来自同次 API 调用中的只读 MTR；事件计数是
 * 进程生命周期累计诊断，不参与恢复正确性。
 *
 * @param available 当前实例是否拥有完整 system.ibd 格式
 * @param configuredMode EngineConfig 请求并在 fresh header 留档的模式
 * @param effectiveMode 结合 legacy/persistent metadata 能力后的实际新 mutation 模式
 * @param pendingRecords page3 与全局树已经校验一致的待处理记录数
 * @param counters 运行期累计事件
 * @param systemTreePages 固定 root 加两个内部 segment 的已用页数
 * @param observedBitmapPages 当前进程实际验证过 envelope 的不同 bitmap 页数
 * @param workerState 后台主动合并生命周期；demand merge 不依赖该状态
 */
public record ChangeBufferSnapshot(boolean available, ChangeBufferMode configuredMode,
                                   ChangeBufferMode effectiveMode, long pendingRecords,
                                   ChangeBufferCountersSnapshot counters, long systemTreePages,
                                   long observedBitmapPages, ChangeBufferWorkerState workerState) {

    /** 校验 snapshot 内部数值与可用性形状。 */
    public ChangeBufferSnapshot {
        if (configuredMode == null || effectiveMode == null || counters == null || workerState == null
                || pendingRecords < 0 || systemTreePages < 0 || observedBitmapPages < 0) {
            throw new DatabaseValidationException("change buffer snapshot fields are invalid");
        }
        if (!available && (pendingRecords != 0 || systemTreePages != 0 || observedBitmapPages != 0
                || effectiveMode != ChangeBufferMode.NONE)) {
            throw new DatabaseValidationException("legacy change buffer snapshot must be empty and disabled");
        }
    }
}
