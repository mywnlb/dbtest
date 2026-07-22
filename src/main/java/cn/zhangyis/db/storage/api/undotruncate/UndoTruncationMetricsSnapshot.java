package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * purge 驱动 undo truncate 的不可变观测快照。
 *
 * @param enabled 当前配置是否启用自动调度
 * @param checks 已越过 cooldown 并调用 attempt target 的次数
 * @param skipped 因 extent 阈值不足跳过的次数
 * @param deferred 因 access/history/active/reuse 竞争延期的次数
 * @param completed 完成 crash-safe truncate 的次数
 * @param failures 传播给 purge driver 的真实存储失败次数
 * @param reclaimedPages 所有完成 cycle 在取得 X lease 时观察到的累计可回收页数
 * @param lastStatus 最近状态；禁用和从未运行也使用显式枚举表达
 * @param lastCompletedEpoch 最近完成的持久 truncate epoch；从未完成时为 0
 * @param lastFailure 最近真实失败的类名与消息；尚无失败时为空字符串
 */
public record UndoTruncationMetricsSnapshot(
        boolean enabled,
        long checks,
        long skipped,
        long deferred,
        long completed,
        long failures,
        long reclaimedPages,
        UndoTruncationCycleStatus lastStatus,
        long lastCompletedEpoch,
        String lastFailure) {

    /**
     * 校验所有累计量单调域与非空诊断字段。
     *
     * @throws DatabaseValidationException 计数为负或状态/失败字符串为空引用时抛出
     */
    public UndoTruncationMetricsSnapshot {
        if (checks < 0 || skipped < 0 || deferred < 0 || completed < 0 || failures < 0
                || reclaimedPages < 0 || lastCompletedEpoch < 0) {
            throw new DatabaseValidationException("undo truncation metrics counters must not be negative");
        }
        if (lastStatus == null || lastFailure == null) {
            throw new DatabaseValidationException("undo truncation metrics status/failure must not be null");
        }
    }

    /**
     * 创建与配置启用状态一致的零计数初始快照，供组合根在 scheduler 尚未构造时提供稳定诊断。
     *
     * @param enabled 当前自动截断配置是否启用
     * @return 未执行任何候选检查的不可变初始快照
     */
    public static UndoTruncationMetricsSnapshot initial(boolean enabled) {
        return new UndoTruncationMetricsSnapshot(enabled, 0, 0, 0, 0, 0, 0,
                enabled ? UndoTruncationCycleStatus.NEVER_RUN : UndoTruncationCycleStatus.DISABLED,
                0, "");
    }
}
