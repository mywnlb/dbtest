package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Change Buffer 运行期累计事件的不可变观测值。计数表示已经跨越对应提交边界的动作；失败前未提交的尝试
 * 不计入 buffered/merged/discarded，但会计入 mergeFailures。
 *
 * @param bufferedOperations 已由全局树 durable 接管的二级 mutation 数
 * @param mergedOperations 已在目标 leaf 应用并从全局树 consume 的 mutation 数
 * @param discardedOperations DDL 在回收物理 identity 前直接 consume 的 mutation 数
 * @param directFallbacks eligibility 不成立后在真实二级树完成的 mutation 数
 * @param mergeFailures 发布前或后台合并无法安全完成的次数
 */
public record ChangeBufferCountersSnapshot(long bufferedOperations, long mergedOperations,
                                           long discardedOperations, long directFallbacks,
                                           long mergeFailures) {

    /** 校验计数均为单调非负值。 */
    public ChangeBufferCountersSnapshot {
        if (bufferedOperations < 0 || mergedOperations < 0 || discardedOperations < 0
                || directFallbacks < 0 || mergeFailures < 0) {
            throw new DatabaseValidationException("change buffer counters must not be negative");
        }
    }
}
