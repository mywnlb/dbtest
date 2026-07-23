package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.concurrent.atomic.LongAdder;

/**
 * 一个 StorageEngine 生命周期内由所有 Change Buffer 路径共享的无锁统计 owner。LongAdder 仅用于诊断，
 * 不参与 eligibility、恢复或提交裁决，因而弱一致读取不会影响数据库正确性。
 */
public final class ChangeBufferCounters {

    /** 已由 global tree/header/bitmap 原子提交接管的 mutation 数。 */
    private final LongAdder buffered = new LongAdder();
    /** 已应用到真实 leaf 并从 global tree 原子消费的 mutation 数。 */
    private final LongAdder merged = new LongAdder();
    /** DDL 在物理 identity 回收前直接消费的 mutation 数。 */
    private final LongAdder discarded = new LongAdder();
    /** eligibility 不成立后成功提交到真实二级树的 mutation 次数。 */
    private final LongAdder directFallbacks = new LongAdder();
    /** 发布前或后台 merge 未能安全提交、且目标页保持不可见的失败次数。 */
    private final LongAdder mergeFailures = new LongAdder();

    /** 在全局记录、header 与 bitmap 同一 MTR 提交后记录一次 durable 接管。 */
    public void recordBuffered() {
        buffered.increment();
    }

    /**
     * 在目标页修改与全局 consume 同一 MTR 提交后累计实际记录数。
     *
     * @param count 本次已 durable merge/consume 的正 mutation 数
     * @throws DatabaseValidationException count 非正时抛出，统计保持不变
     */
    public void recordMerged(long count) {
        addPositive(merged, count, "merged");
    }

    /**
     * 在 DDL discard MTR 提交后累计实际 consume 数。
     *
     * @param count 本次已 durable discard 的正 mutation 数
     * @throws DatabaseValidationException count 非正时抛出，统计保持不变
     */
    public void recordDiscarded(long count) {
        addPositive(discarded, count, "discarded");
    }

    /** 在真实二级 B+Tree 分支提交后记录一次 eligibility fallback。 */
    public void recordDirectFallback() {
        directFallbacks.increment();
    }

    /** 发布前/后台 merge 抛出异常并保持页面不可见时记录一次失败。 */
    public void recordMergeFailure() {
        mergeFailures.increment();
    }

    /** @return 当前五类累计值的不可变弱一致快照。 */
    public ChangeBufferCountersSnapshot snapshot() {
        return new ChangeBufferCountersSnapshot(buffered.sum(), merged.sum(), discarded.sum(),
                directFallbacks.sum(), mergeFailures.sum());
    }

    private static void addPositive(LongAdder target, long count, String name) {
        if (count <= 0) {
            throw new DatabaseValidationException("change buffer " + name + " count must be positive: " + count);
        }
        target.add(count);
    }
}
