package cn.zhangyis.db.storage.flush.policy;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.redo.RedoCapacityDecision;
import cn.zhangyis.db.storage.redo.RedoCapacityPressure;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** 速率/IO 上限驱动的批量策略边界测试。 */
class AdaptiveFlushBatchPlanTest {

    /**
     * 验证 {@code redoDeficitRaisesTargetButIoCapacityCapsIt} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void redoDeficitRaisesTargetButIoCapacityCapsIt() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(1, 1, 100);
        FlushTuning tuning = new FlushTuning(100, 1, 1, 100, 4, 8, 10, 10);
        FlushRuntimeSnapshot runtime = new FlushRuntimeSnapshot(10_000, 0, 1.0, 20, 100, 50);
        FlushBatchPlan plan = policy.planBatches(decision(RedoCapacityPressure.SYNC_FLUSH), 20, 100,
                runtime, tuning);
        assertTrue(plan.totalPages() <= 4);
        assertEquals(0, plan.lruPages());
    }

    /**
     * 验证 {@code lowFreeRatioAllocatesBatchToLru} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void lowFreeRatioAllocatesBatchToLru() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(1, 1, 20);
        FlushTuning tuning = new FlushTuning(100, 1, 1, 20, 20, 20, 100, 20);
        FlushRuntimeSnapshot runtime = new FlushRuntimeSnapshot(0, 0, 1.0, 8, 10, 1);
        FlushBatchPlan plan = policy.planBatches(decision(RedoCapacityPressure.ASYNC_FLUSH), 8, 20,
                runtime, tuning);
        assertTrue(plan.lruPages() > 0);
        assertEquals(plan.totalPages(), plan.lruPages() + plan.flushListPages());
    }

    private static RedoCapacityDecision decision(RedoCapacityPressure pressure) {
        return new RedoCapacityDecision(pressure, 100, Lsn.of(100));
    }
}
