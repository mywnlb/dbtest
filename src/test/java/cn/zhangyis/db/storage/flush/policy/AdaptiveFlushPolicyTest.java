package cn.zhangyis.db.storage.flush.policy;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.redo.RedoCapacityDecision;
import cn.zhangyis.db.storage.redo.RedoCapacityPressure;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adaptive flush 策略测试：
 * <ul>
 *   <li>{@code fixed}（离散）：pressure → 固定档位，忽略脏页 backlog；</li>
 *   <li>{@code adaptive}（§7.4 比例版）：clamp(basePages + factor·dirtyPagesBeforeTarget, min, max)，随 backlog 自适应。</li>
 * </ul>
 */
class AdaptiveFlushPolicyTest {

    // ---- fixed（离散）：忽略 backlog ----

    @Test
    void fixedPressureMapsToDeterministicBatchSizes() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.fixed(2, 10);

        assertAdvice(policy.plan(decision(RedoCapacityPressure.NONE), 7, 99), 0, false);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.ASYNC_FLUSH), 7, 99), 2, false);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 7, 99), 6, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 7, 99), 10, true);
    }

    @Test
    void fixedRequestMaxClampsPlannedPages() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.fixed(2, 10);

        FlushAdvice advice = policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 0, 3);

        assertEquals(3, advice.maxPages());
        assertEquals(Lsn.of(40), advice.targetLsn());
        assertTrue(advice.synchronousPressure());
    }

    // ---- adaptive（§7.4 比例）：随 backlog ----

    @Test
    void adaptiveScalesBatchWithDirtyBacklog() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(2, 1, 20);

        // SYNC factor 0.5：base 2 + round(0.5*backlog)，clamp[1,20]
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 0, 99), 2, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 10, 99), 7, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 40, 99), 20, true);
    }

    @Test
    void adaptiveHardPressureScalesBacklogToMax() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(2, 1, 20);

        // HARD factor 1.0：base 2 + backlog
        assertAdvice(policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 15, 99), 17, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 50, 99), 20, true);
    }

    @Test
    void adaptiveNonePressureFlushesNothing() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(2, 1, 20);

        FlushAdvice advice = policy.plan(decision(RedoCapacityPressure.NONE), 100, 99);

        assertEquals(0, advice.maxPages());
        assertFalse(advice.shouldFlush());
    }

    @Test
    void adaptiveFloorsToMinBatchAndClampsToRequestMax() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(1, 5, 20);

        // ASYNC factor 0.25，backlog 0 → raw 1 → floor 到 min 5
        assertAdvice(policy.plan(decision(RedoCapacityPressure.ASYNC_FLUSH), 0, 99), 5, false);
        // planned 远大于 requestMax 时被外层上限裁剪
        assertEquals(3, policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 50, 3).maxPages());
    }

    @Test
    void invalidConfigurationOrRequestIsRejected() {
        assertThrows(DatabaseValidationException.class, () -> AdaptiveFlushPolicy.fixed(0, 10));
        assertThrows(DatabaseValidationException.class, () -> AdaptiveFlushPolicy.fixed(10, 2));
        assertThrows(DatabaseValidationException.class, () -> AdaptiveFlushPolicy.adaptive(-1, 1, 10));
        assertThrows(DatabaseValidationException.class, () -> AdaptiveFlushPolicy.adaptive(1, 0, 10));
        assertThrows(DatabaseValidationException.class, () -> AdaptiveFlushPolicy.adaptive(1, 10, 2));

        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(1, 1, 10);
        assertThrows(DatabaseValidationException.class,
                () -> policy.plan(decision(RedoCapacityPressure.ASYNC_FLUSH), -1, 5));
        assertThrows(DatabaseValidationException.class,
                () -> policy.plan(decision(RedoCapacityPressure.ASYNC_FLUSH), 0, -1));
    }

    private static RedoCapacityDecision decision(RedoCapacityPressure pressure) {
        return new RedoCapacityDecision(pressure, 80, Lsn.of(40));
    }

    private static void assertAdvice(FlushAdvice advice, int pages, boolean synchronousPressure) {
        assertEquals(Lsn.of(40), advice.targetLsn());
        assertEquals(pages, advice.maxPages());
        assertEquals(synchronousPressure, advice.synchronousPressure());
        assertFalse(advice.shouldFlush() && pages == 0);
    }
}
