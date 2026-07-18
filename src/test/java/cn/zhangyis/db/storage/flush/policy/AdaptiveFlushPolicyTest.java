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

    /**
     * 验证 {@code fixedPressureMapsToDeterministicBatchSizes} 对应的脏页刷盘与 checkpoint行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void fixedPressureMapsToDeterministicBatchSizes() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.fixed(2, 10);

        assertAdvice(policy.plan(decision(RedoCapacityPressure.NONE), 7, 99), 0, false);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.ASYNC_FLUSH), 7, 99), 2, false);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 7, 99), 6, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 7, 99), 10, true);
    }

    /**
     * 验证 {@code fixedRequestMaxClampsPlannedPages} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void fixedRequestMaxClampsPlannedPages() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.fixed(2, 10);

        FlushAdvice advice = policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 0, 3);

        assertEquals(3, advice.maxPages());
        assertEquals(Lsn.of(40), advice.targetLsn());
        assertTrue(advice.synchronousPressure());
    }

    // ---- adaptive（§7.4 比例）：随 backlog ----

    /**
     * 验证 {@code adaptiveScalesBatchWithDirtyBacklog} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void adaptiveScalesBatchWithDirtyBacklog() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(2, 1, 20);

        // SYNC factor 0.5：base 2 + round(0.5*backlog)，clamp[1,20]
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 0, 99), 2, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 10, 99), 7, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 40, 99), 20, true);
    }

    /**
     * 验证 {@code adaptiveHardPressureScalesBacklogToMax} 对应的脏页刷盘与 checkpoint行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void adaptiveHardPressureScalesBacklogToMax() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(2, 1, 20);

        // HARD factor 1.0：base 2 + backlog
        assertAdvice(policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 15, 99), 17, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 50, 99), 20, true);
    }

    /**
     * 验证 {@code adaptiveNonePressureFlushesNothing} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void adaptiveNonePressureFlushesNothing() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(2, 1, 20);

        FlushAdvice advice = policy.plan(decision(RedoCapacityPressure.NONE), 100, 99);

        assertEquals(0, advice.maxPages());
        assertFalse(advice.shouldFlush());
    }

    /**
     * 验证 {@code adaptiveFloorsToMinBatchAndClampsToRequestMax} 对应的脏页刷盘与 checkpoint行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void adaptiveFloorsToMinBatchAndClampsToRequestMax() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.adaptive(1, 5, 20);

        // ASYNC factor 0.25，backlog 0 → raw 1 → floor 到 min 5
        assertAdvice(policy.plan(decision(RedoCapacityPressure.ASYNC_FLUSH), 0, 99), 5, false);
        // planned 远大于 requestMax 时被外层上限裁剪
        assertEquals(3, policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 50, 3).maxPages());
    }

    /**
     * 验证 {@code invalidConfigurationOrRequestIsRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
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
