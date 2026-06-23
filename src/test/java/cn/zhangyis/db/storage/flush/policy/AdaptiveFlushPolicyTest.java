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
 * F2 adaptive flush 策略测试：把 redo capacity pressure 映射为本轮 flush target 和页数，不直接访问 BufferPool。
 */
class AdaptiveFlushPolicyTest {

    @Test
    void pressureMapsToDeterministicBatchSizes() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.fixed(2, 10);

        assertAdvice(policy.plan(decision(RedoCapacityPressure.NONE), 99), 0, false);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.ASYNC_FLUSH), 99), 2, false);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.SYNC_FLUSH), 99), 6, true);
        assertAdvice(policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 99), 10, true);
    }

    @Test
    void requestMaxClampsPlannedPages() {
        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.fixed(2, 10);

        FlushAdvice advice = policy.plan(decision(RedoCapacityPressure.HARD_LIMIT), 3);

        assertEquals(3, advice.maxPages());
        assertEquals(Lsn.of(40), advice.targetLsn());
        assertTrue(advice.synchronousPressure());
    }

    @Test
    void invalidConfigurationOrRequestIsRejected() {
        assertThrows(DatabaseValidationException.class, () -> AdaptiveFlushPolicy.fixed(0, 10));
        assertThrows(DatabaseValidationException.class, () -> AdaptiveFlushPolicy.fixed(10, 2));

        AdaptiveFlushPolicy policy = AdaptiveFlushPolicy.fixed(2, 10);
        assertThrows(DatabaseValidationException.class, () -> policy.plan(decision(RedoCapacityPressure.ASYNC_FLUSH), -1));
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
