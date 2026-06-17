package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * R2 redo capacity pressure 测试：checkpoint age 按 capacity 阈值分级，供后续 page cleaner/前台限流接入。
 */
class RedoCapacityPolicyTest {

    @Test
    void fixedPolicyClassifiesCheckpointAge() {
        RedoCapacityPolicy policy = RedoCapacityPolicy.fixed(100);

        assertEquals(RedoCapacityPressure.NONE,
                policy.evaluate(Lsn.of(49), Lsn.of(0)).pressure());
        assertEquals(RedoCapacityPressure.ASYNC_FLUSH,
                policy.evaluate(Lsn.of(50), Lsn.of(0)).pressure());
        assertEquals(RedoCapacityPressure.SYNC_FLUSH,
                policy.evaluate(Lsn.of(75), Lsn.of(0)).pressure());
        assertEquals(RedoCapacityPressure.HARD_LIMIT,
                policy.evaluate(Lsn.of(90), Lsn.of(0)).pressure());
    }

    @Test
    void decisionReportsAgeAndTargetFlushLsn() {
        RedoCapacityPolicy policy = RedoCapacityPolicy.fixed(100);

        RedoCapacityDecision decision = policy.evaluate(Lsn.of(180), Lsn.of(100));

        assertEquals(80, decision.checkpointAgeBytes());
        assertEquals(Lsn.of(130), decision.targetCheckpointLsn());
        assertEquals(RedoCapacityPressure.SYNC_FLUSH, decision.pressure());
    }

    @Test
    void invalidInputsAreRejected() {
        assertThrows(DatabaseValidationException.class, () -> RedoCapacityPolicy.fixed(0));
        RedoCapacityPolicy policy = RedoCapacityPolicy.fixed(100);

        assertThrows(DatabaseValidationException.class, () -> policy.evaluate(Lsn.of(10), Lsn.of(11)));
    }
}
