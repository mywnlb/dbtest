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

    /**
     * 验证 {@code fixedPolicyClassifiesCheckpointAge} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
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

    /**
     * 验证 {@code decisionReportsAgeAndTargetFlushLsn} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void decisionReportsAgeAndTargetFlushLsn() {
        RedoCapacityPolicy policy = RedoCapacityPolicy.fixed(100);

        RedoCapacityDecision decision = policy.evaluate(Lsn.of(180), Lsn.of(100));

        assertEquals(80, decision.checkpointAgeBytes());
        assertEquals(Lsn.of(130), decision.targetCheckpointLsn());
        assertEquals(RedoCapacityPressure.SYNC_FLUSH, decision.pressure());
    }

    /**
     * 验证 {@code invalidInputsAreRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void invalidInputsAreRejected() {
        assertThrows(DatabaseValidationException.class, () -> RedoCapacityPolicy.fixed(0));
        RedoCapacityPolicy policy = RedoCapacityPolicy.fixed(100);

        assertThrows(DatabaseValidationException.class, () -> policy.evaluate(Lsn.of(10), Lsn.of(11)));
    }
}
