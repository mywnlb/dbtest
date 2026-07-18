package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TransactionNo：NONE 哨兵、非负校验、值往返。 */
class TransactionNoTest {

    /**
     * 验证 {@code noneIsZeroAndDetected} 对应的数据库内核值对象行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void noneIsZeroAndDetected() {
        assertEquals(0L, TransactionNo.NONE.value());
        assertTrue(TransactionNo.NONE.isNone());
        assertFalse(TransactionNo.of(1).isNone());
    }

    /**
     * 验证 {@code rejectsNegative} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsNegative() {
        assertThrows(DatabaseValidationException.class, () -> TransactionNo.of(-1));
    }

    /**
     * 验证 {@code valueRoundTrips} 对应的数据库内核值对象行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void valueRoundTrips() {
        assertEquals(7L, TransactionNo.of(7).value());
    }
}
