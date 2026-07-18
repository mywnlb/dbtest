package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证数据库内核值对象中 {@code UndoNo} 的正常、边界与异常语义。
 */
class UndoNoTest {
    /**
     * 验证 {@code noneIsZeroAndDetected} 对应的数据库内核值对象行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test void noneIsZeroAndDetected() {
        assertEquals(0L, UndoNo.NONE.value());
        assertTrue(UndoNo.NONE.isNone());
        assertFalse(UndoNo.of(1).isNone());
    }
    /**
     * 验证 {@code rejectsNegative} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test void rejectsNegative() {
        assertThrows(DatabaseValidationException.class, () -> UndoNo.of(-1));
    }
    /**
     * 验证 {@code valueRoundTrips} 对应的数据库内核值对象行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test void valueRoundTrips() {
        assertEquals(42L, UndoNo.of(42).value());
    }
}
