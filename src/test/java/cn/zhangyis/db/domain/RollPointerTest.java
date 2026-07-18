package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RollPointer：NULL 全零、往返、u16/u32 范围、reserved 位、缓冲不足。 */
class RollPointerTest {

    /**
     * 验证 {@code nullIsAllZeroAndDetected} 对应的数据库内核值对象行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void nullIsAllZeroAndDetected() {
        assertArrayEquals(new byte[7], RollPointer.NULL.encode());
        assertTrue(RollPointer.NULL.isNull());
        assertTrue(RollPointer.decode(new byte[7], 0).isNull());
    }

    /**
     * 验证 {@code roundTripsInsertPageOffset} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void roundTripsInsertPageOffset() {
        RollPointer p = new RollPointer(true, PageNo.of(0x01020304L), 0xABCD);
        RollPointer back = RollPointer.decode(p.encode(), 0);
        assertEquals(p, back);
        assertTrue(back.insert());
        assertEquals(0x01020304L, back.pageNo().value());
        assertEquals(0xABCD, back.offset());
    }

    /**
     * 验证 {@code decodeRespectsOffset} 所描述的稳定格式转换，并断言往返值、字节布局、版本与损坏输入处理。
     */
    @Test
    void decodeRespectsOffset() {
        byte[] buf = new byte[3 + RollPointer.BYTES];
        RollPointer p = new RollPointer(false, PageNo.of(5), 6);
        System.arraycopy(p.encode(), 0, buf, 3, RollPointer.BYTES);
        assertEquals(p, RollPointer.decode(buf, 3));
    }

    /**
     * 验证 {@code rejectsOffsetOutOfU16} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsOffsetOutOfU16() {
        assertThrows(DatabaseValidationException.class,
                () -> new RollPointer(false, PageNo.of(1), 0x10000));
    }

    /**
     * 验证 {@code rejectsPageNoOutOfU32} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsPageNoOutOfU32() {
        assertThrows(DatabaseValidationException.class,
                () -> new RollPointer(false, PageNo.of(0x1_0000_0000L), 0));
    }

    /**
     * 验证 {@code decodeRejectsReservedBitsSet} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void decodeRejectsReservedBitsSet() {
        byte[] bad = new byte[RollPointer.BYTES];
        bad[0] = 0x40;
        assertThrows(DatabaseValidationException.class, () -> RollPointer.decode(bad, 0));
    }

    /**
     * 验证 {@code decodeRejectsShortBuffer} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void decodeRejectsShortBuffer() {
        assertThrows(DatabaseValidationException.class, () -> RollPointer.decode(new byte[6], 0));
    }
}
