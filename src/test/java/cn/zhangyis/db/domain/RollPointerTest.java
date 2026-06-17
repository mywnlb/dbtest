package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RollPointer：NULL 全零、往返、u16/u32 范围、reserved 位、缓冲不足。 */
class RollPointerTest {

    @Test
    void nullIsAllZeroAndDetected() {
        assertArrayEquals(new byte[7], RollPointer.NULL.encode());
        assertTrue(RollPointer.NULL.isNull());
        assertTrue(RollPointer.decode(new byte[7], 0).isNull());
    }

    @Test
    void roundTripsInsertPageOffset() {
        RollPointer p = new RollPointer(true, PageNo.of(0x01020304L), 0xABCD);
        RollPointer back = RollPointer.decode(p.encode(), 0);
        assertEquals(p, back);
        assertTrue(back.insert());
        assertEquals(0x01020304L, back.pageNo().value());
        assertEquals(0xABCD, back.offset());
    }

    @Test
    void decodeRespectsOffset() {
        byte[] buf = new byte[3 + RollPointer.BYTES];
        RollPointer p = new RollPointer(false, PageNo.of(5), 6);
        System.arraycopy(p.encode(), 0, buf, 3, RollPointer.BYTES);
        assertEquals(p, RollPointer.decode(buf, 3));
    }

    @Test
    void rejectsOffsetOutOfU16() {
        assertThrows(DatabaseValidationException.class,
                () -> new RollPointer(false, PageNo.of(1), 0x10000));
    }

    @Test
    void rejectsPageNoOutOfU32() {
        assertThrows(DatabaseValidationException.class,
                () -> new RollPointer(false, PageNo.of(0x1_0000_0000L), 0));
    }

    @Test
    void decodeRejectsReservedBitsSet() {
        byte[] bad = new byte[RollPointer.BYTES];
        bad[0] = 0x40;
        assertThrows(DatabaseValidationException.class, () -> RollPointer.decode(bad, 0));
    }

    @Test
    void decodeRejectsShortBuffer() {
        assertThrows(DatabaseValidationException.class, () -> RollPointer.decode(new byte[6], 0));
    }
}
