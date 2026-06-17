package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** NULL 位图：字节数向上取整、置位/读位、往返一致、越界拒绝。 */
class NullBitmapTest {

    /** byteLength = ceil(count/8)，0 列占 0 字节。 */
    @Test
    void byteLengthRoundsUp() {
        assertEquals(0, NullBitmap.byteLength(0));
        assertEquals(1, NullBitmap.byteLength(1));
        assertEquals(1, NullBitmap.byteLength(8));
        assertEquals(2, NullBitmap.byteLength(9));
    }

    /** set 后 get 为真，未 set 的位为假；跨字节生效。 */
    @Test
    void setAndGetAcrossBytes() {
        NullBitmap bm = new NullBitmap(10);
        bm.set(0);
        bm.set(7);
        bm.set(8);
        assertTrue(bm.get(0));
        assertTrue(bm.get(7));
        assertTrue(bm.get(8));
        assertFalse(bm.get(1));
        assertFalse(bm.get(9));
    }

    /** 写出后按相同列数读回，位状态完全一致。 */
    @Test
    void roundTripThroughBuffer() {
        NullBitmap bm = new NullBitmap(12);
        bm.set(1);
        bm.set(11);
        byte[] buf = new byte[bm.byteLength()];
        bm.writeTo(buf, 0);
        NullBitmap back = NullBitmap.readFrom(buf, 0, 12);
        for (int i = 0; i < 12; i++) {
            assertEquals(bm.get(i), back.get(i), "bit " + i);
        }
    }

    /** 越界索引与负列数应被拒绝。 */
    @Test
    void rejectsOutOfRange() {
        NullBitmap bm = new NullBitmap(3);
        assertThrows(DatabaseValidationException.class, () -> bm.set(3));
        assertThrows(DatabaseValidationException.class, () -> bm.get(-1));
        assertThrows(DatabaseValidationException.class, () -> new NullBitmap(-1));
    }
}
