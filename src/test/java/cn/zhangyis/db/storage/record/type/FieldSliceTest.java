package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FieldSlice 无符号比较与越界。 */
class FieldSliceTest {

    @Test
    void compareUnsignedTreatsHighBitAsLarge() {
        FieldSlice low = new FieldSlice(new byte[] {0x7F}, 0, 1);
        FieldSlice high = new FieldSlice(new byte[] {(byte) 0x80}, 0, 1);
        assertTrue(FieldSlice.compareUnsigned(low, high) < 0);
        assertTrue(FieldSlice.compareUnsigned(high, low) > 0);
        assertTrue(FieldSlice.compareUnsigned(low, low) == 0);
    }

    @Test
    void shorterIsLessWhenPrefixEqual() {
        FieldSlice ab = new FieldSlice(new byte[] {'a', 'b'}, 0, 2);
        FieldSlice abc = new FieldSlice(new byte[] {'a', 'b', 'c'}, 0, 3);
        assertTrue(FieldSlice.compareUnsigned(ab, abc) < 0);
    }

    @Test
    void byteAtOutOfRangeThrows() {
        FieldSlice s = new FieldSlice(new byte[] {1, 2, 3}, 0, 3);
        assertThrows(DatabaseValidationException.class, () -> s.byteAt(3));
    }
}
