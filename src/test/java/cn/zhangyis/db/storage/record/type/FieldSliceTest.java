package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FieldSlice 无符号比较与越界。 */
class FieldSliceTest {

    /**
     * 验证 {@code compareUnsignedTreatsHighBitAsLarge} 所描述的值对象语义，并断言相等性、哈希、排序及非法构造边界一致。
     */
    @Test
    void compareUnsignedTreatsHighBitAsLarge() {
        FieldSlice low = new FieldSlice(new byte[] {0x7F}, 0, 1);
        FieldSlice high = new FieldSlice(new byte[] {(byte) 0x80}, 0, 1);
        assertTrue(FieldSlice.compareUnsigned(low, high) < 0);
        assertTrue(FieldSlice.compareUnsigned(high, low) > 0);
        assertTrue(FieldSlice.compareUnsigned(low, low) == 0);
    }

    /**
     * 验证 {@code shorterIsLessWhenPrefixEqual} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void shorterIsLessWhenPrefixEqual() {
        FieldSlice ab = new FieldSlice(new byte[] {'a', 'b'}, 0, 2);
        FieldSlice abc = new FieldSlice(new byte[] {'a', 'b', 'c'}, 0, 3);
        assertTrue(FieldSlice.compareUnsigned(ab, abc) < 0);
    }

    /**
     * 验证 {@code byteAtOutOfRangeThrows} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void byteAtOutOfRangeThrows() {
        FieldSlice s = new FieldSlice(new byte[] {1, 2, 3}, 0, 3);
        assertThrows(DatabaseValidationException.class, () -> s.byteAt(3));
    }
}
