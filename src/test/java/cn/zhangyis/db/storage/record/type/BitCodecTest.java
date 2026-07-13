package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** BIT(n) 定长 canonical 编码：位宽、unused low bits、防御性复制、往返与 unsigned byte 保序。 */
class BitCodecTest {

    /** BIT(9) 占 2B，最后 7 个低位必须为 0；合法值经 encode/decode 保持字节。 */
    @Test
    void bitNineRoundTripsCanonicalBytes() {
        ColumnType type = ColumnType.bit(9, false);
        BitCodec codec = new BitCodec(9);
        byte[] source = {(byte) 0xA5, (byte) 0x80};
        ColumnValue.BitValue value = new ColumnValue.BitValue(source);
        source[0] = 0;

        byte[] encoded = new byte[codec.encodedLength(value, type)];
        codec.encode(value, type, new FieldWriter(encoded, 0));
        assertArrayEquals(new byte[] {(byte) 0xA5, (byte) 0x80}, encoded);
        assertEquals(2, codec.fixedWidth(type));
        ColumnValue.BitValue decoded = (ColumnValue.BitValue) codec.decode(
                new FieldSlice(encoded, 0, encoded.length), type);
        assertArrayEquals(encoded, decoded.value());
    }

    /** 非 byte-aligned 位宽拒绝未清零的低位，也拒绝错误 byte length。 */
    @Test
    void rejectsNonCanonicalOrWrongLength() {
        ColumnType type = ColumnType.bit(9, false);
        BitCodec codec = new BitCodec(9);
        assertThrows(InvalidColumnValueException.class,
                () -> codec.validate(new ColumnValue.BitValue(new byte[] {1, 1}), type));
        assertThrows(InvalidColumnValueException.class,
                () -> codec.validate(new ColumnValue.BitValue(new byte[] {1}), type));
    }

    /** 固定宽度下 unsigned byte 字典序等于 bit string 自然序。 */
    @Test
    void comparesUnsignedBytes() {
        ColumnType type = ColumnType.bit(9, false);
        BitCodec codec = new BitCodec(9);
        byte[] lower = {0x01, (byte) 0x80};
        byte[] higher = {0x02, 0x00};
        assertTrue(codec.compare(new FieldSlice(lower, 0, 2), new FieldSlice(higher, 0, 2), type) < 0);
    }

    /** byte-aligned 边界不需要 unused-bit mask，BIT(64) 仍只占 8B。 */
    @Test
    void byteAlignedBoundaryUsesExactWidth() {
        assertEquals(1, new BitCodec(1).fixedWidth(ColumnType.bit(1, false)));
        assertEquals(1, new BitCodec(8).fixedWidth(ColumnType.bit(8, false)));
        assertEquals(8, new BitCodec(64).fixedWidth(ColumnType.bit(64, false)));
    }
}
