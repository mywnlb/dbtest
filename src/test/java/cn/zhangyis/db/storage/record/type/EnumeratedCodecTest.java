package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ENUM ordinal 与 SET bitmap 的定长往返、宽度、范围和 unsigned byte 保序测试。 */
class EnumeratedCodecTest {

    @Test
    void enumUsesOneOrTwoUnsignedBytes() {
        ColumnType smallType = ColumnType.enumType(List.of("A", "B", "C"), false);
        EnumCodec small = new EnumCodec(3);
        assertEquals(1, small.fixedWidth(smallType));
        assertEquals(3, enumRoundTrip(small, smallType, 3));

        List<String> largeSymbols = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            largeSymbols.add("V" + i);
        }
        ColumnType largeType = ColumnType.enumType(largeSymbols, false);
        EnumCodec large = new EnumCodec(256);
        assertEquals(2, large.fixedWidth(largeType));
        assertEquals(256, enumRoundTrip(large, largeType, 256));
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> small.validate(new ColumnValue.EnumValue(0), smallType));
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> small.validate(new ColumnValue.EnumValue(4), smallType));
    }

    @Test
    void setUsesMinimalUnsignedBitmapWidth() {
        ColumnType type = ColumnType.setType(List.of("A", "B", "C", "D", "E", "F", "G", "H", "I"), false);
        SetCodec codec = new SetCodec(9);
        assertEquals(2, codec.fixedWidth(type));
        long bitmap = (1L << 0) | (1L << 8);
        byte[] encoded = encode(codec, type, new ColumnValue.SetValue(bitmap));
        assertEquals(bitmap, ((ColumnValue.SetValue) codec.decode(new FieldSlice(encoded, 0, 2), type)).bitmap());
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> codec.validate(new ColumnValue.SetValue(1L << 9), type));
    }

    @Test
    void encodedOrderMatchesOrdinalAndBitmapOrder() {
        ColumnType enumType = ColumnType.enumType(List.of("A", "B"), false);
        EnumCodec enumCodec = new EnumCodec(2);
        assertTrue(FieldSlice.compareUnsigned(
                slice(encode(enumCodec, enumType, new ColumnValue.EnumValue(1))),
                slice(encode(enumCodec, enumType, new ColumnValue.EnumValue(2)))) < 0);

        ColumnType setType = ColumnType.setType(List.of("A", "B", "C"), false);
        SetCodec setCodec = new SetCodec(3);
        assertTrue(FieldSlice.compareUnsigned(
                slice(encode(setCodec, setType, new ColumnValue.SetValue(1))),
                slice(encode(setCodec, setType, new ColumnValue.SetValue(4)))) < 0);
    }

    private static int enumRoundTrip(EnumCodec codec, ColumnType type, int ordinal) {
        byte[] bytes = encode(codec, type, new ColumnValue.EnumValue(ordinal));
        return ((ColumnValue.EnumValue) codec.decode(slice(bytes), type)).ordinal();
    }

    private static byte[] encode(TypeCodec codec, ColumnType type, ColumnValue value) {
        byte[] bytes = new byte[codec.encodedLength(value, type)];
        codec.encode(value, type, new FieldWriter(bytes, 0));
        return bytes;
    }

    private static FieldSlice slice(byte[] bytes) {
        return new FieldSlice(bytes, 0, bytes.length);
    }
}
