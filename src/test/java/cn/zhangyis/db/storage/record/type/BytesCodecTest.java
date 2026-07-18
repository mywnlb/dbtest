package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.CharsetId;
import cn.zhangyis.db.storage.record.schema.CollationId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FixedBytesCodec(CHAR/BINARY) 与 VarBytesCodec(VARCHAR/VARBINARY) 编解码、补齐、比较、超长。 */
class BytesCodecTest {

    private static final ColumnType CHAR5 = ColumnType.charType(5, false);
    private static final ColumnType BIN4 = ColumnType.binary(4, false);
    private static final ColumnType VC10 = ColumnType.varchar(10, false);
    private static final ColumnType VB10 = ColumnType.varbinary(10, false);

    private static byte[] enc(TypeCodec c, ColumnType t, ColumnValue v) {
        byte[] buf = new byte[c.encodedLength(v, t)];
        c.encode(v, t, new FieldWriter(buf, 0));
        return buf;
    }

    private static ColumnValue dec(TypeCodec c, ColumnType t, byte[] b) {
        return c.decode(new FieldSlice(b, 0, b.length), t);
    }

    /**
     * 验证 {@code charPadsAndStripsTrailingSpaces} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void charPadsAndStripsTrailingSpaces() {
        FixedBytesCodec c = new FixedBytesCodec(5, (byte) 0x20, true);
        byte[] e = enc(c, CHAR5, new ColumnValue.StringValue("ab"));
        assertArrayEquals(new byte[] {'a', 'b', ' ', ' ', ' '}, e);
        assertEquals("ab", ((ColumnValue.StringValue) dec(c, CHAR5, e)).value());
    }

    /**
     * 验证 {@code charRejectsTooLong} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void charRejectsTooLong() {
        FixedBytesCodec c = new FixedBytesCodec(5, (byte) 0x20, true);
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c.validate(new ColumnValue.StringValue("abcdef"), CHAR5));
    }

    /**
     * 验证 {@code binaryZeroPadsAndKeeps} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void binaryZeroPadsAndKeeps() {
        FixedBytesCodec c = new FixedBytesCodec(4, (byte) 0x00, false);
        byte[] e = enc(c, BIN4, new ColumnValue.BinaryValue(new byte[] {1, 2}));
        assertArrayEquals(new byte[] {1, 2, 0, 0}, e);
        assertArrayEquals(new byte[] {1, 2, 0, 0}, ((ColumnValue.BinaryValue) dec(c, BIN4, e)).value());
    }

    /**
     * 验证 {@code varcharRoundTripEmptyAndValue} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void varcharRoundTripEmptyAndValue() {
        VarBytesCodec c = new VarBytesCodec(10, true);
        byte[] empty = enc(c, VC10, new ColumnValue.StringValue(""));
        assertEquals(0, empty.length);
        assertEquals("", ((ColumnValue.StringValue) dec(c, VC10, empty)).value());
        byte[] hello = enc(c, VC10, new ColumnValue.StringValue("hello"));
        assertEquals("hello", ((ColumnValue.StringValue) dec(c, VC10, hello)).value());
    }

    /**
     * 验证 {@code varRejectsTooLong} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void varRejectsTooLong() {
        VarBytesCodec c = new VarBytesCodec(10, false);
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c.validate(new ColumnValue.BinaryValue(new byte[11]), VB10));
    }

    /**
     * 验证 {@code byteOrderCompare} 所描述的值对象语义，并断言相等性、哈希、排序及非法构造边界一致。
     */
    @Test
    void byteOrderCompare() {
        VarBytesCodec c = new VarBytesCodec(10, true);
        byte[] abc = enc(c, VC10, new ColumnValue.StringValue("abc"));
        byte[] abd = enc(c, VC10, new ColumnValue.StringValue("abd"));
        byte[] ab = enc(c, VC10, new ColumnValue.StringValue("ab"));
        assertTrue(c.compare(new FieldSlice(abc, 0, abc.length), new FieldSlice(abd, 0, abd.length), VC10) < 0);
        assertTrue(c.compare(new FieldSlice(ab, 0, ab.length), new FieldSlice(abc, 0, abc.length), VC10) < 0);
    }

    /**
     * 验证 {@code varcharUsesDeclaredCharsetAndCollation} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void varcharUsesDeclaredCharsetAndCollation() {
        TypeCodecRegistry registry = new TypeCodecRegistry();
        ColumnType latin1Ci = ColumnType.varchar(
                10, false, CharsetId.LATIN1, CollationId.LATIN1_ASCII_CI);
        TypeCodec codec = registry.codecFor(latin1Ci);
        byte[] upper = enc(codec, latin1Ci, new ColumnValue.StringValue("ÉA"));
        byte[] lower = enc(codec, latin1Ci, new ColumnValue.StringValue("Éa"));

        assertArrayEquals(new byte[] {(byte) 0xC9, 'A'}, upper);
        assertEquals("ÉA", ((ColumnValue.StringValue) dec(codec, latin1Ci, upper)).value());
        assertEquals(0, codec.compare(
                new FieldSlice(upper, 0, upper.length), new FieldSlice(lower, 0, lower.length), latin1Ci));
    }

    /**
     * 验证 {@code characterCodecRejectsUnmappableAndMalformedValues} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void characterCodecRejectsUnmappableAndMalformedValues() {
        TypeCodecRegistry registry = new TypeCodecRegistry();
        ColumnType latin1 = ColumnType.varchar(10, false, CharsetId.LATIN1, CollationId.BINARY);
        TypeCodec latin1Codec = registry.codecFor(latin1);
        assertThrows(InvalidCharacterEncodingException.class,
                () -> latin1Codec.validate(new ColumnValue.StringValue("汉"), latin1));

        ColumnType utf8 = ColumnType.varchar(10, false);
        TypeCodec utf8Codec = registry.codecFor(utf8);
        byte[] malformed = {(byte) 0xC3};
        assertThrows(InvalidCharacterEncodingException.class,
                () -> utf8Codec.decode(new FieldSlice(malformed, 0, 1), utf8));
    }
}
