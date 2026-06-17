package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
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

    @Test
    void charPadsAndStripsTrailingSpaces() {
        FixedBytesCodec c = new FixedBytesCodec(5, (byte) 0x20, true);
        byte[] e = enc(c, CHAR5, new ColumnValue.StringValue("ab"));
        assertArrayEquals(new byte[] {'a', 'b', ' ', ' ', ' '}, e);
        assertEquals("ab", ((ColumnValue.StringValue) dec(c, CHAR5, e)).value());
    }

    @Test
    void charRejectsTooLong() {
        FixedBytesCodec c = new FixedBytesCodec(5, (byte) 0x20, true);
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c.validate(new ColumnValue.StringValue("abcdef"), CHAR5));
    }

    @Test
    void binaryZeroPadsAndKeeps() {
        FixedBytesCodec c = new FixedBytesCodec(4, (byte) 0x00, false);
        byte[] e = enc(c, BIN4, new ColumnValue.BinaryValue(new byte[] {1, 2}));
        assertArrayEquals(new byte[] {1, 2, 0, 0}, e);
        assertArrayEquals(new byte[] {1, 2, 0, 0}, ((ColumnValue.BinaryValue) dec(c, BIN4, e)).value());
    }

    @Test
    void varcharRoundTripEmptyAndValue() {
        VarBytesCodec c = new VarBytesCodec(10, true);
        byte[] empty = enc(c, VC10, new ColumnValue.StringValue(""));
        assertEquals(0, empty.length);
        assertEquals("", ((ColumnValue.StringValue) dec(c, VC10, empty)).value());
        byte[] hello = enc(c, VC10, new ColumnValue.StringValue("hello"));
        assertEquals("hello", ((ColumnValue.StringValue) dec(c, VC10, hello)).value());
    }

    @Test
    void varRejectsTooLong() {
        VarBytesCodec c = new VarBytesCodec(10, false);
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c.validate(new ColumnValue.BinaryValue(new byte[11]), VB10));
    }

    @Test
    void byteOrderCompare() {
        VarBytesCodec c = new VarBytesCodec(10, true);
        byte[] abc = enc(c, VC10, new ColumnValue.StringValue("abc"));
        byte[] abd = enc(c, VC10, new ColumnValue.StringValue("abd"));
        byte[] ab = enc(c, VC10, new ColumnValue.StringValue("ab"));
        assertTrue(c.compare(new FieldSlice(abc, 0, abc.length), new FieldSlice(abd, 0, abd.length), VC10) < 0);
        assertTrue(c.compare(new FieldSlice(ab, 0, ab.length), new FieldSlice(abc, 0, abc.length), VC10) < 0);
    }
}
