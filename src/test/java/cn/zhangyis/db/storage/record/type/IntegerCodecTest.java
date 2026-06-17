package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** IntegerCodec 往返、范围、保序（编码字节无符号序 = 数值序）。 */
class IntegerCodecTest {

    private static final ColumnType INT_S = ColumnType.intType(false, false);
    private static final ColumnType TINY_U = ColumnType.tinyint(true, false);
    private static final ColumnType BIG_S = ColumnType.bigint(false, false);

    private static byte[] enc(TypeCodec c, ColumnType t, long v) {
        byte[] buf = new byte[c.encodedLength(new ColumnValue.IntValue(v), t)];
        c.encode(new ColumnValue.IntValue(v), t, new FieldWriter(buf, 0));
        return buf;
    }

    private static long dec(TypeCodec c, ColumnType t, byte[] b) {
        return ((ColumnValue.IntValue) c.decode(new FieldSlice(b, 0, b.length), t)).value();
    }

    private static int cmp(byte[] a, byte[] b) {
        return FieldSlice.compareUnsigned(new FieldSlice(a, 0, a.length), new FieldSlice(b, 0, b.length));
    }

    @Test
    void signedIntRoundTripAndOrder() {
        IntegerCodec c = new IntegerCodec(4, false);
        for (long v : new long[] {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 12345, -12345}) {
            assertEquals(v, dec(c, INT_S, enc(c, INT_S, v)));
        }
        assertTrue(cmp(enc(c, INT_S, Integer.MIN_VALUE), enc(c, INT_S, -1)) < 0);
        assertTrue(cmp(enc(c, INT_S, -1), enc(c, INT_S, 0)) < 0);
        assertTrue(cmp(enc(c, INT_S, 0), enc(c, INT_S, 1)) < 0);
        assertTrue(cmp(enc(c, INT_S, 1), enc(c, INT_S, Integer.MAX_VALUE)) < 0);
    }

    @Test
    void unsignedTinyintRoundTripAndOrder() {
        IntegerCodec c = new IntegerCodec(1, true);
        assertEquals(0L, dec(c, TINY_U, enc(c, TINY_U, 0)));
        assertEquals(255L, dec(c, TINY_U, enc(c, TINY_U, 255)));
        assertTrue(cmp(enc(c, TINY_U, 0), enc(c, TINY_U, 255)) < 0);
        assertTrue(cmp(enc(c, TINY_U, 127), enc(c, TINY_U, 128)) < 0);
    }

    @Test
    void bigintRoundTrip() {
        IntegerCodec c = new IntegerCodec(8, false);
        for (long v : new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
            assertEquals(v, dec(c, BIG_S, enc(c, BIG_S, v)));
        }
        assertTrue(cmp(enc(c, BIG_S, Long.MIN_VALUE), enc(c, BIG_S, Long.MAX_VALUE)) < 0);
    }

    @Test
    void rangeChecks() {
        IntegerCodec c4 = new IntegerCodec(4, false);
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c4.validate(new ColumnValue.IntValue(1L << 31), INT_S));
        IntegerCodec c1u = new IntegerCodec(1, true);
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c1u.validate(new ColumnValue.IntValue(256), TINY_U));
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c1u.validate(new ColumnValue.IntValue(-1), TINY_U));
        assertThrows(InvalidColumnValueException.class,
                () -> c4.validate(new ColumnValue.StringValue("x"), INT_S));
    }
}
