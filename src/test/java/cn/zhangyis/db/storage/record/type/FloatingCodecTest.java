package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** FloatingCodec 往返与排序（-Inf<负<-0=+0<正<+Inf<NaN，-0.0==+0.0）。 */
class FloatingCodecTest {

    private static final ColumnType DBL = ColumnType.doubleType(false);
    private static final ColumnType FLT = ColumnType.floatType(false);

    private static byte[] enc(TypeCodec c, ColumnType t, double v) {
        byte[] buf = new byte[c.encodedLength(new ColumnValue.DoubleValue(v), t)];
        c.encode(new ColumnValue.DoubleValue(v), t, new FieldWriter(buf, 0));
        return buf;
    }

    private static double dec(TypeCodec c, ColumnType t, byte[] b) {
        return ((ColumnValue.DoubleValue) c.decode(new FieldSlice(b, 0, b.length), t)).value();
    }

    private static int cmp(byte[] a, byte[] b) {
        return FieldSlice.compareUnsigned(new FieldSlice(a, 0, a.length), new FieldSlice(b, 0, b.length));
    }

    @Test
    void doubleRoundTrip() {
        FloatingCodec c = new FloatingCodec(8);
        for (double v : new double[] {0.0, 1.5, -1.5, Double.MIN_VALUE, Double.MAX_VALUE,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertEquals(v, dec(c, DBL, enc(c, DBL, v)));
        }
        assertTrue(Double.isNaN(dec(c, DBL, enc(c, DBL, Double.NaN))));
    }

    @Test
    void totalOrder() {
        FloatingCodec c = new FloatingCodec(8);
        byte[] negInf = enc(c, DBL, Double.NEGATIVE_INFINITY);
        byte[] neg = enc(c, DBL, -1.5);
        byte[] zero = enc(c, DBL, 0.0);
        byte[] pos = enc(c, DBL, 1.5);
        byte[] posInf = enc(c, DBL, Double.POSITIVE_INFINITY);
        byte[] nan = enc(c, DBL, Double.NaN);
        assertTrue(cmp(negInf, neg) < 0);
        assertTrue(cmp(neg, zero) < 0);
        assertTrue(cmp(zero, pos) < 0);
        assertTrue(cmp(pos, posInf) < 0);
        assertTrue(cmp(posInf, nan) < 0);
    }

    @Test
    void negativeZeroEqualsPositiveZero() {
        FloatingCodec c = new FloatingCodec(8);
        assertEquals(0, cmp(enc(c, DBL, -0.0), enc(c, DBL, 0.0)));
    }

    @Test
    void floatNarrowsAndRoundTrips() {
        FloatingCodec c = new FloatingCodec(4);
        assertEquals(1.5, dec(c, FLT, enc(c, FLT, 1.5)));
        assertEquals(-2.25, dec(c, FLT, enc(c, FLT, -2.25)));
        assertTrue(cmp(enc(c, FLT, -2.25), enc(c, FLT, 1.5)) < 0);
    }
}
