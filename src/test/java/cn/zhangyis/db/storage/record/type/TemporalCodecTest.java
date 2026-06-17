package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TemporalCodec DATE/DATETIME 往返与排序（含 epoch 前负值）、kind 不匹配拒绝。 */
class TemporalCodecTest {

    private static final ColumnType DATE = ColumnType.date(false);
    private static final ColumnType DATETIME = ColumnType.datetime(false);

    private static byte[] enc(TemporalCodec c, ColumnType t, ColumnValue v) {
        byte[] buf = new byte[c.encodedLength(v, t)];
        c.encode(v, t, new FieldWriter(buf, 0));
        return buf;
    }

    private static long dec(TemporalCodec c, ColumnType t, byte[] b) {
        return ((ColumnValue.TemporalValue) c.decode(new FieldSlice(b, 0, b.length), t)).normalized();
    }

    private static int cmp(byte[] a, byte[] b) {
        return FieldSlice.compareUnsigned(new FieldSlice(a, 0, a.length), new FieldSlice(b, 0, b.length));
    }

    @Test
    void dateRoundTripAndOrder() {
        TemporalCodec c = new TemporalCodec(TemporalKind.DATE);
        for (long d : new long[] {0, 19000, -1, -100000}) {
            assertEquals(d, dec(c, DATE, enc(c, DATE, new ColumnValue.TemporalValue(TemporalKind.DATE, d))));
        }
        byte[] before = enc(c, DATE, new ColumnValue.TemporalValue(TemporalKind.DATE, -1));
        byte[] epoch = enc(c, DATE, new ColumnValue.TemporalValue(TemporalKind.DATE, 0));
        byte[] after = enc(c, DATE, new ColumnValue.TemporalValue(TemporalKind.DATE, 19000));
        assertTrue(cmp(before, epoch) < 0);
        assertTrue(cmp(epoch, after) < 0);
    }

    @Test
    void datetimeRoundTripAndOrder() {
        TemporalCodec c = new TemporalCodec(TemporalKind.DATETIME);
        for (long ms : new long[] {Long.MIN_VALUE, -1, 0, 1, 1700000000000L, Long.MAX_VALUE}) {
            assertEquals(ms, dec(c, DATETIME, enc(c, DATETIME, new ColumnValue.TemporalValue(TemporalKind.DATETIME, ms))));
        }
        byte[] neg = enc(c, DATETIME, new ColumnValue.TemporalValue(TemporalKind.DATETIME, -1));
        byte[] pos = enc(c, DATETIME, new ColumnValue.TemporalValue(TemporalKind.DATETIME, 1));
        assertTrue(cmp(neg, pos) < 0);
    }

    @Test
    void kindMismatchRejected() {
        TemporalCodec c = new TemporalCodec(TemporalKind.DATE);
        assertThrows(InvalidColumnValueException.class,
                () -> c.validate(new ColumnValue.TemporalValue(TemporalKind.DATETIME, 0), DATE));
    }
}
