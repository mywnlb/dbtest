package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TemporalCodec 五种时间类型的往返、保序、物理边界与 kind 不匹配拒绝。 */
class TemporalCodecTest {

    private static final ColumnType DATE = ColumnType.date(false);
    private static final ColumnType TIME = ColumnType.time(false);
    private static final ColumnType DATETIME = ColumnType.datetime(false);
    private static final ColumnType TIMESTAMP = ColumnType.timestamp(false);
    private static final ColumnType YEAR = ColumnType.year(false);

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

    /** 0.21e1 不能改变已经写入页/undo 的 DATE、DATETIME 保序字节。 */
    @Test
    void existingDateAndDatetimeBytesStayStable() {
        TemporalCodec date = new TemporalCodec(TemporalKind.DATE);
        assertArrayEquals(new byte[] {(byte) 0x80, 0, 0, 0},
                enc(date, DATE, new ColumnValue.TemporalValue(TemporalKind.DATE, 0)));
        TemporalCodec datetime = new TemporalCodec(TemporalKind.DATETIME);
        assertArrayEquals(new byte[] {(byte) 0x80, 0, 0, 0, 0, 0, 0, 0},
                enc(datetime, DATETIME, new ColumnValue.TemporalValue(TemporalKind.DATETIME, 0)));
    }

    /** TIME 是带符号毫秒 duration，完整 long 边界往返且编码字节保持自然序。 */
    @Test
    void timeRoundTripAndOrder() {
        TemporalCodec c = new TemporalCodec(TemporalKind.TIME);
        for (long ms : new long[] {Long.MIN_VALUE, -86_400_000L, -1, 0, 1, 86_400_000L, Long.MAX_VALUE}) {
            assertEquals(ms, dec(c, TIME, enc(c, TIME, new ColumnValue.TemporalValue(TemporalKind.TIME, ms))));
        }
        byte[] negative = enc(c, TIME, new ColumnValue.TemporalValue(TemporalKind.TIME, -1));
        byte[] zero = enc(c, TIME, new ColumnValue.TemporalValue(TemporalKind.TIME, 0));
        byte[] positive = enc(c, TIME, new ColumnValue.TemporalValue(TemporalKind.TIME, 1));
        assertTrue(cmp(negative, zero) < 0);
        assertTrue(cmp(zero, positive) < 0);
    }

    /** TIMESTAMP 只承载已归一化 UTC epoch millis；record 层不执行 session 时区转换。 */
    @Test
    void timestampRoundTripAndOrder() {
        TemporalCodec c = new TemporalCodec(TemporalKind.TIMESTAMP);
        for (long ms : new long[] {Long.MIN_VALUE, -1, 0, 1, 1_700_000_000_000L, Long.MAX_VALUE}) {
            assertEquals(ms, dec(c, TIMESTAMP,
                    enc(c, TIMESTAMP, new ColumnValue.TemporalValue(TemporalKind.TIMESTAMP, ms))));
        }
        byte[] beforeEpoch = enc(c, TIMESTAMP, new ColumnValue.TemporalValue(TemporalKind.TIMESTAMP, -1));
        byte[] afterEpoch = enc(c, TIMESTAMP, new ColumnValue.TemporalValue(TemporalKind.TIMESTAMP, 1));
        assertTrue(cmp(beforeEpoch, afterEpoch) < 0);
    }

    /** YEAR 使用 2B unsigned 教学编码，覆盖完整物理范围并拒绝越界。 */
    @Test
    void yearRoundTripOrderAndRange() {
        TemporalCodec c = new TemporalCodec(TemporalKind.YEAR);
        for (long year : new long[] {0, 1901, 2026, 2155, 65_535}) {
            byte[] encoded = enc(c, YEAR, new ColumnValue.TemporalValue(TemporalKind.YEAR, year));
            assertEquals(2, encoded.length);
            assertEquals(year, dec(c, YEAR, encoded));
        }
        byte[] early = enc(c, YEAR, new ColumnValue.TemporalValue(TemporalKind.YEAR, 1901));
        byte[] late = enc(c, YEAR, new ColumnValue.TemporalValue(TemporalKind.YEAR, 2026));
        assertTrue(cmp(early, late) < 0);
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c.validate(new ColumnValue.TemporalValue(TemporalKind.YEAR, -1), YEAR));
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c.validate(new ColumnValue.TemporalValue(TemporalKind.YEAR, 65_536), YEAR));
    }

    @Test
    void kindAndColumnTypeMismatchRejected() {
        TemporalCodec c = new TemporalCodec(TemporalKind.DATE);
        assertThrows(InvalidColumnValueException.class,
                () -> c.validate(new ColumnValue.TemporalValue(TemporalKind.DATETIME, 0), DATE));
        assertThrows(InvalidColumnValueException.class,
                () -> c.validate(new ColumnValue.TemporalValue(TemporalKind.DATE, 0), DATETIME));
    }
}
