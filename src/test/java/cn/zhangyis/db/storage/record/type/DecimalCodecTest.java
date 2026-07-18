package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DecimalCodec 往返、保序、scale/precision 校验、列宽。 */
class DecimalCodecTest {

    private static final ColumnType DEC = ColumnType.decimal(10, 2, false);

    private static byte[] enc(DecimalCodec c, BigDecimal v) {
        byte[] buf = new byte[c.encodedLength(new ColumnValue.DecimalValue(v), DEC)];
        c.encode(new ColumnValue.DecimalValue(v), DEC, new FieldWriter(buf, 0));
        return buf;
    }

    private static BigDecimal dec(DecimalCodec c, byte[] b) {
        return ((ColumnValue.DecimalValue) c.decode(new FieldSlice(b, 0, b.length), DEC)).value();
    }

    private static int cmp(byte[] a, byte[] b) {
        return FieldSlice.compareUnsigned(new FieldSlice(a, 0, a.length), new FieldSlice(b, 0, b.length));
    }

    /**
     * 验证 {@code roundTripAndWidth} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void roundTripAndWidth() {
        DecimalCodec c = new DecimalCodec(10, 2);
        assertEquals(5, c.encodedLength(new ColumnValue.DecimalValue(new BigDecimal("0.00")), DEC));
        assertEquals(new BigDecimal("123.45"), dec(c, enc(c, new BigDecimal("123.45"))));
        assertEquals(new BigDecimal("-123.45"), dec(c, enc(c, new BigDecimal("-123.45"))));
        assertEquals(new BigDecimal("0.00"), dec(c, enc(c, new BigDecimal("0.00"))));
    }

    /**
     * 验证 {@code orderPreserving} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void orderPreserving() {
        DecimalCodec c = new DecimalCodec(10, 2);
        assertTrue(cmp(enc(c, new BigDecimal("-1.00")), enc(c, new BigDecimal("0.00"))) < 0);
        assertTrue(cmp(enc(c, new BigDecimal("0.00")), enc(c, new BigDecimal("1.00"))) < 0);
        assertTrue(cmp(enc(c, new BigDecimal("99.99")), enc(c, new BigDecimal("100.00"))) < 0);
    }

    /**
     * 验证 {@code rejectsScaleAndPrecisionOverflow} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsScaleAndPrecisionOverflow() {
        DecimalCodec c = new DecimalCodec(10, 2);
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c.validate(new ColumnValue.DecimalValue(new BigDecimal("1.234")), DEC));
        assertThrows(ColumnValueOutOfRangeException.class,
                () -> c.validate(new ColumnValue.DecimalValue(new BigDecimal("100000000.00")), DEC));
        assertThrows(InvalidColumnValueException.class,
                () -> c.validate(new ColumnValue.IntValue(1), DEC));
    }
}
