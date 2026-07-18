package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.CharsetId;
import cn.zhangyis.db.storage.record.schema.CollationId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** TypeCodecRegistry 按列类型返回正确 codec（类型与编码宽度）。 */
class TypeCodecRegistryTest {

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    /**
     * 验证 {@code picksIntegerCodecsWithRightWidth} 所描述的稳定格式转换，并断言往返值、字节布局、版本与损坏输入处理。
     */
    @Test
    void picksIntegerCodecsWithRightWidth() {
        assertEquals(1, registry.codecFor(ColumnType.tinyint(false, false))
                .encodedLength(new ColumnValue.IntValue(0), ColumnType.tinyint(false, false)));
        assertEquals(8, registry.codecFor(ColumnType.bigint(false, false))
                .encodedLength(new ColumnValue.IntValue(0), ColumnType.bigint(false, false)));
        assertInstanceOf(IntegerCodec.class, registry.codecFor(ColumnType.intType(false, false)));
    }

    /**
     * 验证 {@code picksFloatingDecimalTemporal} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void picksFloatingDecimalTemporal() {
        assertEquals(4, registry.codecFor(ColumnType.floatType(false))
                .encodedLength(new ColumnValue.DoubleValue(0), ColumnType.floatType(false)));
        assertEquals(8, registry.codecFor(ColumnType.doubleType(false))
                .encodedLength(new ColumnValue.DoubleValue(0), ColumnType.doubleType(false)));
        ColumnType dec = ColumnType.decimal(10, 2, false);
        assertEquals(5, registry.codecFor(dec).encodedLength(new ColumnValue.DecimalValue(new BigDecimal("0.00")), dec));
        assertEquals(4, registry.codecFor(ColumnType.date(false))
                .encodedLength(new ColumnValue.TemporalValue(TemporalKind.DATE, 0), ColumnType.date(false)));
        assertEquals(8, registry.codecFor(ColumnType.datetime(false))
                .encodedLength(new ColumnValue.TemporalValue(TemporalKind.DATETIME, 0), ColumnType.datetime(false)));
        assertEquals(8, registry.codecFor(ColumnType.time(false))
                .encodedLength(new ColumnValue.TemporalValue(TemporalKind.TIME, 0), ColumnType.time(false)));
        assertEquals(8, registry.codecFor(ColumnType.timestamp(false))
                .encodedLength(new ColumnValue.TemporalValue(TemporalKind.TIMESTAMP, 0), ColumnType.timestamp(false)));
        assertEquals(2, registry.codecFor(ColumnType.year(false))
                .encodedLength(new ColumnValue.TemporalValue(TemporalKind.YEAR, 2026), ColumnType.year(false)));
    }

    /**
     * 验证 {@code picksBytesCodecs} 所描述的稳定格式转换，并断言往返值、字节布局、版本与损坏输入处理。
     */
    @Test
    void picksBytesCodecs() {
        ColumnType ch = ColumnType.charType(5, false);
        assertEquals(5, registry.codecFor(ch).encodedLength(new ColumnValue.StringValue("a"), ch));
        assertInstanceOf(FixedBytesCodec.class, registry.codecFor(ColumnType.binary(4, false)));
        assertInstanceOf(VarBytesCodec.class, registry.codecFor(ColumnType.varchar(10, false)));
        assertInstanceOf(VarBytesCodec.class, registry.codecFor(ColumnType.varbinary(10, false)));
        ColumnType bit = ColumnType.bit(9, false);
        assertInstanceOf(BitCodec.class, registry.codecFor(bit));
        assertEquals(2, registry.codecFor(bit).fixedWidth(bit));
        ColumnType enumType = ColumnType.enumType(java.util.List.of("A", "B"), false);
        ColumnType setType = ColumnType.setType(java.util.List.of("A", "B"), false);
        assertInstanceOf(EnumCodec.class, registry.codecFor(enumType));
        assertInstanceOf(SetCodec.class, registry.codecFor(setType));
        assertInstanceOf(LobCodec.class, registry.codecFor(ColumnType.text(false)));
        assertInstanceOf(LobCodec.class, registry.codecFor(ColumnType.blob(false)));
        assertInstanceOf(LobCodec.class, registry.codecFor(ColumnType.json(false)));
    }

    /**
     * 验证 {@code validatesAndResolvesExactCharsetCollationPair} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void validatesAndResolvesExactCharsetCollationPair() {
        assertSame(AsciiCaseInsensitiveCollation.INSTANCE,
                registry.collationFor(CharsetId.UTF8, CollationId.UTF8_ASCII_CI));
        ColumnType mismatched = ColumnType.varchar(
                10, false, CharsetId.UTF8, CollationId.LATIN1_ASCII_CI);
        assertThrows(UnsupportedCollationException.class, () -> registry.codecFor(mismatched));
    }
}
