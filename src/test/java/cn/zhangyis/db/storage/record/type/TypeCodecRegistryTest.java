package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.storage.record.schema.ColumnType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** TypeCodecRegistry 按列类型返回正确 codec（类型与编码宽度）。 */
class TypeCodecRegistryTest {

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    @Test
    void picksIntegerCodecsWithRightWidth() {
        assertEquals(1, registry.codecFor(ColumnType.tinyint(false, false))
                .encodedLength(new ColumnValue.IntValue(0), ColumnType.tinyint(false, false)));
        assertEquals(8, registry.codecFor(ColumnType.bigint(false, false))
                .encodedLength(new ColumnValue.IntValue(0), ColumnType.bigint(false, false)));
        assertInstanceOf(IntegerCodec.class, registry.codecFor(ColumnType.intType(false, false)));
    }

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
    }

    @Test
    void picksBytesCodecs() {
        ColumnType ch = ColumnType.charType(5, false);
        assertEquals(5, registry.codecFor(ch).encodedLength(new ColumnValue.StringValue("a"), ch));
        assertInstanceOf(FixedBytesCodec.class, registry.codecFor(ColumnType.binary(4, false)));
        assertInstanceOf(VarBytesCodec.class, registry.codecFor(ColumnType.varchar(10, false)));
        assertInstanceOf(VarBytesCodec.class, registry.codecFor(ColumnType.varbinary(10, false)));
    }
}
