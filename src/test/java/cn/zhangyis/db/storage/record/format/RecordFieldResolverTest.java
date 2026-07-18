package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 列级解析器：按列序取 NULL/切片/值，整条 materialize 与 RecordDecoder 一致。 */
class RecordFieldResolverTest {

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordEncoder encoder = new RecordEncoder(registry);
    private final RecordFieldResolver resolver = new RecordFieldResolver(registry);

    private static ColumnDef col(int ordinal, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ordinal), name, type, ordinal);
    }

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                col(0, "id", ColumnType.intType(false, false)),
                col(1, "name", ColumnType.varchar(20, true)),
                col(2, "amt", ColumnType.decimal(10, 2, false))));
    }

    /**
     * 验证 {@code resolvesColumnsWithPresentNullable} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void resolvesColumnsWithPresentNullable() {
        TableSchema s = schema();
        List<ColumnValue> vals = List.of(
                new ColumnValue.IntValue(7),
                new ColumnValue.StringValue("hi"),
                new ColumnValue.DecimalValue(new BigDecimal("3.14")));
        byte[] bytes = encoder.encode(new LogicalRecord(1, vals, false, RecordType.CONVENTIONAL), s);

        RecordFieldResolver.Resolved r = resolver.resolve(bytes, s);
        assertFalse(r.isNull(0));
        assertFalse(r.isNull(1));
        assertFalse(r.isNull(2));
        assertEquals(new ColumnValue.IntValue(7), r.value(0));
        assertEquals(new ColumnValue.StringValue("hi"), r.value(1));
        assertEquals(0, ((ColumnValue.DecimalValue) r.value(2)).value().compareTo(new BigDecimal("3.14")));
        assertEquals(4, r.slice(0).length());  // INT 定长 4 字节
        assertEquals(2, r.slice(1).length());  // VARCHAR "hi" = 2 字节
        assertEquals(5, r.slice(2).length());  // DECIMAL(10,2) = 5 字节

        // materialize 与 RecordDecoder 一致（decode 现委托同一 resolver）。
        assertEquals(new RecordDecoder(registry).decode(bytes, s), r.materialize());
    }

    /**
     * 验证 {@code nullColumnHasNoSlice} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void nullColumnHasNoSlice() {
        TableSchema s = schema();
        List<ColumnValue> vals = Arrays.asList(
                new ColumnValue.IntValue(9),
                ColumnValue.NullValue.INSTANCE,
                new ColumnValue.DecimalValue(new BigDecimal("1.00")));
        byte[] bytes = encoder.encode(new LogicalRecord(1, vals, false, RecordType.CONVENTIONAL), s);

        RecordFieldResolver.Resolved r = resolver.resolve(bytes, s);
        assertTrue(r.isNull(1));
        assertInstanceOf(ColumnValue.NullValue.class, r.value(1));
        assertThrows(RecordFormatException.class, () -> r.slice(1));
        assertFalse(r.isNull(0));
        assertEquals(new ColumnValue.IntValue(9), r.value(0));
    }
}
