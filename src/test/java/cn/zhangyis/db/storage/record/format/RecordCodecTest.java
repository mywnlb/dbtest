package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.schema.TypeId;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TemporalKind;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 记录编解码端到端：encode → decode 往返，覆盖定长、变长、NULL、DECIMAL/浮点/时间负值，
 * 以及 schemaVersion 不匹配、非空列写 NULL、列数不符、recordLength 损坏等拒绝路径。
 */
class RecordCodecTest {

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordEncoder encoder = new RecordEncoder(registry);
    private final RecordDecoder decoder = new RecordDecoder(registry);

    /** 构造列定义；ordinal 与 id 必须等于位置（TableSchema 约束）。 */
    private static ColumnDef col(int ordinal, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ordinal), name, type, ordinal);
    }

    private static TableSchema schema(long version, ColumnDef... cols) {
        return new TableSchema(version, List.of(cols));
    }

    /** 全定长列（含数值与五种时间类型）往返一致，无变长目录。 */
    @Test
    void fixedOnlyRoundTrip() {
        TableSchema s = schema(1,
                col(0, "a", ColumnType.intType(false, false)),
                col(1, "b", ColumnType.bigint(true, false)),
                col(2, "c", ColumnType.doubleType(false)),
                col(3, "d", ColumnType.date(false)),
                col(4, "e", ColumnType.time(false)),
                col(5, "f", ColumnType.datetime(false)),
                col(6, "g", ColumnType.timestamp(false)),
                col(7, "h", ColumnType.year(false)));
        List<ColumnValue> vals = List.of(
                new ColumnValue.IntValue(-12345),
                new ColumnValue.IntValue(-1L),                 // unsigned 64 位以原始 bits 承载
                new ColumnValue.DoubleValue(3.141592653589793),
                new ColumnValue.TemporalValue(TemporalKind.DATE, -100),
                new ColumnValue.TemporalValue(TemporalKind.TIME, -86_400_000L),
                new ColumnValue.TemporalValue(TemporalKind.DATETIME, -86_400_000L),
                new ColumnValue.TemporalValue(TemporalKind.TIMESTAMP, 1_700_000_000_000L),
                new ColumnValue.TemporalValue(TemporalKind.YEAR, 2026));
        assertRoundTrip(s, new LogicalRecord(1, vals, false, RecordType.CONVENTIONAL));
    }

    /** 变长列（VARCHAR/VARBINARY）与定长 CHAR/BINARY/INT 混合往返。 */
    @Test
    void variableAndFixedRoundTrip() {
        TableSchema s = schema(7,
                col(0, "id", ColumnType.intType(false, false)),
                col(1, "name", ColumnType.varchar(32, false)),
                col(2, "blob", ColumnType.varbinary(16, false)),
                col(3, "code", ColumnType.charType(4, false)),
                col(4, "raw", ColumnType.binary(3, false)));
        List<ColumnValue> vals = List.of(
                new ColumnValue.IntValue(42),
                new ColumnValue.StringValue("héllo"),         // 多字节 UTF-8
                new ColumnValue.BinaryValue(new byte[]{1, 2, 3, 4, 5}),
                new ColumnValue.StringValue("ab"),            // CHAR(4) 编码补空格、解码去尾空格 → "ab"
                new ColumnValue.BinaryValue(new byte[]{9, 8, 7})); // BINARY(3) 恰好 3 字节，往返一致
        assertRoundTrip(s, new LogicalRecord(7, vals, false, RecordType.CONVENTIONAL));
    }

    /** 含 NULL：可空列写 NULL，解码回 NullValue；非空列正常往返。 */
    @Test
    void nullsRoundTrip() {
        TableSchema s = schema(3,
                col(0, "a", ColumnType.intType(false, true)),
                col(1, "b", ColumnType.varchar(20, true)),
                col(2, "c", ColumnType.intType(false, false)),
                col(3, "d", ColumnType.varchar(20, true)));
        List<ColumnValue> vals = new ArrayList<>();
        vals.add(ColumnValue.NullValue.INSTANCE);
        vals.add(ColumnValue.NullValue.INSTANCE);
        vals.add(new ColumnValue.IntValue(99));
        vals.add(new ColumnValue.StringValue("present"));
        LogicalRecord decoded = assertRoundTrip(s, new LogicalRecord(3, vals, false, RecordType.CONVENTIONAL));
        assertInstanceOf(ColumnValue.NullValue.class, decoded.columnValues().get(0));
        assertInstanceOf(ColumnValue.NullValue.class, decoded.columnValues().get(1));
    }

    /** DECIMAL、FLOAT 负值与边界往返（FLOAT 取可精确表示的值）。 */
    @Test
    void decimalFloatRoundTrip() {
        TableSchema s = schema(5,
                col(0, "p", ColumnType.decimal(10, 2, false)),
                col(1, "q", ColumnType.decimal(10, 2, false)),
                col(2, "f", ColumnType.floatType(false)),
                col(3, "g", ColumnType.floatType(false)));
        List<ColumnValue> vals = List.of(
                new ColumnValue.DecimalValue(new BigDecimal("123.45")),
                new ColumnValue.DecimalValue(new BigDecimal("-7.10")),
                new ColumnValue.DoubleValue(-2.25),
                new ColumnValue.DoubleValue(0.0));
        assertRoundTrip(s, new LogicalRecord(5, vals, false, RecordType.CONVENTIONAL));
    }

    /** BIT(n) 通过真实 fixed-area 布局往返，非 byte-aligned canonical 尾位保持不变。 */
    @Test
    void bitRoundTrip() {
        TableSchema s = schema(6,
                col(0, "flags", ColumnType.bit(9, false)),
                col(1, "mask", ColumnType.bit(64, false)));
        List<ColumnValue> vals = List.of(
                new ColumnValue.BitValue(new byte[] {(byte) 0xA5, (byte) 0x80}),
                new ColumnValue.BitValue(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
        assertRoundTrip(s, new LogicalRecord(6, vals, false, RecordType.CONVENTIONAL));
    }

    /** ENUM ordinal 与 SET bitmap 进入 fixed area，并按 schema 字典宽度往返。 */
    @Test
    void enumAndSetRoundTrip() {
        TableSchema s = schema(8,
                col(0, "state", ColumnType.enumType(List.of("NEW", "DONE"), false)),
                col(1, "permissions", ColumnType.setType(List.of("READ", "WRITE", "ADMIN"), false)));
        assertRoundTrip(s, new LogicalRecord(8, List.of(
                new ColumnValue.EnumValue(2),
                new ColumnValue.SetValue(0b101)), false, RecordType.CONVENTIONAL));
    }

    /** overflow-capable 列和普通 VARCHAR 共用变长目录；external envelope 不把完整 payload 塞回记录。 */
    @Test
    void inlineAndExternalLobRoundTripThroughVariableDirectory() {
        TableSchema s = schema(9,
                col(0, "summary", ColumnType.text(false)),
                col(1, "payload", ColumnType.longBlob(false)));
        LobReference reference = new LobReference(SpaceId.of(8), PageNo.of(64), 90_000, 6,
                SegmentId.of(3), 1, 1234);
        byte[] prefix = new byte[]{1, 2, 3};
        LogicalRecord record = new LogicalRecord(9, List.of(
                new ColumnValue.StringValue("inline text"),
                new ColumnValue.ExternalValue(TypeId.LONGBLOB, reference, prefix)),
                false, RecordType.CONVENTIONAL);

        LogicalRecord decoded = decoder.decode(encoder.encode(record, s), s);
        assertEquals(new ColumnValue.StringValue("inline text"), decoded.columnValues().get(0));
        ColumnValue.ExternalValue external = assertInstanceOf(
                ColumnValue.ExternalValue.class, decoded.columnValues().get(1));
        assertEquals(TypeId.LONGBLOB, external.typeId());
        assertEquals(reference, external.reference());
        assertArrayEquals(prefix, external.inlinePrefix());
    }

    /** delete-mark 与 recordType 在往返中保留。 */
    @Test
    void deleteMarkAndTypePreserved() {
        TableSchema s = schema(1, col(0, "a", ColumnType.intType(false, false)));
        LogicalRecord rec = new LogicalRecord(1, List.of(new ColumnValue.IntValue(1)), true, RecordType.NODE_POINTER);
        byte[] buf = encoder.encode(rec, s);
        LogicalRecord back = decoder.decode(buf, s);
        assertTrue(back.deleted());
        assertEquals(RecordType.NODE_POINTER, back.recordType());
    }

    /** record 的 schemaVersion 与 schema 不一致 → 拒绝。 */
    @Test
    void rejectsSchemaVersionMismatch() {
        TableSchema s = schema(1, col(0, "a", ColumnType.intType(false, false)));
        LogicalRecord rec = new LogicalRecord(2, List.of(new ColumnValue.IntValue(1)), false, RecordType.CONVENTIONAL);
        assertThrows(SchemaVersionMismatchException.class, () -> encoder.encode(rec, s));
    }

    /** 非空列写 NULL → 拒绝。 */
    @Test
    void rejectsNullForNonNullable() {
        TableSchema s = schema(1, col(0, "a", ColumnType.intType(false, false)));
        LogicalRecord rec = new LogicalRecord(1, List.of(ColumnValue.NullValue.INSTANCE), false, RecordType.CONVENTIONAL);
        assertThrows(RecordFormatException.class, () -> encoder.encode(rec, s));
    }

    /** 列值数量与 schema 列数不符 → 拒绝。 */
    @Test
    void rejectsColumnCountMismatch() {
        TableSchema s = schema(1,
                col(0, "a", ColumnType.intType(false, false)),
                col(1, "b", ColumnType.intType(false, false)));
        LogicalRecord rec = new LogicalRecord(1, List.of(new ColumnValue.IntValue(1)), false, RecordType.CONVENTIONAL);
        assertThrows(RecordFormatException.class, () -> encoder.encode(rec, s));
    }

    /** 头部 recordLength 与缓冲区实际长度不符（截断/损坏）→ 解码拒绝。 */
    @Test
    void rejectsRecordLengthMismatchOnDecode() {
        TableSchema s = schema(1, col(0, "a", ColumnType.bigint(false, false)));
        byte[] buf = encoder.encode(
                new LogicalRecord(1, List.of(new ColumnValue.IntValue(7)), false, RecordType.CONVENTIONAL), s);
        byte[] truncated = Arrays.copyOf(buf, buf.length - 1);
        assertThrows(RecordFormatException.class, () -> decoder.decode(truncated, s));
    }

    /** 编码后解码，逐列断言一致，并校验 recordLength == 缓冲区长度。返回解码结果供进一步断言。 */
    private LogicalRecord assertRoundTrip(TableSchema s, LogicalRecord rec) {
        byte[] buf = encoder.encode(rec, s);
        assertEquals(buf.length, RecordHeader.readFrom(buf, 0).recordLength());
        LogicalRecord back = decoder.decode(buf, s);
        assertEquals(rec.columnValues().size(), back.columnValues().size());
        for (int i = 0; i < rec.columnValues().size(); i++) {
            assertValueEquals(rec.columnValues().get(i), back.columnValues().get(i), i);
        }
        return back;
    }

    /** 值相等断言：BinaryValue 比字节、DecimalValue 比数值，其余用 equals。 */
    private static void assertValueEquals(ColumnValue expected, ColumnValue actual, int ordinal) {
        if (expected instanceof ColumnValue.BinaryValue eb) {
            assertInstanceOf(ColumnValue.BinaryValue.class, actual, "col " + ordinal);
            assertArrayEquals(eb.value(), ((ColumnValue.BinaryValue) actual).value(), "col " + ordinal);
        } else if (expected instanceof ColumnValue.BitValue eb) {
            assertInstanceOf(ColumnValue.BitValue.class, actual, "col " + ordinal);
            assertArrayEquals(eb.value(), ((ColumnValue.BitValue) actual).value(), "col " + ordinal);
        } else if (expected instanceof ColumnValue.DecimalValue ed) {
            assertInstanceOf(ColumnValue.DecimalValue.class, actual, "col " + ordinal);
            assertEquals(0, ed.value().compareTo(((ColumnValue.DecimalValue) actual).value()), "col " + ordinal);
        } else {
            assertEquals(expected, actual, "col " + ordinal);
        }
    }
}
