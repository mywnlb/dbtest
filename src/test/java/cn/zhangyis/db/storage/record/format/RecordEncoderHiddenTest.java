package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 聚簇隐藏区编解码：clustered 追加 15B、一致性拒绝、往返带隐藏列且 columnValues 干净、尾部位置校验
 * （通过 clustered/非 clustered schema 交叉解码触发）。
 */
class RecordEncoderHiddenTest {

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final RecordEncoder encoder = new RecordEncoder(registry);
    private final RecordDecoder decoder = new RecordDecoder(registry);

    private static ColumnDef col(int ord, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ord), name, type, ord);
    }

    private TableSchema clustered() {
        return new TableSchema(1, List.of(col(0, "id", ColumnType.intType(false, false))), true);
    }

    private TableSchema nonClustered() {
        return new TableSchema(1, List.of(col(0, "id", ColumnType.intType(false, false))));
    }

    private List<ColumnValue> vals() {
        return List.of(new ColumnValue.IntValue(7));
    }

    private HiddenColumns hidden() {
        return new HiddenColumns(TransactionId.of(0x99), new RollPointer(true, PageNo.of(3), 4));
    }

    /**
     * 验证 {@code clusteredAppendsFifteenBytes} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void clusteredAppendsFifteenBytes() {
        byte[] plain = encoder.encode(
                new LogicalRecord(1, vals(), false, RecordType.CONVENTIONAL), nonClustered());
        byte[] withHidden = encoder.encode(
                new LogicalRecord(1, vals(), false, RecordType.CONVENTIONAL, hidden()), clustered());
        assertEquals(plain.length + HiddenColumnLayout.HIDDEN_BYTES, withHidden.length);
    }

    /**
     * 验证 {@code clusteredSchemaButNoHiddenRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void clusteredSchemaButNoHiddenRejected() {
        LogicalRecord noHidden = new LogicalRecord(1, vals(), false, RecordType.CONVENTIONAL);
        assertThrows(DatabaseValidationException.class, () -> encoder.encode(noHidden, clustered()));
    }

    /**
     * 验证 {@code nonClusteredWithHiddenRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void nonClusteredWithHiddenRejected() {
        LogicalRecord withHidden = new LogicalRecord(1, vals(), false, RecordType.CONVENTIONAL, hidden());
        assertThrows(DatabaseValidationException.class, () -> encoder.encode(withHidden, nonClustered()));
    }

    /**
     * 验证 {@code clusteredRoundTripCarriesHiddenAndKeepsColumnsClean} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void clusteredRoundTripCarriesHiddenAndKeepsColumnsClean() {
        HiddenColumns h = hidden();
        byte[] buf = encoder.encode(new LogicalRecord(1, vals(), false, RecordType.CONVENTIONAL, h), clustered());
        LogicalRecord back = decoder.decode(buf, clustered());
        assertEquals(h, back.hiddenColumns());
        assertEquals(vals(), back.columnValues());
    }

    /**
     * 验证 {@code nonClusteredRoundTripHasNoHidden} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void nonClusteredRoundTripHasNoHidden() {
        byte[] buf = encoder.encode(new LogicalRecord(1, vals(), false, RecordType.CONVENTIONAL), nonClustered());
        LogicalRecord back = decoder.decode(buf, nonClustered());
        assertNull(back.hiddenColumns());
    }

    /**
     * 验证 {@code decoderRejectsClusteredSchemaOnNonClusteredBytes} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void decoderRejectsClusteredSchemaOnNonClusteredBytes() {
        byte[] plain = encoder.encode(
                new LogicalRecord(1, vals(), false, RecordType.CONVENTIONAL), nonClustered());
        assertThrows(RecordFormatException.class, () -> decoder.decode(plain, clustered()));
    }

    /**
     * 验证 {@code decoderRejectsNonClusteredSchemaOnClusteredBytes} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void decoderRejectsNonClusteredSchemaOnClusteredBytes() {
        byte[] withHidden = encoder.encode(
                new LogicalRecord(1, vals(), false, RecordType.CONVENTIONAL, hidden()), clustered());
        assertThrows(RecordFormatException.class, () -> decoder.decode(withHidden, nonClustered()));
    }
}
