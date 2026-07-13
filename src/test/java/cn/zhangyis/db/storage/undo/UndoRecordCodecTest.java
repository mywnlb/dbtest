package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.schema.TypeId;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TemporalKind;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** T1.3a undo 枚举落盘值钉死 + UndoRecord 构造校验 + UndoRecordCodec 往返/损坏拒绝。 */
class UndoRecordCodecTest {

    // ---- Task 3：枚举落盘值钉死 ----

    @Test void undoLogKindOrdinalsStable() {
        assertEquals(0, UndoLogKind.INSERT.ordinal());
        assertEquals(1, UndoLogKind.UPDATE.ordinal());
        assertEquals(2, UndoLogKind.TEMPORARY.ordinal());
    }

    @Test void undoRecordTypeCodesStable() {
        assertEquals(1, UndoRecordType.INSERT_ROW.code());
        assertEquals(2, UndoRecordType.UPDATE_ROW.code());
        assertEquals(3, UndoRecordType.DELETE_MARK.code());
        assertEquals(UndoRecordType.INSERT_ROW, UndoRecordType.fromCode(1));
        assertThrows(DatabaseValidationException.class, () -> UndoRecordType.fromCode(99));
    }

    // UndoRecord 构造校验（NONE undoNo / 空 key / 类型 old image 可空性 / DELETE_MARK 拒绝）见 UndoRecordTest。

    // ---- codec 往返/损坏拒绝 ----

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema twoColSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(64, true), 1)), true);
    }
    // 复合 key：(id, name)，覆盖 int + varchar。
    private static IndexKeyDef twoColKey() {
        return new IndexKeyDef(9L, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0)));
    }
    private UndoRecord rec(List<ColumnValue> key, RollPointer prev) {
        return UndoRecord.insert(UndoNo.of(5), TransactionId.of(0x1122334455L), 7L, 9L, key, prev);
    }

    @Test void roundTripsTwoColKeyNullRollPtr() {
        UndoRecord r = rec(List.of(new ColumnValue.IntValue(42),
                new ColumnValue.StringValue("alice")), RollPointer.NULL);
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(r, twoColKey(), twoColSchema());
        UndoRecord back = codec.decode(buf, 0, twoColKey(), twoColSchema());
        assertEquals(r, back);
    }

    @Test void roundTripsNonNullPrevAndNullKeyColumn() {
        RollPointer prev = new RollPointer(true, PageNo.of(0x01020304L), 0xABCD);
        UndoRecord r = rec(List.of(new ColumnValue.IntValue(1),
                ColumnValue.NullValue.INSTANCE), prev);
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        UndoRecord back = codec.decode(codec.encode(r, twoColKey(), twoColSchema()), 0,
                twoColKey(), twoColSchema());
        assertEquals(r, back);
        assertTrue(back.prevRollPointer().insert());
        assertEquals(ColumnValue.NullValue.INSTANCE, back.clusterKey().get(1));
    }

    /** 新时间标量必须经 UndoRecordCodec 的真实 self-framing key payload 往返，不能只在 RecordCodec 生效。 */
    @Test void roundTripsInlineTemporalClusterKey() {
        TableSchema schema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "duration", ColumnType.time(false), 0),
                new ColumnDef(new ColumnId(1), "created_at", ColumnType.timestamp(false), 1),
                new ColumnDef(new ColumnId(2), "year_value", ColumnType.year(false), 2)), true);
        IndexKeyDef keyDef = new IndexKeyDef(9L, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(2), KeyOrder.ASC, 0)));
        List<ColumnValue> key = List.of(
                new ColumnValue.TemporalValue(TemporalKind.TIME, -86_400_000L),
                new ColumnValue.TemporalValue(TemporalKind.TIMESTAMP, 1_700_000_000_000L),
                new ColumnValue.TemporalValue(TemporalKind.YEAR, 2026));
        UndoRecord expected = rec(key, RollPointer.NULL);
        UndoRecordCodec codec = new UndoRecordCodec(registry);

        assertEquals(expected, codec.decode(codec.encode(expected, keyDef, schema), 0, keyDef, schema));
    }

    @Test void decodeRejectsTruncated() {
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(rec(List.of(new ColumnValue.IntValue(1),
                new ColumnValue.StringValue("x")), RollPointer.NULL), twoColKey(), twoColSchema());
        byte[] cut = Arrays.copyOf(buf, buf.length - 1);
        assertThrows(UndoLogFormatException.class, () -> codec.decode(cut, 0, twoColKey(), twoColSchema()));
    }

    @Test void decodeRejectsKeyColCountMismatch() {
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(rec(List.of(new ColumnValue.IntValue(1),
                new ColumnValue.StringValue("x")), RollPointer.NULL), twoColKey(), twoColSchema());
        buf[40] = 1; // keyColCount 字节：type(1)+undoNo(8)+trx(8)+table(8)+index(8)+rollPtr(7)
        assertThrows(UndoLogFormatException.class, () -> codec.decode(buf, 0, twoColKey(), twoColSchema()));
    }

    @Test void decodeRejectsUnknownTypeOnDisk() {
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(rec(List.of(new ColumnValue.IntValue(1),
                new ColumnValue.StringValue("x")), RollPointer.NULL), twoColKey(), twoColSchema());
        buf[0] = (byte) 99; // 未知 type code
        assertThrows(UndoLogFormatException.class, () -> codec.decode(buf, 0, twoColKey(), twoColSchema()));
    }

    // ---- T1.3e：UPDATE_ROW 往返 + 全量旧 image + 损坏 + INSERT golden bytes ----

    private static final cn.zhangyis.db.storage.record.format.HiddenColumns OLD_HIDDEN =
            new cn.zhangyis.db.storage.record.format.HiddenColumns(
                    TransactionId.of(3), new RollPointer(false, PageNo.of(0x0A0B), 0x22));

    private UndoRecord updateRec(List<ColumnValue> key, List<ColumnValue> oldRow, RollPointer prev) {
        return UndoRecord.update(UndoNo.of(6), TransactionId.of(0x99), 7L, 9L, key, oldRow, OLD_HIDDEN, prev);
    }

    @Test void roundTripsUpdateRowFullOldImage() {
        UndoRecord r = updateRec(
                List.of(new ColumnValue.IntValue(42), new ColumnValue.StringValue("alice")),
                List.of(new ColumnValue.IntValue(42), new ColumnValue.StringValue("alice-old")),
                new RollPointer(false, PageNo.of(70), 5));
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(r, twoColKey(), twoColSchema());
        UndoRecord back = codec.decode(buf, 0, twoColKey(), twoColSchema());
        assertEquals(r, back);
        assertEquals(UndoRecordType.UPDATE_ROW, back.type());
        assertEquals(OLD_HIDDEN, back.oldHiddenColumns(), "old hidden columns (版本链上一版本指针) round-trips");
        assertEquals("alice-old", ((ColumnValue.StringValue) back.oldColumnValues().get(1)).value());
    }

    @Test void roundTripsDeleteMarkRowFullOldImage() {
        UndoRecord r = UndoRecord.deleteMark(UndoNo.of(7), TransactionId.of(0x99), 7L, 9L,
                List.of(new ColumnValue.IntValue(42), new ColumnValue.StringValue("alice")),
                List.of(new ColumnValue.IntValue(42), new ColumnValue.StringValue("alice")),
                OLD_HIDDEN, new RollPointer(false, PageNo.of(70), 5));
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        UndoRecord back = codec.decode(codec.encode(r, twoColKey(), twoColSchema()), 0, twoColKey(), twoColSchema());
        assertEquals(r, back);
        assertEquals(UndoRecordType.DELETE_MARK, back.type());
        assertEquals(OLD_HIDDEN, back.oldHiddenColumns());
    }

    @Test void updateRoundTripsNullColumnInOldImage() {
        UndoRecord r = updateRec(
                List.of(new ColumnValue.IntValue(1), new ColumnValue.StringValue("k")),
                List.of(new ColumnValue.IntValue(1), ColumnValue.NullValue.INSTANCE),
                RollPointer.NULL);
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        UndoRecord back = codec.decode(codec.encode(r, twoColKey(), twoColSchema()), 0, twoColKey(), twoColSchema());
        assertEquals(ColumnValue.NullValue.INSTANCE, back.oldColumnValues().get(1));
    }

    /** UPDATE undo 保存 external reference 本身，不复制大 payload；rollback 可据此恢复旧聚簇记录引用。 */
    @Test void updateRoundTripsExternalLobReferenceInOldImage() {
        TableSchema schema = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.longBlob(true), 1)), true);
        IndexKeyDef keyDef = new IndexKeyDef(9L, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
        LobReference reference = new LobReference(SpaceId.of(7), PageNo.of(64), 80_000, 5,
                SegmentId.of(4), 2, 0x1234_5678L);
        ColumnValue.ExternalValue external = new ColumnValue.ExternalValue(
                TypeId.LONGBLOB, reference, new byte[]{1, 2, 3});
        UndoRecord record = updateRec(List.of(new ColumnValue.IntValue(1)),
                List.of(new ColumnValue.IntValue(1), external), RollPointer.NULL);
        UndoRecordCodec codec = new UndoRecordCodec(registry);

        UndoRecord decoded = codec.decode(codec.encode(record, keyDef, schema), 0, keyDef, schema);
        assertEquals(external, decoded.oldColumnValues().get(1));
    }

    @Test void decodeRejectsUpdateTruncated() {
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(updateRec(
                List.of(new ColumnValue.IntValue(1), new ColumnValue.StringValue("k")),
                List.of(new ColumnValue.IntValue(1), new ColumnValue.StringValue("old")),
                RollPointer.NULL), twoColKey(), twoColSchema());
        byte[] cut = Arrays.copyOf(buf, buf.length - 1);
        assertThrows(UndoLogFormatException.class, () -> codec.decode(cut, 0, twoColKey(), twoColSchema()));
    }

    @Test void decodeRejectsRowColCountSchemaMismatch() {
        // 2 列 schema 编码（rowColCount=2），用 3 列 schema 解码：key 列(0,1)仍可解析，但 row 阶段 2≠3 必抛。
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(updateRec(
                List.of(new ColumnValue.IntValue(1), new ColumnValue.StringValue("k")),
                List.of(new ColumnValue.IntValue(1), new ColumnValue.StringValue("old")),
                RollPointer.NULL), twoColKey(), twoColSchema());
        TableSchema threeCol = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(64, true), 1),
                new ColumnDef(new ColumnId(2), "extra", ColumnType.intType(false, true), 2)), true);
        assertThrows(UndoLogFormatException.class, () -> codec.decode(buf, 0, twoColKey(), threeCol));
    }

    @Test void insertGoldenBytesStable() {
        // 固定 INSERT_ROW 编码（NULL key 列，完全确定，不依赖列 codec）证明 undo framing 字节不漂移。
        TableSchema oneCol = new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0)), true);
        IndexKeyDef oneColKey = new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
        UndoRecord r = UndoRecord.insert(UndoNo.of(5), TransactionId.of(0x1122334455L), 7L, 9L,
                List.of(ColumnValue.NullValue.INSTANCE), RollPointer.NULL);
        byte[] buf = new UndoRecordCodec(registry).encode(r, oneColKey, oneCol);
        byte[] expected = {
                0x01,                                              // type=INSERT_ROW
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,    // undoNo=5
                0x00, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,    // txn=0x1122334455
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,    // tableId=7
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,    // indexId=9
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,          // prevRollPointer=NULL (7B)
                0x01,                                              // keyColCount=1
                0x01                                               // col0 nullFlag=1 (NULL)
        };
        assertArrayEquals(expected, buf, "INSERT undo framing must stay byte-stable");
    }
}
