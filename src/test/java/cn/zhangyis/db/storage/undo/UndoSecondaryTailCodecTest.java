package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
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
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 secondary undo tail 的向后兼容磁盘协议。旧 record 以 EOF 表示空 tail；新 tail 使用独立 magic/version/count，
 * INSERT 同时携带 LOB ownership 时固定按 LOB tail -> secondary tail 排列。
 */
class UndoSecondaryTailCodecTest {

    /** 测试 codec；使用生产 registry 验证 LOB ownership 与 secondary tail 能在同一 record 中协作。 */
    private final UndoRecordCodec codec = new UndoRecordCodec(new TypeCodecRegistry());

    /** 三种 undo action 都必须往返，并保持 identity 固定前缀可在不知道 tail 内容时读取。 */
    @Test
    void roundTripsAllSecondaryMutationActions() {
        UndoRecord insert = UndoRecord.insert(UndoNo.of(1), TransactionId.of(21), 7, 9, keyValues(), List.of(),
                List.of(SecondaryUndoMutation.insertEntry(11), SecondaryUndoMutation.insertEntry(12)),
                RollPointer.NULL);
        UndoRecord update = UndoRecord.update(UndoNo.of(2), TransactionId.of(21), 7, 9, keyValues(), rowValues(),
                oldHidden(), List.of(
                        SecondaryUndoMutation.changeKey(11, SecondaryEntryBeforeState.ABSENT),
                        SecondaryUndoMutation.changeKey(12, SecondaryEntryBeforeState.DELETE_MARKED)),
                RollPointer.NULL);
        UndoRecord delete = UndoRecord.deleteMark(UndoNo.of(3), TransactionId.of(21), 7, 9, keyValues(), rowValues(),
                oldHidden(), List.of(SecondaryUndoMutation.deleteMarkEntry(11)), RollPointer.NULL);

        for (UndoRecord expected : List.of(insert, update, delete)) {
            byte[] encoded = codec.encode(expected, clusteredKey(), schema());
            assertEquals(expected, codec.decode(encoded, 0, clusteredKey(), schema()));
            assertEquals(expected.type(), codec.peekIdentity(encoded, 0).type());
        }
    }

    /** INSERT 同时存在 LOB ownership 与 secondary mutation 时，decoder 必须按固定双尾顺序完整恢复两个列表。 */
    @Test
    void roundTripsLobThenSecondaryDualTail() {
        InsertedLobOwnership ownership = new InsertedLobOwnership(1,
                new ColumnValue.ExternalValue(TypeId.TEXT,
                        new LobReference(SpaceId.of(7), PageNo.of(80), 4096, 1,
                                SegmentId.of(4), 2, 0x1234_5678L),
                        "prefix".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        UndoRecord expected = UndoRecord.insert(UndoNo.of(1), TransactionId.of(21), 7, 9, keyValues(),
                List.of(ownership), List.of(SecondaryUndoMutation.insertEntry(11)), RollPointer.NULL);

        UndoRecord decoded = codec.decode(codec.encode(expected, clusteredKey(), schema()),
                0, clusteredKey(), schema());

        assertEquals(expected.insertedLobs(), decoded.insertedLobs());
        assertEquals(expected.secondaryMutations(), decoded.secondaryMutations());
    }

    /** 旧 EOF record 解码为空列表，且新增可选 tail 不能改变没有 mutation 的既有编码字节。 */
    @Test
    void legacyEndOfRecordDecodesAsEmptySecondaryTail() {
        UndoRecord legacy = UndoRecord.insert(UndoNo.of(1), TransactionId.of(21), 7, 9,
                keyValues(), RollPointer.NULL);
        byte[] before = codec.encode(legacy, clusteredKey(), schema());
        UndoRecord explicitEmpty = UndoRecord.insert(UndoNo.of(1), TransactionId.of(21), 7, 9,
                keyValues(), List.of(), List.of(), RollPointer.NULL);

        assertEquals(legacy, codec.decode(before, 0, clusteredKey(), schema()));
        assertEquals(List.of(), codec.decode(before, 0, clusteredKey(), schema()).secondaryMutations());
        assertEquals(Arrays.toString(before),
                Arrays.toString(codec.encode(explicitEmpty, clusteredKey(), schema())));
    }

    /** 未知 magic/version、重复 index、非法 action/state、截断和尾随垃圾必须按物理损坏 fail-closed。 */
    @Test
    void rejectsCorruptSecondaryTail() {
        UndoRecord record = UndoRecord.insert(UndoNo.of(1), TransactionId.of(21), 7, 9, keyValues(), List.of(),
                List.of(SecondaryUndoMutation.insertEntry(11)), RollPointer.NULL);
        byte[] legacy = codec.encode(UndoRecord.insert(UndoNo.of(1), TransactionId.of(21), 7, 9,
                keyValues(), RollPointer.NULL), clusteredKey(), schema());
        byte[] encoded = codec.encode(record, clusteredKey(), schema());
        int tail = legacy.length;

        byte[] badMagic = encoded.clone();
        badMagic[tail] ^= 0x01;
        byte[] badVersion = encoded.clone();
        badVersion[tail + 2] = 99;
        byte[] badAction = encoded.clone();
        badAction[tail + 13] = 99;
        byte[] badState = encoded.clone();
        badState[tail + 14] = 99;
        byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);

        for (byte[] corrupt : List.of(badMagic, badVersion, badAction, badState, truncated, trailing)) {
            assertThrows(UndoLogFormatException.class,
                    () -> codec.decode(corrupt, 0, clusteredKey(), schema()));
        }
    }

    /** 重复或倒序 index id 会改变跨树 inverse 顺序，decoder 必须在恢复执行前拒绝。 */
    @Test
    void rejectsDuplicateOrOutOfOrderIndexIdsOnDisk() {
        UndoRecord record = UndoRecord.insert(UndoNo.of(1), TransactionId.of(21), 7, 9, keyValues(), List.of(),
                List.of(SecondaryUndoMutation.insertEntry(11), SecondaryUndoMutation.insertEntry(12)),
                RollPointer.NULL);
        byte[] legacy = codec.encode(UndoRecord.insert(UndoNo.of(1), TransactionId.of(21), 7, 9,
                keyValues(), RollPointer.NULL), clusteredKey(), schema());
        byte[] encoded = codec.encode(record, clusteredKey(), schema());
        int tail = legacy.length;
        int secondIndexId = tail + 5 + 10;
        Arrays.fill(encoded, secondIndexId, secondIndexId + 8, (byte) 0);
        encoded[secondIndexId + 7] = 11;

        assertThrows(UndoLogFormatException.class,
                () -> codec.decode(encoded, 0, clusteredKey(), schema()));
    }

    /** 测试表 schema；TEXT 只用于 INSERT LOB ownership，secondary tail 本身不依赖字段类型。 */
    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "description", ColumnType.text(true), 1)), true);
    }

    /** undo 固定 identity 指向聚簇索引，因此 keyDef 只包含主键列。 */
    private static IndexKeyDef clusteredKey() {
        return new IndexKeyDef(9, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    /** 聚簇主键值。 */
    private static List<ColumnValue> keyValues() {
        return List.of(new ColumnValue.IntValue(7));
    }

    /** UPDATE/DELETE 全量旧 image；TEXT 使用 NULL 避免把 ownership 与 old-version reference 混为一谈。 */
    private static List<ColumnValue> rowValues() {
        return List.of(new ColumnValue.IntValue(7), ColumnValue.NullValue.INSTANCE);
    }

    /** 旧版本链隐藏列。 */
    private static cn.zhangyis.db.storage.record.format.HiddenColumns oldHidden() {
        return new cn.zhangyis.db.storage.record.format.HiddenColumns(
                TransactionId.of(8), new RollPointer(false, PageNo.of(64), 2));
    }
}
