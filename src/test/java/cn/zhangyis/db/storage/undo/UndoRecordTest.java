package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.schema.TypeId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * T1.3e UndoRecord 类型判别命令对象单元测试。INSERT_ROW 不带旧 image；UPDATE_ROW 带全量旧列值 + 旧隐藏列。
 * 静态工厂固定 type，compact ctor 按 type 校验 old* 字段可空性，并仍拒绝 DELETE_MARK（→ T1.3f）。
 */
class UndoRecordTest {

    private static final TransactionId TXN = TransactionId.of(7);
    private static final List<ColumnValue> KEY = List.of(new ColumnValue.IntValue(100));
    private static final List<ColumnValue> OLD_ROW =
            List.of(new ColumnValue.IntValue(100), new ColumnValue.StringValue("old"));
    private static final HiddenColumns OLD_HIDDEN =
            new HiddenColumns(TransactionId.of(3), new RollPointer(false, PageNo.of(66), 12));
    private static final RollPointer PREV = new RollPointer(false, PageNo.of(66), 40);

    /**
     * 验证 {@code insertFactoryHasNoOldImage} 对应的Undo 日志行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void insertFactoryHasNoOldImage() {
        UndoRecord rec = UndoRecord.insert(UndoNo.of(1), TXN, 1L, 9L, KEY, RollPointer.NULL);
        assertEquals(UndoRecordType.INSERT_ROW, rec.type());
        assertEquals(KEY, rec.clusterKey());
        assertNull(rec.oldColumnValues(), "INSERT undo carries no old row image");
        assertNull(rec.oldHiddenColumns(), "INSERT undo carries no old hidden columns");
    }

    /**
     * 验证 {@code updateFactoryCarriesFullOldImage} 对应的Undo 日志行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void updateFactoryCarriesFullOldImage() {
        UndoRecord rec = UndoRecord.update(UndoNo.of(2), TXN, 1L, 9L, KEY, OLD_ROW, OLD_HIDDEN, PREV);
        assertEquals(UndoRecordType.UPDATE_ROW, rec.type());
        assertEquals(KEY, rec.clusterKey());
        assertEquals(OLD_ROW, rec.oldColumnValues());
        assertEquals(OLD_HIDDEN, rec.oldHiddenColumns());
        assertEquals(PREV, rec.prevRollPointer());
    }

    /**
     * 验证 {@code insertWithOldImageRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void insertWithOldImageRejected() {
        // 直接走 canonical ctor 传 old image 给 INSERT_ROW → 拒绝
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.INSERT_ROW, UndoNo.of(1), TXN, 1L, 9L, KEY, OLD_ROW, OLD_HIDDEN,
                List.of(), RollPointer.NULL));
    }

    /**
     * 验证 {@code updateWithoutOldImageRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void updateWithoutOldImageRejected() {
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.UPDATE_ROW, UndoNo.of(2), TXN, 1L, 9L, KEY, null, null, List.of(), PREV));
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.UPDATE_ROW, UndoNo.of(2), TXN, 1L, 9L, KEY, OLD_ROW, null, List.of(), PREV));
    }

    /**
     * 验证 {@code deleteMarkFactoryCarriesFullOldImage} 对应的Undo 日志行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void deleteMarkFactoryCarriesFullOldImage() {
        // T1.3f：DELETE_MARK 放开，复用 UPDATE 旧 image 结构（删除前存活版本）
        UndoRecord rec = UndoRecord.deleteMark(UndoNo.of(3), TXN, 1L, 9L, KEY, OLD_ROW, OLD_HIDDEN, PREV);
        assertEquals(UndoRecordType.DELETE_MARK, rec.type());
        assertEquals(OLD_ROW, rec.oldColumnValues());
        assertEquals(OLD_HIDDEN, rec.oldHiddenColumns());
        assertEquals(PREV, rec.prevRollPointer());
    }

    /**
     * 验证 {@code deleteMarkWithoutOldImageRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void deleteMarkWithoutOldImageRejected() {
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.DELETE_MARK, UndoNo.of(1), TXN, 1L, 9L, KEY, null, null, List.of(), PREV));
    }

    /**
     * 验证 {@code undoNoNoneAndEmptyKeyRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void undoNoNoneAndEmptyKeyRejected() {
        assertThrows(DatabaseValidationException.class,
                () -> UndoRecord.insert(UndoNo.NONE, TXN, 1L, 9L, KEY, RollPointer.NULL));
        assertThrows(DatabaseValidationException.class,
                () -> UndoRecord.insert(UndoNo.of(1), TXN, 1L, 9L, List.of(), RollPointer.NULL));
    }

    /** INSERT ownership 以严格递增 column ordinal 冻结，调用方后续修改集合不能改变 undo 语义。 */
    @Test
    void insertOwnsImmutableOrderedExternalLobReferences() {
        List<InsertedLobOwnership> mutable = new ArrayList<>();
        mutable.add(ownership(1, TypeId.TEXT, 64));
        mutable.add(ownership(3, TypeId.LONGBLOB, 96));

        UndoRecord record = UndoRecord.insert(UndoNo.of(1), TXN, 1L, 9L, KEY, mutable, RollPointer.NULL);
        mutable.clear();

        assertEquals(2, record.insertedLobs().size());
        assertThrows(UnsupportedOperationException.class, () -> record.insertedLobs().clear());
        assertThrows(DatabaseValidationException.class,
                () -> new InsertedLobOwnership(-1, external(TypeId.TEXT, 64)));
        assertThrows(DatabaseValidationException.class,
                () -> new InsertedLobOwnership(1, null));
    }

    /** 重复/逆序 ordinal 会让 rollback ownership 含义不确定，非 INSERT 类型则根本不得携带该列表。 */
    @Test
    void rejectsDuplicateOutOfOrderOrNonInsertLobOwnership() {
        assertThrows(DatabaseValidationException.class, () -> UndoRecord.insert(UndoNo.of(1), TXN,
                1L, 9L, KEY, List.of(ownership(2, TypeId.TEXT, 64), ownership(2, TypeId.TEXT, 65)),
                RollPointer.NULL));
        assertThrows(DatabaseValidationException.class, () -> UndoRecord.insert(UndoNo.of(1), TXN,
                1L, 9L, KEY, List.of(ownership(3, TypeId.TEXT, 64), ownership(1, TypeId.TEXT, 65)),
                RollPointer.NULL));
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(UndoRecordType.UPDATE_ROW,
                UndoNo.of(2), TXN, 1L, 9L, KEY, OLD_ROW, OLD_HIDDEN,
                List.of(ownership(1, TypeId.TEXT, 64)), PREV));
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(UndoRecordType.DELETE_MARK,
                UndoNo.of(2), TXN, 1L, 9L, KEY, OLD_ROW, OLD_HIDDEN,
                List.of(ownership(1, TypeId.TEXT, 64)), PREV));
    }

    /** UPDATE ownership 同时表达“回滚释放新链”和“提交后 purge 释放旧链”，两者不能再由 old image 猜测。 */
    @Test
    void updateCarriesOrderedLobVersionOwnership() {
        ColumnValue.ExternalValue oldExternal = external(TypeId.TEXT, 64);
        ColumnValue.ExternalValue newExternal = external(TypeId.TEXT, 96);
        List<ColumnValue> oldRow = List.of(new ColumnValue.IntValue(100), oldExternal);
        LobVersionOwnership ownership = new LobVersionOwnership(1, true, Optional.of(newExternal));

        UndoRecord record = UndoRecord.update(UndoNo.of(2), TXN, 1L, 9L, KEY, oldRow, OLD_HIDDEN,
                List.of(ownership), List.of(), PREV);

        assertEquals(List.of(ownership), record.lobVersionOwnerships());
        assertThrows(UnsupportedOperationException.class, () -> record.lobVersionOwnerships().clear());
    }

    /** INSERT 不能携带 version ownership；DELETE 只能声明 purge 旧链，不能产生 rollback-new owner。 */
    @Test
    void rejectsInvalidLobVersionOwnershipByUndoType() {
        ColumnValue.ExternalValue external = external(TypeId.TEXT, 64);
        LobVersionOwnership rollbackAndPurge = new LobVersionOwnership(1, true, Optional.of(external));
        LobVersionOwnership purgeOnly = new LobVersionOwnership(1, true, Optional.empty());

        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.INSERT_ROW, UndoNo.of(1), TXN, 1L, 9L, KEY, null, null,
                List.of(), List.of(rollbackAndPurge), List.of(), RollPointer.NULL));
        assertThrows(DatabaseValidationException.class, () -> UndoRecord.deleteMark(
                UndoNo.of(3), TXN, 1L, 9L, KEY,
                List.of(new ColumnValue.IntValue(100), external), OLD_HIDDEN,
                List.of(rollbackAndPurge), List.of(), PREV));

        UndoRecord delete = UndoRecord.deleteMark(UndoNo.of(3), TXN, 1L, 9L, KEY,
                List.of(new ColumnValue.IntValue(100), external), OLD_HIDDEN,
                List.of(purgeOnly), List.of(), PREV);
        assertEquals(List.of(purgeOnly), delete.lobVersionOwnerships());
    }

    private static InsertedLobOwnership ownership(int ordinal, TypeId typeId, long pageNo) {
        return new InsertedLobOwnership(ordinal, external(typeId, pageNo));
    }

    private static ColumnValue.ExternalValue external(TypeId typeId, long pageNo) {
        return new ColumnValue.ExternalValue(typeId,
                new LobReference(SpaceId.of(8), PageNo.of(pageNo), 4_096, 1,
                        SegmentId.of(12), 3, 0x1234_5678L), new byte[]{1, 2, 3});
    }
}
