package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import org.junit.jupiter.api.Test;

import java.util.List;

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

    @Test
    void insertFactoryHasNoOldImage() {
        UndoRecord rec = UndoRecord.insert(UndoNo.of(1), TXN, 1L, 9L, KEY, RollPointer.NULL);
        assertEquals(UndoRecordType.INSERT_ROW, rec.type());
        assertEquals(KEY, rec.clusterKey());
        assertNull(rec.oldColumnValues(), "INSERT undo carries no old row image");
        assertNull(rec.oldHiddenColumns(), "INSERT undo carries no old hidden columns");
    }

    @Test
    void updateFactoryCarriesFullOldImage() {
        UndoRecord rec = UndoRecord.update(UndoNo.of(2), TXN, 1L, 9L, KEY, OLD_ROW, OLD_HIDDEN, PREV);
        assertEquals(UndoRecordType.UPDATE_ROW, rec.type());
        assertEquals(KEY, rec.clusterKey());
        assertEquals(OLD_ROW, rec.oldColumnValues());
        assertEquals(OLD_HIDDEN, rec.oldHiddenColumns());
        assertEquals(PREV, rec.prevRollPointer());
    }

    @Test
    void insertWithOldImageRejected() {
        // 直接走 canonical ctor 传 old image 给 INSERT_ROW → 拒绝
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.INSERT_ROW, UndoNo.of(1), TXN, 1L, 9L, KEY, OLD_ROW, OLD_HIDDEN, RollPointer.NULL));
    }

    @Test
    void updateWithoutOldImageRejected() {
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.UPDATE_ROW, UndoNo.of(2), TXN, 1L, 9L, KEY, null, null, PREV));
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.UPDATE_ROW, UndoNo.of(2), TXN, 1L, 9L, KEY, OLD_ROW, null, PREV));
    }

    @Test
    void deleteMarkFactoryCarriesFullOldImage() {
        // T1.3f：DELETE_MARK 放开，复用 UPDATE 旧 image 结构（删除前存活版本）
        UndoRecord rec = UndoRecord.deleteMark(UndoNo.of(3), TXN, 1L, 9L, KEY, OLD_ROW, OLD_HIDDEN, PREV);
        assertEquals(UndoRecordType.DELETE_MARK, rec.type());
        assertEquals(OLD_ROW, rec.oldColumnValues());
        assertEquals(OLD_HIDDEN, rec.oldHiddenColumns());
        assertEquals(PREV, rec.prevRollPointer());
    }

    @Test
    void deleteMarkWithoutOldImageRejected() {
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.DELETE_MARK, UndoNo.of(1), TXN, 1L, 9L, KEY, null, null, PREV));
    }

    @Test
    void undoNoNoneAndEmptyKeyRejected() {
        assertThrows(DatabaseValidationException.class,
                () -> UndoRecord.insert(UndoNo.NONE, TXN, 1L, 9L, KEY, RollPointer.NULL));
        assertThrows(DatabaseValidationException.class,
                () -> UndoRecord.insert(UndoNo.of(1), TXN, 1L, 9L, List.of(), RollPointer.NULL));
    }
}
