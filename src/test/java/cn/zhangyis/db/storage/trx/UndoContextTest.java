package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 1.6 双 undo log 事务上下文单元测试。 */
class UndoContextTest {
    private static final RollbackSegmentId RSEG = RollbackSegmentId.of(0);
    private static final UndoSlotId SLOT = UndoSlotId.of(3);
    private static final PageId FIRST = PageId.of(SpaceId.of(77), PageNo.of(65));

    @Test void freshContextHasNoBindingsAndNoneHighWater() {
        UndoContext context = new UndoContext(RSEG);
        assertTrue(context.bindings().isEmpty());
        assertEquals(UndoNo.NONE, context.lastUndoNo());
    }

    @Test void attachInsertBindingKeepsIdentity() {
        UndoContext context = freshInsert();
        assertEquals(SLOT, context.binding(UndoLogKind.INSERT).slotId());
        assertEquals(FIRST, context.binding(UndoLogKind.INSERT).firstPageId());
    }

    @Test void duplicateKindIsRejected() {
        UndoContext context = freshInsert();
        assertThrows(DatabaseValidationException.class, () -> context.attach(new UndoLogBinding(
                UndoLogKind.INSERT, UndoSlotId.of(4), PageId.of(SpaceId.of(77), PageNo.of(66)),
                UndoLogicalHead.EMPTY)));
    }

    @Test void globalHighWaterAndLocalHeadAdvanceTogetherAfterAppend() {
        UndoContext context = freshInsert();
        RollPointer pointer = new RollPointer(true, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), pointer);
        assertEquals(UndoNo.of(1), context.lastUndoNo());
        assertEquals(new UndoLogicalHead(UndoNo.of(1), pointer), context.head(UndoLogKind.INSERT));
    }

    @Test void appendMustAdvanceGlobalUndoNumber() {
        UndoContext context = freshInsert();
        RollPointer pointer = new RollPointer(true, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), pointer);
        assertThrows(DatabaseValidationException.class,
                () -> context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), pointer));
    }

    @Test void savepointCapturesBothHeads() {
        Transaction txn = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext context = freshInsert();
        txn.setUndoContext(context);
        RollPointer pointer = new RollPointer(true, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), pointer);
        TransactionSavepoint savepoint = context.createSavepoint(txn);
        assertEquals(new UndoLogicalHead(UndoNo.of(1), pointer), savepoint.insertHead());
        assertEquals(UndoLogicalHead.EMPTY, savepoint.updateHead());
    }

    @Test void rollbackToSavepointDoesNotRewindGlobalHighWater() {
        Transaction txn = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext context = freshInsert();
        txn.setUndoContext(context);
        RollPointer first = new RollPointer(true, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), first);
        TransactionSavepoint savepoint = context.createSavepoint(txn);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(2),
                new RollPointer(true, PageNo.of(65), 150));
        context.completeRollbackToSavepoint(savepoint);
        assertEquals(UndoNo.of(2), context.lastUndoNo());
        assertEquals(new UndoLogicalHead(UndoNo.of(1), first), context.head(UndoLogKind.INSERT));
    }

    @Test void emptyBoundaryClearsEveryExistingLocalHead() {
        UndoContext context = freshInsert();
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1),
                new RollPointer(true, PageNo.of(65), 120));
        context.completeRollbackToEmptyBoundary();
        assertEquals(UndoLogicalHead.EMPTY, context.head(UndoLogKind.INSERT));
        assertEquals(UndoNo.of(1), context.lastUndoNo());
    }

    @Test void releaseSavepointRemovesNestedRange() {
        Transaction txn = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext context = freshInsert();
        txn.setUndoContext(context);
        TransactionSavepoint first = context.createSavepoint(txn);
        context.createSavepoint(txn);
        context.releaseSavepoint(first);
        assertEquals(0, context.savepointCount());
    }

    @Test void temporaryBindingIsRejected() {
        assertThrows(DatabaseValidationException.class, () -> new UndoLogBinding(
                UndoLogKind.TEMPORARY, SLOT, FIRST, UndoLogicalHead.EMPTY));
    }

    @Test void nonEmptyBindingHeadMustMatchPersistentLogKind() {
        assertThrows(DatabaseValidationException.class, () -> new UndoLogBinding(
                UndoLogKind.INSERT, SLOT, FIRST,
                new UndoLogicalHead(UndoNo.of(1), new RollPointer(false, PageNo.of(65), 120))));
        assertThrows(DatabaseValidationException.class, () -> new UndoLogBinding(
                UndoLogKind.UPDATE, SLOT, FIRST,
                new UndoLogicalHead(UndoNo.of(1), new RollPointer(true, PageNo.of(65), 120))));
    }

    @Test void nullRollbackSegmentIsRejected() {
        assertThrows(DatabaseValidationException.class, () -> new UndoContext(null));
        assertFalse(RSEG.value() < 0);
    }

    private static UndoContext freshInsert() {
        UndoContext context = new UndoContext(RSEG);
        context.attach(new UndoLogBinding(UndoLogKind.INSERT, SLOT, FIRST, UndoLogicalHead.EMPTY));
        return context;
    }
}
