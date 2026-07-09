package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.3c UndoContext 单元测试。UndoContext 是挂 {@link Transaction} 的事务运行时 undo 子状态
 * （设计 §5.3），由 {@code UndoLogManager.ensureUndoContext} 在首写时惰性创建。本测试覆盖字段往返与
 * 「刚建段、尚未 append」的惰性初值：{@code lastRollPointer=NULL}、{@code lastUndoNo=NONE}、
 * {@code insertUndoFirstPageId} 为刚分配的 insert undo segment 首页。
 *
 * <p>1.4 起补充保存点边界语义：partial rollback 可退回逻辑链头，但 append 高水位不能倒回。
 */
class UndoContextTest {

    private static final RollbackSegmentId RSEG = RollbackSegmentId.of(0);
    private static final UndoSlotId SLOT = UndoSlotId.of(3);
    private static final PageId FIRST_PAGE = PageId.of(SpaceId.of(77), PageNo.of(65));

    @Test
    void freshContextHasLazyInitialValues() {
        UndoContext ctx = new UndoContext(RSEG, SLOT, FIRST_PAGE);

        assertEquals(RSEG, ctx.rollbackSegmentId());
        assertEquals(SLOT, ctx.slotId());
        assertEquals(FIRST_PAGE, ctx.undoFirstPageId());
        assertTrue(ctx.lastUndoNo().isNone(), "fresh context lastUndoNo must be NONE before any append");
        assertTrue(ctx.lastRollPointer().isNull(),
                "fresh context lastRollPointer must be NULL before any append");
    }

    @Test
    void setLastUndoNoUpdatesField() {
        UndoContext ctx = new UndoContext(RSEG, SLOT, FIRST_PAGE);

        ctx.setLastUndoNo(UndoNo.of(2));

        assertEquals(UndoNo.of(2), ctx.lastUndoNo());
        // rollbackSegmentId/slotId/insertUndoFirstPageId 不随 append 改变
        assertEquals(RSEG, ctx.rollbackSegmentId());
        assertEquals(SLOT, ctx.slotId());
        assertEquals(FIRST_PAGE, ctx.undoFirstPageId());
    }

    @Test
    void setLastRollPointerUpdatesField() {
        UndoContext ctx = new UndoContext(RSEG, SLOT, FIRST_PAGE);

        RollPointer rp = new RollPointer(true, PageNo.of(65), 97);
        ctx.setLastRollPointer(rp);

        assertEquals(rp, ctx.lastRollPointer());
        assertNotNull(ctx.lastRollPointer());
    }

    @Test
    void savepointRestoreMovesLogicalBoundaryWithoutReusingAppendUndoNo() {
        Transaction txn = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext ctx = new UndoContext(RSEG, SLOT, FIRST_PAGE);
        RollPointer first = new RollPointer(true, PageNo.of(65), 97);
        RollPointer second = new RollPointer(true, PageNo.of(65), 128);
        ctx.setLastUndoNo(UndoNo.of(1));
        ctx.setLastRollPointer(first);

        TransactionSavepoint savepoint = ctx.createSavepoint(txn);
        ctx.setLastUndoNo(UndoNo.of(2));
        ctx.setLastRollPointer(second);
        ctx.completeRollbackToSavepoint(savepoint);

        assertEquals(UndoNo.of(2), ctx.lastUndoNo(),
                "partial rollback must not rewind append high-water or the next append would reuse undoNo");
        assertEquals(UndoNo.of(1), ctx.logicalLastUndoNo(),
                "logical chain head returns to the savepoint boundary");
        assertEquals(first, ctx.lastRollPointer(), "rollback chain head returns to the savepoint roll pointer");
        assertEquals(1, ctx.savepointCount(), "target savepoint remains valid for repeated rollback-to");
    }

    @Test
    void createSavepointRejectsTransactionBoundToAnotherUndoContext() {
        Transaction txn = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext owned = new UndoContext(RSEG, SLOT, FIRST_PAGE);
        UndoContext other = new UndoContext(RSEG, UndoSlotId.of(4), PageId.of(SpaceId.of(77), PageNo.of(66)));
        txn.setUndoContext(owned);

        assertThrows(DatabaseValidationException.class, () -> other.createSavepoint(txn));
    }

    @Test
    void setLastRollPointerRejectsNullJavaRef() {
        UndoContext ctx = new UndoContext(RSEG, SLOT, FIRST_PAGE);
        // RollPointer.NULL 是合法值（表「无前驱」），但 Java null 引用必须拒绝，避免隐藏 NPE
        assertThrows(DatabaseValidationException.class, () -> ctx.setLastRollPointer(null));
    }

    @Test
    void constructorRejectsNullFields() {
        assertThrows(DatabaseValidationException.class,
                () -> new UndoContext(null, SLOT, FIRST_PAGE));
        assertThrows(DatabaseValidationException.class,
                () -> new UndoContext(RSEG, null, FIRST_PAGE));
        assertThrows(DatabaseValidationException.class,
                () -> new UndoContext(RSEG, SLOT, null));
    }

    @Test
    void hasUpdateUndoDefaultsFalseAndMarksTrue() {
        UndoContext ctx = new UndoContext(RSEG, SLOT, FIRST_PAGE);
        assertFalse(ctx.hasUpdateUndo(), "fresh context has no update undo");
        ctx.markHasUpdateUndo();
        assertTrue(ctx.hasUpdateUndo(), "markHasUpdateUndo sets the flag (commit must keep slot)");
    }

    @Test
    void setLastUndoNoRejectsNull() {
        UndoContext ctx = new UndoContext(RSEG, SLOT, FIRST_PAGE);
        assertThrows(DatabaseValidationException.class, () -> ctx.setLastUndoNo(null));
    }

    @Test
    void rollbackSegmentIdAndUndoSlotIdValidateNonNegative() {
        assertThrows(DatabaseValidationException.class, () -> RollbackSegmentId.of(-1));
        assertThrows(DatabaseValidationException.class, () -> UndoSlotId.of(-1));
        // 合法值往返（record 按组件值相等，不用 assertSame）
        assertEquals(RollbackSegmentId.of(0), RollbackSegmentId.of(0));
        assertEquals(7, UndoSlotId.of(7).value());
        assertEquals(2, RollbackSegmentId.of(2).value());
    }
}
