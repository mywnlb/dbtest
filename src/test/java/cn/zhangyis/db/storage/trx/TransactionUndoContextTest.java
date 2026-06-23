package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoSlotId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * T1.3c Transaction 与 UndoContext 的绑定。事务首写前 {@code undoContext()} 为 null（惰性，未建 undo segment）；
 * {@code setUndoContext} 由 {@code UndoLogManager.ensureUndoContext} 在首写时调用一次，null 引用必须拒绝。
 */
class TransactionUndoContextTest {

    @Test
    void freshTransactionHasNoUndoContext() {
        Transaction t = new Transaction(TransactionOptions.defaults(), 1L);
        assertNull(t.undoContext(), "undo context is lazy: null before first write");
    }

    @Test
    void setUndoContextBindsContext() {
        Transaction t = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext ctx = new UndoContext(RollbackSegmentId.of(0), UndoSlotId.of(1),
                PageId.of(SpaceId.of(77), PageNo.of(65)));

        t.setUndoContext(ctx);

        assertEquals(ctx, t.undoContext());
    }

    @Test
    void setUndoContextRejectsNull() {
        Transaction t = new Transaction(TransactionOptions.defaults(), 1L);
        assertThrows(DatabaseValidationException.class, () -> t.setUndoContext(null));
    }

    @Test
    void setUndoContextAllowsOverwriteByManager() {
        // UndoLogManager 控制单次调用；mutator 本身不强制单次，避免把生命周期约束塞进 setter
        Transaction t = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext first = new UndoContext(RollbackSegmentId.of(0), UndoSlotId.of(0),
                PageId.of(SpaceId.of(77), PageNo.of(65)));
        UndoContext second = new UndoContext(RollbackSegmentId.of(0), UndoSlotId.of(1),
                PageId.of(SpaceId.of(77), PageNo.of(66)));

        t.setUndoContext(first);
        assertDoesNotThrow(() -> t.setUndoContext(second));
        assertEquals(second, t.undoContext());
    }
}
