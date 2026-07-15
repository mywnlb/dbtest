package cn.zhangyis.db.storage.trx;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 1.6：事务必须分别持有 INSERT/UPDATE undo 绑定，并只共享全局 undoNo 高水位。 */
class UndoContextV2Test {

    @Test
    void keepsIndependentHeadsWhileGlobalUndoNumberAdvances() {
        UndoContext context = new UndoContext(RollbackSegmentId.of(0));
        UndoLogBinding insert = new UndoLogBinding(UndoLogKind.INSERT, UndoSlotId.of(0),
                PageId.of(SpaceId.of(77), PageNo.of(64)), UndoLogicalHead.EMPTY);
        UndoLogBinding update = new UndoLogBinding(UndoLogKind.UPDATE, UndoSlotId.of(1),
                PageId.of(SpaceId.of(77), PageNo.of(65)), UndoLogicalHead.EMPTY);
        context.attach(insert);
        context.attach(update);

        RollPointer insertPointer = new RollPointer(true, PageNo.of(64), 120);
        RollPointer updatePointer = new RollPointer(false, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), insertPointer);
        context.publishAppend(UndoLogKind.UPDATE, UndoNo.of(2), updatePointer);

        assertEquals(UndoNo.of(2), context.lastUndoNo());
        assertEquals(new UndoLogicalHead(UndoNo.of(1), insertPointer), context.head(UndoLogKind.INSERT));
        assertEquals(new UndoLogicalHead(UndoNo.of(2), updatePointer), context.head(UndoLogKind.UPDATE));
        assertTrue(context.hasBinding(UndoLogKind.INSERT));
        assertTrue(context.hasBinding(UndoLogKind.UPDATE));
        assertFalse(context.hasBinding(UndoLogKind.TEMPORARY));
    }
}
