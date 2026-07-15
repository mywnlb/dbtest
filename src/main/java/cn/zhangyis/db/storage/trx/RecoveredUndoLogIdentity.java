package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;

/** 恢复期同一 creator 下单条 ACTIVE undo log 的无 latch identity。 */
public record RecoveredUndoLogIdentity(UndoLogKind kind, UndoSlotId slotId, PageId firstPageId) {
    public RecoveredUndoLogIdentity {
        if (kind == null || kind == UndoLogKind.TEMPORARY || slotId == null || firstPageId == null) {
            throw new DatabaseValidationException("invalid recovered undo log identity");
        }
    }
}
