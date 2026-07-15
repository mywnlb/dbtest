package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;

/** 测试夹具对双 log context 的显式选择工具，不向生产 UndoContext 重新引入单 log 兼容 API。 */
public final class UndoTestContexts {

    private UndoTestContexts() {
    }

    public static UndoLogBinding newestBinding(UndoContext context) {
        if (context == null || context.bindings().isEmpty()) {
            throw new DatabaseValidationException("test undo context has no binding");
        }
        UndoLogBinding newest = null;
        for (UndoLogBinding binding : context.bindings()) {
            if (newest == null || binding.logicalHead().undoNo().value()
                    > newest.logicalHead().undoNo().value()) {
                newest = binding;
            }
        }
        return newest;
    }

    public static PageId firstPage(UndoContext context) { return newestBinding(context).firstPageId(); }
    public static UndoSlotId slot(UndoContext context) { return newestBinding(context).slotId(); }
    public static UndoLogicalHead head(UndoContext context) { return newestBinding(context).logicalHead(); }
    public static UndoNo logicalUndoNo(UndoContext context) { return head(context).undoNo(); }
    public static RollPointer rollPointer(UndoContext context) { return head(context).rollPointer(); }

    public static UndoContext restored(RollbackSegmentId rseg, UndoLogKind kind, UndoSlotId slot,
                                       PageId firstPage, UndoNo globalHighWater, UndoLogicalHead head) {
        UndoContext context = new UndoContext(rseg);
        context.restoreBinding(new UndoLogBinding(kind, slot, firstPage, head), globalHighWater);
        return context;
    }
}
