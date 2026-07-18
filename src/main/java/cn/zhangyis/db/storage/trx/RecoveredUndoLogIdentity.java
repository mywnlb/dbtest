package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;

/** 恢复期同一 creator 下单条 ACTIVE undo log 的无 latch identity。
 *
 * @param kind 选择 {@code 构造} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 * @param slotId 参与 {@code 构造} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
 */
public record RecoveredUndoLogIdentity(UndoLogKind kind, UndoSlotId slotId, PageId firstPageId) {
    public RecoveredUndoLogIdentity {
        if (kind == null || kind == UndoLogKind.TEMPORARY || slotId == null || firstPageId == null) {
            throw new DatabaseValidationException("invalid recovered undo log identity");
        }
    }
}
