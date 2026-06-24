package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoSlotId;

import java.util.Map;

/**
 * rollback segment header 页（page3）读取快照：rseg 元数据 + 当前占用的 slot 映射（slot -> insert-undo 首页）。
 * 恢复期据此重建内存 {@code RollbackSegmentSlotManager}。空 slot 不进入 {@link #occupiedSlots}。
 *
 * @param rollbackSegmentId 页上记录的 rseg id。
 * @param slotCapacity      页上记录的 slot 容量。
 * @param occupiedSlots     当前占用的 slot -> 该 slot 登记的 insert-undo segment 首页（不可变）。
 */
public record RollbackSegmentHeaderSnapshot(RollbackSegmentId rollbackSegmentId, int slotCapacity,
                                            Map<UndoSlotId, PageId> occupiedSlots) {

    public RollbackSegmentHeaderSnapshot {
        if (rollbackSegmentId == null || occupiedSlots == null) {
            throw new DatabaseValidationException("rseg header snapshot fields must not be null");
        }
        occupiedSlots = Map.copyOf(occupiedSlots);
    }
}
