package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoSlotId;

import java.util.Map;
import java.util.List;

/**
 * rollback segment header 页（page3）读取快照：rseg 元数据、当前占用的 active slot 映射，以及 INSERT/UPDATE
 * 两个持久 cached segment 栈及 history base。恢复期据此分别重建 active slot、cache directory 与 history 链；
 * cache 列表按栈底到栈顶排列。
 *
 * @param rollbackSegmentId 页上记录的 rseg id。
 * @param slotCapacity      页上记录的 slot 容量。
 * @param cacheCapacityPerKind 页上记录的每类 cache 栈容量。
 * @param occupiedSlots     当前占用的 slot -> 该 slot 登记的 insert-undo segment 首页（不可变）。
 * @param cachedInsertSegments INSERT cached 栈，按栈底到栈顶排列。
 * @param cachedUpdateSegments UPDATE cached 栈，按栈底到栈顶排列。
 * @param historyBase 持久 history 链首尾、长度及事务号高水位。
 */
public record RollbackSegmentHeaderSnapshot(RollbackSegmentId rollbackSegmentId, int slotCapacity,
                                            int cacheCapacityPerKind,
                                            Map<UndoSlotId, PageId> occupiedSlots,
                                            List<PageId> cachedInsertSegments,
                                            List<PageId> cachedUpdateSegments,
                                            RollbackSegmentHistoryBase historyBase) {

    public RollbackSegmentHeaderSnapshot {
        if (rollbackSegmentId == null || occupiedSlots == null
                || cachedInsertSegments == null || cachedUpdateSegments == null || historyBase == null) {
            throw new DatabaseValidationException("rseg header snapshot fields must not be null");
        }
        if (slotCapacity <= 0 || cacheCapacityPerKind < 0
                || cachedInsertSegments.size() > cacheCapacityPerKind
                || cachedUpdateSegments.size() > cacheCapacityPerKind) {
            throw new DatabaseValidationException("rseg header snapshot capacities are invalid");
        }
        occupiedSlots = Map.copyOf(occupiedSlots);
        cachedInsertSegments = List.copyOf(cachedInsertSegments);
        cachedUpdateSegments = List.copyOf(cachedUpdateSegments);
    }

    /** 返回指定普通 undo kind 的缓存栈；TEMPORARY 没有持久 cache。 */
    public List<PageId> cachedSegments(UndoLogKind kind) {
        if (kind == null) {
            throw new DatabaseValidationException("cached undo kind must not be null");
        }
        return switch (kind) {
            case INSERT -> cachedInsertSegments;
            case UPDATE -> cachedUpdateSegments;
            case TEMPORARY -> throw new DatabaseValidationException("temporary undo has no cached stack");
        };
    }
}
