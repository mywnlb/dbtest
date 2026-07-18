package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoSlotId;

import java.util.Map;
import java.util.List;

/**
 * rollback segment header 页（page3）读取快照：rseg 元数据、当前占用的 active slot 映射，以及 INSERT/UPDATE
 * 两个持久 cached segment 栈、history base 及 free-list base。恢复期据此重建全部持久 owner；
 * cache 列表按栈底到栈顶排列。
 *
 * @param rollbackSegmentId 页上记录的 rseg id。
 * @param slotCapacity      页上记录的 slot 容量。
 * @param cacheCapacityPerKind 页上记录的每类 cache 栈容量。
 * @param occupiedSlots     当前占用的 slot -> 该 slot 登记的 insert-undo segment 首页（不可变）。
 * @param cachedInsertSegments INSERT cached 栈，按栈底到栈顶排列。
 * @param cachedUpdateSegments UPDATE cached 栈，按栈底到栈顶排列。
 * @param historyBase 持久 history 链首尾、长度及事务号高水位。
 * @param freeListBase 持久 free FIFO 首尾与长度。
 */
public record RollbackSegmentHeaderSnapshot(RollbackSegmentId rollbackSegmentId, int slotCapacity,
                                            int cacheCapacityPerKind,
                                            Map<UndoSlotId, PageId> occupiedSlots,
                                            List<PageId> cachedInsertSegments,
                                            List<PageId> cachedUpdateSegments,
                                            RollbackSegmentHistoryBase historyBase,
                                            RollbackSegmentFreeListBase freeListBase) {

    public RollbackSegmentHeaderSnapshot {
        if (rollbackSegmentId == null || occupiedSlots == null
                || cachedInsertSegments == null || cachedUpdateSegments == null
                || historyBase == null || freeListBase == null) {
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

    /** 返回指定普通 undo kind 的缓存栈；TEMPORARY 没有持久 cache。
     *
     * @param kind 选择 {@code cachedSegments} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code cachedSegments} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
