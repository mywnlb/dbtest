package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;

import java.util.Collection;
import java.util.List;

/** Undo append/finalization 的领域 workload 估算器；只消费事务/segment 只读事实，不访问 redo capacity。 */
public final class UndoRedoBudgetEstimator {

    private UndoRedoBudgetEstimator() {
    }

    /** fresh 首写覆盖 FSP create，cached 首写覆盖 page3 owner move/header reset，existing 只覆盖 append/grow。 */
    public static RedoBudgetWorkload append(UndoSegmentAcquisition acquisition) {
        if (acquisition == null) {
            throw new DatabaseValidationException("undo append acquisition must not be null");
        }
        return RedoBudgetWorkload.pageImages(switch (acquisition) {
            case ALLOCATE_NEW -> 12L;
            case REUSE_CACHED -> 8L;
            case APPEND_EXISTING -> 4L;
        });
    }

    /**
     * 兼容只区分首写与追加的预算调用方。生产写路径必须传入明确的 segment 获取方式，
     * 否则无法表达 cached segment 激活所需的 page 3 owner 转移与首页重置开销。
     */
    public static RedoBudgetWorkload append(boolean firstUndoWrite) {
        return append(firstUndoWrite ? UndoSegmentAcquisition.ALLOCATE_NEW
                : UndoSegmentAcquisition.APPEND_EXISTING);
    }

    /** external payload 每页覆盖 FSP allocation、PAGE_INIT 与完整 PAGE_BYTES，按 LOB 同级每页追加 8 份余量。 */
    public static RedoBudgetWorkload append(UndoSegmentAcquisition acquisition, int externalPages) {
        if (acquisition == null) {
            throw new DatabaseValidationException("undo append acquisition must not be null");
        }
        if (externalPages < 0) {
            throw new DatabaseValidationException("external undo page count must not be negative: " + externalPages);
        }
        try {
            long base = switch (acquisition) {
                case ALLOCATE_NEW -> 12L;
                case REUSE_CACHED -> 8L;
                case APPEND_EXISTING -> 4L;
            };
            return RedoBudgetWorkload.pageImages(Math.addExact(base,
                    Math.multiplyExact(8L, externalPages)));
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("external undo redo workload overflows", error);
        }
    }

    /**
     * drop 固定覆盖 page0/page2/page3、inode/slot 与发布边界；每 fragment 计两份、每 extent 计四份元数据余量。
     */
    public static RedoBudgetWorkload finalization(UndoSegmentDropPlan plan, boolean includesTerminalDelta) {
        if (plan == null) {
            throw new DatabaseValidationException("undo finalization drop plan must not be null");
        }
        try {
            long pages = 7L;
            pages = Math.addExact(pages, Math.multiplyExact(2L, plan.fragmentPageCount()));
            pages = Math.addExact(pages, Math.multiplyExact(4L, plan.extentCount()));
            if (includesTerminalDelta) {
                pages = Math.addExact(pages, 1L);
            }
            return RedoBudgetWorkload.pageImages(pages);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("undo finalization redo workload overflows", error);
        }
    }

    /** 多 segment 原子终结只计算一次 batch/page3/terminal 固定开销，各 drop plan 的动态规模分别累加。 */
    public static RedoBudgetWorkload finalization(Collection<UndoSegmentDropPlan> plans,
                                                   boolean includesTerminalDelta) {
        if (plans == null || plans.isEmpty() || plans.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("undo finalization plans must not be empty or contain null");
        }
        try {
            long pages = 7L;
            for (UndoSegmentDropPlan plan : plans) {
                pages = Math.addExact(pages, Math.multiplyExact(2L, plan.fragmentPageCount()));
                pages = Math.addExact(pages, Math.multiplyExact(4L, plan.extentCount()));
                pages = Math.addExact(pages, 2L);
            }
            if (includesTerminalDelta) {
                pages = Math.addExact(pages, 1L);
            }
            return RedoBudgetWorkload.pageImages(pages);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("multi-segment undo finalization workload overflows", error);
        }
    }

    /**
     * cache/drop 混合终结预算。drop 继续按 fragment/extent 规模计费；每个 cached segment 额外覆盖 page3 owner
     * transition、首页 header reset 与重复 metadata delta 余量。允许 droppedPlans 为空，但 cachedCount 必须为正。
     */
    public static RedoBudgetWorkload finalization(Collection<UndoSegmentDropPlan> droppedPlans,
                                                   int cachedCount,
                                                   boolean includesTerminalDelta) {
        if (droppedPlans == null || droppedPlans.stream().anyMatch(java.util.Objects::isNull)
                || cachedCount < 0 || droppedPlans.isEmpty() && cachedCount == 0) {
            throw new DatabaseValidationException("mixed undo finalization workload is invalid");
        }
        try {
            long pages = 7L;
            for (UndoSegmentDropPlan plan : droppedPlans) {
                pages = Math.addExact(pages, Math.multiplyExact(2L, plan.fragmentPageCount()));
                pages = Math.addExact(pages, Math.multiplyExact(4L, plan.extentCount()));
                pages = Math.addExact(pages, 2L);
            }
            pages = Math.addExact(pages, Math.multiplyExact(4L, cachedCount));
            if (includesTerminalDelta) {
                pages = Math.addExact(pages, 1L);
            }
            return RedoBudgetWorkload.pageImages(pages);
        } catch (ArithmeticException error) {
            throw new DatabaseValidationException("mixed undo finalization workload overflows", error);
        }
    }

    /** UPDATE header 加 terminal delta；mixed commit 再合并 INSERT drop plan。 */
    public static RedoBudgetWorkload commit(UndoSegmentDropPlan insertDropPlan) {
        if (insertDropPlan == null) {
            return RedoBudgetWorkload.pageImages(3L);
        }
        return finalization(java.util.List.of(insertDropPlan), true).plus(RedoBudgetWorkload.pageImages(2L));
    }

    /** mixed/INSERT commit 根据最终 disposition 选择 drop 或 cached header reset 上界。 */
    public static RedoBudgetWorkload commit(UndoSegmentDropPlan insertPlan, boolean cacheInsert) {
        if (insertPlan == null) {
            return commit(null);
        }
        if (cacheInsert) {
            return finalization(List.of(), 1, true).plus(RedoBudgetWorkload.pageImages(2L));
        }
        return finalization(List.of(insertPlan), 0, true).plus(RedoBudgetWorkload.pageImages(2L));
    }
}
