package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;

/** Undo append/finalization 的领域 workload 估算器；只消费事务/segment 只读事实，不访问 redo capacity。 */
public final class UndoRedoBudgetEstimator {

    private UndoRedoBudgetEstimator() {
    }

    /** 首写覆盖 create segment、首页、page3 claim 与 FSP 元数据；已有 segment 只覆盖 append/grow。 */
    public static RedoBudgetWorkload append(boolean firstWrite) {
        return RedoBudgetWorkload.pageImages(firstWrite ? 12 : 4);
    }

    /** external payload 每页覆盖 FSP allocation、PAGE_INIT 与完整 PAGE_BYTES，按 LOB 同级每页追加 8 份余量。 */
    public static RedoBudgetWorkload append(boolean firstWrite, int externalPages) {
        if (externalPages < 0) {
            throw new DatabaseValidationException("external undo page count must not be negative: " + externalPages);
        }
        try {
            return RedoBudgetWorkload.pageImages(Math.addExact(firstWrite ? 12L : 4L,
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
}
