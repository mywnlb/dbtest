package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * undo segment drop 前由 FSP inode 只读物化的稳定规模快照。
 *
 * @param fragmentPageCount inode 32 个 fragment 槽中已占用数量。
 * @param extentCount SEG_FREE/NOT_FULL/FULL 三条 extent 链节点总数。
 * @param usedPageCount inode 权威已用页计数，用于交叉校验 fragment 账本。
 */
public record UndoSegmentDropPlan(long fragmentPageCount, long extentCount, long usedPageCount) {

    public UndoSegmentDropPlan {
        if (fragmentPageCount < 0 || fragmentPageCount > 32 || extentCount < 0 || usedPageCount < 0) {
            throw new DatabaseValidationException("undo segment drop plan values are invalid: fragments="
                    + fragmentPageCount + ", extents=" + extentCount + ", usedPages=" + usedPageCount);
        }
        if (fragmentPageCount > usedPageCount) {
            throw new DatabaseValidationException("undo fragment count exceeds used page count: fragments="
                    + fragmentPageCount + ", usedPages=" + usedPageCount);
        }
    }
}
