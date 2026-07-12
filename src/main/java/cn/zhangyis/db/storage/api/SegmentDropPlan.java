package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * DiskSpaceManager 在 drop 写批次前从 inode page2 物化的 segment 规模快照。
 *
 * @param fragmentPageCount inode fragment 槽的占用数，用于估算逐页归还的元数据写放大。
 * @param extentCount 三条 segment extent 链的持久节点总数，用于估算摘链/XDES/FSP 写放大。
 * @param usedPageCount inode 权威已用页计数；用于拒绝 fragment 账本超过总用量的损坏状态。
 */
public record SegmentDropPlan(long fragmentPageCount, long extentCount, long usedPageCount) {

    public SegmentDropPlan {
        if (fragmentPageCount < 0 || fragmentPageCount > 32 || extentCount < 0 || usedPageCount < 0
                || fragmentPageCount > usedPageCount) {
            throw new DatabaseValidationException("segment drop plan values are invalid: fragments="
                    + fragmentPageCount + ", extents=" + extentCount + ", usedPages=" + usedPageCount);
        }
    }
}
