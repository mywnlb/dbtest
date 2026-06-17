package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.SegmentId;

/**
 * SegmentInode 投影（fragment 槽按槽单独访问）。逻辑 segment 归属：fragment slots + 三个 extent list base + 计数。
 *
 * @param inodeSlot         该 inode 在 page 2 中的槽下标。
 * @param segmentId         segment 逻辑编号。
 * @param purpose           segment 用途。
 * @param usedPageCount     已使用页计数。
 * @param reservedPageCount 预留页计数（reserve factor 未来使用）。
 * @param freeExtentList    SEG_FREE extent 链 base。
 * @param notFullExtentList SEG_NOT_FULL extent 链 base。
 * @param fullExtentList    SEG_FULL extent 链 base。
 */
public record SegmentInode(
        int inodeSlot,
        SegmentId segmentId,
        SegmentPurpose purpose,
        long usedPageCount,
        long reservedPageCount,
        FlstBase freeExtentList,
        FlstBase notFullExtentList,
        FlstBase fullExtentList) {
}
