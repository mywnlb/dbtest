package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;

/**
 * Segment 句柄：定位某表空间内一个 segment 的 inode 槽与逻辑编号。由 createSegment 返回，allocate/free/drop 传入。
 *
 * @param spaceId   所属表空间。
 * @param inodeSlot page2 inode 槽下标。
 * @param segmentId segment 逻辑编号（&gt;0）。
 */
public record SegmentRef(SpaceId spaceId, int inodeSlot, SegmentId segmentId) {

    public SegmentRef {
        if (spaceId == null || segmentId == null) {
            throw new DatabaseValidationException("segment ref fields must not be null");
        }
        if (inodeSlot < 0) {
            throw new DatabaseValidationException("inode slot must be non-negative: " + inodeSlot);
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("segment id must be positive: " + segmentId.value());
        }
    }
}
