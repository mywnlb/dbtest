package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Segment 逻辑编号。Segment 的权威状态来自 INODE entry，而不是连续物理文件区域。
 *
 * @param value tablespace 内 segment 逻辑编号；后续 SegmentInodeRepository 用它定位 INODE entry。
 */
public record SegmentId(long value) {

    public SegmentId {
        if (value < 0) {
            throw new DatabaseValidationException("segment id must be non-negative");
        }
    }

    /**
     * 创建 segment 逻辑编号；负数不能映射到有效 INODE entry。
     *
     * @param value segment 编号。
     * @return 通过校验的 segment 编号。
     */
    public static SegmentId of(long value) {
        return new SegmentId(value);
    }
}
