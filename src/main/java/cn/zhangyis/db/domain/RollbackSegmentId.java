package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Rollback segment 标识（设计 §5.1/§6.2）。在一个 undo tablespace 内唯一，定位一条 rollback segment 及其
 * slot array、history list base。本片（T1.3c）固定单一默认 rollback segment，多 rseg 选择策略留后续片；
 * {@code value} 不进 {@link RollPointer} 编码（单 rseg/单 undo space 假设，见 {@code RollPointer} javadoc）。
 *
 * <p>用值对象而非裸 int 传递，避免与 {@code SpaceId}/{@code SegmentId} 混淆，并保护 rseg 目录定位不变量。
 *
 * @param value rollback segment 编号；非负。
 */
public record RollbackSegmentId(int value) {

    public RollbackSegmentId {
        if (value < 0) {
            throw new DatabaseValidationException("rollback segment id must be non-negative: " + value);
        }
    }

    /**
     * 构造一个 rollback segment id。
     *
     * @param value rollback segment 编号。
     * @return 通过校验的 rollback segment id。
     */
    public static RollbackSegmentId of(int value) {
        return new RollbackSegmentId(value);
    }
}
