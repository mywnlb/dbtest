package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;

/**
 * FSP 页分配意图 redo。它记录某个 segment 在本 MTR 中获得了一个物理页号，恢复阶段只据此确保物理
 * 文件至少覆盖该页；page0/page2/XDES/INODE 的账本字段由同一 batch 内的 {@link FspMetadataDeltaRecord}
 * 重放，未迁移的页信封或生命周期字节仍可由 {@link PageBytesRecord} 保护。
 *
 * <p>该 record 不携带 Java allocator 决策对象，也不要求 recovery 重新选择 free-list 或 segment extent。
 * 这样可以把 0.19b 控制在“持久逻辑 record + handler 接线”的范围内，同时避免恢复期重新执行前台分配策略。
 *
 * @param allocatedPageId 被分配的数据页；handler 以它作为 affected page 和 ensureCapacity 目标。
 * @param inodeSlot       segment inode 槽位，供诊断与后续完整 INODE redo 对齐。
 * @param segmentId       segment 逻辑编号，供诊断与恢复一致性测试校验。
 * @param autoExtendRetry true 表示该页号来自 DiskSpaceManager autoextend 后的第二次 allocator 尝试。
 */
public record FspPageAllocationRecord(
        PageId allocatedPageId,
        int inodeSlot,
        SegmentId segmentId,
        boolean autoExtendRetry) implements RedoRecord {

    /** tag(1) + pageId(space 4 + pageNo 8) + inodeSlot(4) + segmentId(8) + autoExtendRetry(1)。
     *
     * 稳定布局常量，参与页内偏移、长度或位域计算；编解码两端必须保持完全一致。
     */
    private static final int LENGTH = 26;

    public FspPageAllocationRecord {
        if (allocatedPageId == null || segmentId == null) {
            throw new DatabaseValidationException("FSP allocation record pageId/segmentId must not be null");
        }
        if (inodeSlot < 0) {
            throw new DatabaseValidationException("FSP allocation record inode slot must be non-negative: "
                    + inodeSlot);
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("FSP allocation record segment id must be positive: "
                    + segmentId.value());
        }
    }

    @Override
    public int byteLength() {
        return LENGTH;
    }
}
