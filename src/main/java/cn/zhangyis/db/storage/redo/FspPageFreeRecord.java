package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;

/**
 * FSP 页释放意图 redo。它记录某个 segment 在本 MTR 中释放了一个物理页号；恢复期不重新执行 free-list
 * 或 segment 状态机，真实账本字段变化由同批 {@link FspMetadataDeltaRecord} 承载。
 *
 * @param freedPageId 被释放的数据页。
 * @param inodeSlot   segment inode 槽位，用于诊断和与 metadata delta 对齐。
 * @param segmentId   segment 逻辑编号，用于审计释放归属。
 */
public record FspPageFreeRecord(PageId freedPageId, int inodeSlot, SegmentId segmentId) implements RedoRecord {

    /** tag(1) + pageId(space 4 + pageNo 8) + inodeSlot(4) + segmentId(8)。 */
    private static final int LENGTH = 25;

    public FspPageFreeRecord {
        if (freedPageId == null || segmentId == null) {
            throw new DatabaseValidationException("FSP page free record pageId/segmentId must not be null");
        }
        if (inodeSlot < 0) {
            throw new DatabaseValidationException("FSP page free record inode slot must be non-negative: "
                    + inodeSlot);
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("FSP page free record segment id must be positive: "
                    + segmentId.value());
        }
    }

    @Override
    public int byteLength() {
        return LENGTH;
    }
}
