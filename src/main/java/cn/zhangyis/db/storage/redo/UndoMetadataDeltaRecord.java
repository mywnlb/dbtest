package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.Arrays;

/**
 * Undo/rseg metadata 字段 after-image redo。它把 rollback segment slot、undo page header 和 undo log header 的
 * 字段变化编码为稳定值；恢复期只执行页内 patch，不重新运行事务提交、slot claim/release、undo append 或 purge 逻辑。
 *
 * <p>0.19f 起生产 MTR 仍通过 PageGuard 写真实页内容以维持 dirty/pageLSN 语义，但提交视图会过滤掉被本 record
 * after-image 精确覆盖的 undo metadata {@link PageBytesRecord}。完整 undo record payload 尚未逻辑化，仍继续保留
 * 物理 {@code PAGE_BYTES}。
 *
 * @param pageId     被 patch 的 undo/rseg 页。
 * @param kind       稳定磁盘分类，用于审计和边界校验。
 * @param subjectId  rsegId、segmentId 或 0，避免恢复依赖 Java 对象图。
 * @param subIndex   slot index、inode slot 或字段内序号；无子索引时为 0。
 * @param offset     页内起始偏移。
 * @param afterImage 要覆盖的 after image；防御性复制。
 */
public record UndoMetadataDeltaRecord(
        PageId pageId,
        UndoMetadataDeltaKind kind,
        long subjectId,
        int subIndex,
        int offset,
        byte[] afterImage) implements RedoRecord {

    /** tag(1)+pageId(12)+kind(1)+subjectId(8)+subIndex(4)+offset(4)+payloadLen(4)。 */
    private static final int HEADER_BYTES = 34;

    public UndoMetadataDeltaRecord {
        if (pageId == null || kind == null) {
            throw new DatabaseValidationException("undo metadata delta pageId/kind must not be null");
        }
        if (subjectId < 0) {
            throw new DatabaseValidationException("undo metadata delta subject id must be non-negative: "
                    + subjectId);
        }
        if (subIndex < 0) {
            throw new DatabaseValidationException("undo metadata delta sub-index must be non-negative: "
                    + subIndex);
        }
        if (offset < 0) {
            throw new DatabaseValidationException("undo metadata delta offset must be non-negative: " + offset);
        }
        if (afterImage == null || afterImage.length == 0) {
            throw new DatabaseValidationException("undo metadata delta after image must not be null or empty");
        }
        afterImage = afterImage.clone();
    }

    /** 返回防御性副本，避免数组型 record 字段被外部修改。 */
    @Override
    public byte[] afterImage() {
        return afterImage.clone();
    }

    @Override
    public int byteLength() {
        return HEADER_BYTES + afterImage.length;
    }

    /**
     * record 默认会对数组字段做引用比较；redo payload 是值对象，codec round-trip 后必须按字节内容相等。
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UndoMetadataDeltaRecord that)) {
            return false;
        }
        return subjectId == that.subjectId
                && subIndex == that.subIndex
                && offset == that.offset
                && pageId.equals(that.pageId)
                && kind == that.kind
                && Arrays.equals(afterImage, that.afterImage);
    }

    @Override
    public int hashCode() {
        int result = pageId.hashCode();
        result = 31 * result + kind.hashCode();
        result = 31 * result + Long.hashCode(subjectId);
        result = 31 * result + Integer.hashCode(subIndex);
        result = 31 * result + Integer.hashCode(offset);
        result = 31 * result + Arrays.hashCode(afterImage);
        return result;
    }
}
