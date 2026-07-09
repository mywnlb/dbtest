package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.Arrays;

/**
 * B+Tree 结构页 after-image redo。它把 split/merge/root shrink 等结构维护中稳定的页字段变化编码为
 * page-local patch，恢复期不重新执行 B+Tree 算法，也不依赖当前内存索引对象图。
 *
 * <p>0.19h v1 先接入 sibling link 字段；record row bytes、node pointer 数组和 root format 仍允许继续由
 * {@link PageBytesRecord} 保护，后续可逐步迁移为更细的结构 delta。
 *
 * @param pageId     被 patch 的索引页。
 * @param indexId    索引 id，用于诊断和后续恢复阶段定位。
 * @param kind       稳定磁盘分类。
 * @param subjectId  结构变化主体，例如 sibling 页号；无主体时为 0。
 * @param offset     页内起始 offset。
 * @param afterImage 要覆盖的 after-image；防御性复制。
 */
public record BTreePageDeltaRecord(
        PageId pageId,
        long indexId,
        BTreePageDeltaKind kind,
        long subjectId,
        int offset,
        byte[] afterImage) implements RedoRecord {

    /** tag(1)+pageId(12)+indexId(8)+kind(1)+subjectId(8)+offset(4)+payloadLen(4)。 */
    private static final int HEADER_BYTES = 38;

    public BTreePageDeltaRecord {
        if (pageId == null || kind == null) {
            throw new DatabaseValidationException("B+Tree page delta pageId/kind must not be null");
        }
        if (indexId < 0) {
            throw new DatabaseValidationException("B+Tree page delta index id must be non-negative: " + indexId);
        }
        if (subjectId < 0) {
            throw new DatabaseValidationException("B+Tree page delta subject id must be non-negative: "
                    + subjectId);
        }
        if (offset < 0) {
            throw new DatabaseValidationException("B+Tree page delta offset must be non-negative: " + offset);
        }
        if (afterImage == null || afterImage.length == 0) {
            throw new DatabaseValidationException("B+Tree page delta after image must not be null or empty");
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

    /** redo payload 是值对象，数组字段按内容比较。 */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BTreePageDeltaRecord that)) {
            return false;
        }
        return indexId == that.indexId
                && subjectId == that.subjectId
                && offset == that.offset
                && pageId.equals(that.pageId)
                && kind == that.kind
                && Arrays.equals(afterImage, that.afterImage);
    }

    @Override
    public int hashCode() {
        int result = pageId.hashCode();
        result = 31 * result + Long.hashCode(indexId);
        result = 31 * result + kind.hashCode();
        result = 31 * result + Long.hashCode(subjectId);
        result = 31 * result + Integer.hashCode(offset);
        result = 31 * result + Arrays.hashCode(afterImage);
        return result;
    }
}
