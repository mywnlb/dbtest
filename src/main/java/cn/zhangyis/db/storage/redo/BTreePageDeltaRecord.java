package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.Arrays;

/**
 * B+Tree 结构页 after-image redo。它把 split/merge/root shrink 等结构维护中稳定的页字段变化编码为
 * page-local patch，恢复期不重新执行 B+Tree 算法，也不依赖当前内存索引对象图。
 *
 * <p>0.19h v1 接入 sibling link；后续切片复用预留 kind 接入 internal header、node-pointer used heap/directory
 * 与 root level/index identity。leaf row bytes 继续由 {@link PageBytesRecord} 保护；同值物理写由 MTR 提交视图精确过滤。
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
    /** INDEX page header 固定布局 `[38,66)`。 */
    public static final int INDEX_HEADER_OFFSET = 38;
    /** INDEX page header 固定字节数。 */
    public static final int INDEX_HEADER_BYTES = 28;
    /** PAGE_LEVEL(u16)+INDEX_ID(u64) 固定布局 `[56,66)`。 */
    public static final int ROOT_IDENTITY_OFFSET = 56;
    /** root level/index identity 固定字节数。 */
    public static final int ROOT_IDENTITY_BYTES = 10;
    /** node pointer heap 最早从 infimum 起始偏移 66 开始。 */
    public static final int NODE_AREA_MIN_OFFSET = 66;

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
        validateKindLayout(kind, offset, afterImage.length);
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

    /** kind 是稳定磁盘语义，固定结构字段必须在读写两侧 fail-closed，不能退化成任意 offset patch。 */
    private static void validateKindLayout(BTreePageDeltaKind kind, int offset, int length) {
        switch (kind) {
            case SIBLING_LINKS -> {
                if (offset != 12 || length != 8) {
                    throw new DatabaseValidationException("B+Tree sibling delta layout must be [12,20)");
                }
            }
            case PAGE_FORMAT_IMAGE -> {
                if (offset != INDEX_HEADER_OFFSET || length != INDEX_HEADER_BYTES) {
                    throw new DatabaseValidationException("B+Tree page format delta layout must be [38,66)");
                }
            }
            case NODE_POINTER_AREA -> {
                if (offset < NODE_AREA_MIN_OFFSET) {
                    throw new DatabaseValidationException("B+Tree node pointer delta starts before record body: "
                            + offset);
                }
            }
            case ROOT_LEVEL_OR_HEADER -> {
                boolean fullHeader = offset == INDEX_HEADER_OFFSET && length == INDEX_HEADER_BYTES;
                boolean identityOnly = offset == ROOT_IDENTITY_OFFSET && length == ROOT_IDENTITY_BYTES;
                if (!fullHeader && !identityOnly) {
                    throw new DatabaseValidationException(
                            "B+Tree root delta must cover [38,66) or [56,66)");
                }
            }
        }
    }
}
