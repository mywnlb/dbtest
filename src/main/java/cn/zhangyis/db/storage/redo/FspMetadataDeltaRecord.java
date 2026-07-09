package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.Arrays;

/**
 * FSP metadata 字段 after-image redo。它把“哪个 FSP 元数据对象发生了哪类字段变化”编码为稳定值，
 * 恢复期只按 pageId/offset/afterImage 执行页内 patch，不重新运行 allocator、FLST 或 segment 策略。
 *
 * <p>该 record 与现有 {@link PageBytesRecord} 在 0.19c 并存：生产 MTR 仍通过 PageGuard 写监听收集物理字节，
 * 同时追加本 record 作为可审计、可独立重放的逻辑 delta。后续切片才能在不破坏 pageLSN/touched-page 语义的前提下
 * 去重或替代 FSP metadata 的物理字节 redo。
 *
 * @param pageId     被 patch 的 FSP 元数据页，当前主要是 page0 或 page2。
 * @param kind       稳定磁盘分类，用于审计和边界校验。
 * @param subjectId  extentNo、inodeSlot 或 0，避免恢复依赖 Java 对象图。
 * @param subIndex   bitmap byte、fragment slot 或字段内序号；无子索引时为 0。
 * @param offset     页内起始偏移。
 * @param afterImage 要覆盖的 after image；防御性复制。
 */
public record FspMetadataDeltaRecord(
        PageId pageId,
        FspMetadataDeltaKind kind,
        long subjectId,
        int subIndex,
        int offset,
        byte[] afterImage) implements RedoRecord {

    /** tag(1)+pageId(12)+kind(1)+subjectId(8)+subIndex(4)+offset(4)+payloadLen(4)。 */
    private static final int HEADER_BYTES = 34;

    public FspMetadataDeltaRecord {
        if (pageId == null || kind == null) {
            throw new DatabaseValidationException("FSP metadata delta pageId/kind must not be null");
        }
        if (subjectId < 0) {
            throw new DatabaseValidationException("FSP metadata delta subject id must be non-negative: "
                    + subjectId);
        }
        if (subIndex < 0) {
            throw new DatabaseValidationException("FSP metadata delta sub-index must be non-negative: "
                    + subIndex);
        }
        if (offset < 0) {
            throw new DatabaseValidationException("FSP metadata delta offset must be non-negative: " + offset);
        }
        if (afterImage == null || afterImage.length == 0) {
            throw new DatabaseValidationException("FSP metadata delta after image must not be null or empty");
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
        if (!(obj instanceof FspMetadataDeltaRecord that)) {
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
