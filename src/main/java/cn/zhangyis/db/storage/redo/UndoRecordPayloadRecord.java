package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;

import java.util.Arrays;

/**
 * 完整 undo record 槽 after-image redo。它只覆盖页内 {@code [len u16][payload]} 槽字节，
 * undo page header / undo log header 仍由 {@link UndoMetadataDeltaRecord} 表达。
 *
 * <p>恢复期只能按 {@link #pageId()}、{@link #recordOffset()} 和 {@link #slotImage()} 做页内 patch，
 * 不能重新运行 undo append 状态机；否则会重复推进 free offset、record count 或整链 header，破坏 MTR 幂等边界。
 *
 * @param pageId        槽所在 undo 页。
 * @param transactionId 生成该 undo record 的事务写 id，用于诊断和后续恢复阶段关联。
 * @param undoNo        事务内 undo 序号，真实 record 必须大于 0。
 * @param recordOffset  槽起始 offset，指向 len 前缀。
 * @param slotImage     完整槽 after-image；前 2 字节必须等于 payload 长度。
 */
public record UndoRecordPayloadRecord(
        PageId pageId,
        TransactionId transactionId,
        UndoNo undoNo,
        int recordOffset,
        byte[] slotImage) implements RedoRecord {

    /** tag(1)+pageId(12)+transactionId(8)+undoNo(8)+recordOffset(4)+payloadLen(4)。 */
    private static final int HEADER_BYTES = 37;

    public UndoRecordPayloadRecord {
        if (pageId == null || transactionId == null || undoNo == null) {
            throw new DatabaseValidationException("undo record payload pageId/transactionId/undoNo must not be null");
        }
        if (undoNo.isNone()) {
            throw new DatabaseValidationException("undo record payload undoNo must not be NONE");
        }
        if (recordOffset < 0) {
            throw new DatabaseValidationException("undo record payload offset must be non-negative: "
                    + recordOffset);
        }
        if (slotImage == null || slotImage.length < Short.BYTES) {
            throw new DatabaseValidationException("undo record payload slot image must include len prefix");
        }
        int declaredPayloadLength = ((slotImage[0] & 0xFF) << 8) | (slotImage[1] & 0xFF);
        if (declaredPayloadLength != slotImage.length - Short.BYTES) {
            throw new DatabaseValidationException("undo record payload len prefix " + declaredPayloadLength
                    + " does not match slot image length " + slotImage.length);
        }
        slotImage = slotImage.clone();
    }

    /** 返回防御性副本，避免数组型 record 字段被外部修改。 */
    @Override
    public byte[] slotImage() {
        return slotImage.clone();
    }

    @Override
    public int byteLength() {
        return HEADER_BYTES + slotImage.length;
    }

    /** redo payload 是值对象，数组字段按内容比较。 */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UndoRecordPayloadRecord that)) {
            return false;
        }
        return recordOffset == that.recordOffset
                && pageId.equals(that.pageId)
                && transactionId.equals(that.transactionId)
                && undoNo.equals(that.undoNo)
                && Arrays.equals(slotImage, that.slotImage);
    }

    @Override
    public int hashCode() {
        int result = pageId.hashCode();
        result = 31 * result + transactionId.hashCode();
        result = 31 * result + undoNo.hashCode();
        result = 31 * result + Integer.hashCode(recordOffset);
        result = 31 * result + Arrays.hashCode(slotImage);
        return result;
    }
}
