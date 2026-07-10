package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.mtr.MtrRedoCategoryScope;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.UndoRecordPayloadRecord;

import java.nio.ByteBuffer;

/**
 * Undo/rseg 元数据逻辑 redo 收集小工具。Undo 模块仍通过 {@link PageGuard} 改真实页内容，本类把 header/slot
 * after-image 同步登记为 {@link UndoMetadataDeltaRecord}，并把对应物理字节写标为
 * {@link MtrRedoCategory#UNDO_PAGE_BYTES}。
 *
 * <p>0.19f 起 MTR 提交视图会精确过滤被 metadata after-image 完整覆盖的物理 {@code PAGE_BYTES}；未覆盖的
 * record payload 等字节仍保留物理 redo。1.4b 的 15B logical-head pair 也通过同一机制作为单条 delta 持久化。
 */
final class UndoRedoDeltas {

    private UndoRedoDeltas() {
    }

    /** 在 undo metadata 分类 scope 内执行页写入，使诊断能区分 undo/rseg 元数据物理字节。 */
    static void withUndoCategory(MiniTransaction mtr, String reason, Runnable action) {
        requireMtr(mtr);
        if (action == null) {
            throw new DatabaseValidationException("undo redo category action must not be null");
        }
        try (MtrRedoCategoryScope ignored = mtr.enterRedoCategory(MtrRedoCategory.UNDO_PAGE_BYTES, reason)) {
            action.run();
        }
    }

    /** 写 u8 字段并追加对应 undo metadata after-image delta。 */
    static void writeU8(MiniTransaction mtr, PageGuard guard, PageId pageId,
                        UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                        int offset, int value, String reason) {
        if (value < 0 || value > 0xFF) {
            throw new DatabaseValidationException("u8 out of range: " + value);
        }
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, new byte[]{(byte) value}, reason);
    }

    /** 写 u16 字段并追加对应 undo metadata after-image delta。 */
    static void writeU16(MiniTransaction mtr, PageGuard guard, PageId pageId,
                         UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                         int offset, int value, String reason) {
        if (value < 0 || value > 0xFFFF) {
            throw new DatabaseValidationException("u16 out of range: " + value);
        }
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset,
                new byte[]{(byte) (value >>> 8), (byte) value}, reason);
    }

    /** 写 u32/int 字段并追加对应 undo metadata after-image delta。 */
    static void writeInt(MiniTransaction mtr, PageGuard guard, PageId pageId,
                         UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                         int offset, int value, String reason) {
        byte[] image = ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写 long 字段并追加对应 undo metadata after-image delta。 */
    static void writeLong(MiniTransaction mtr, PageGuard guard, PageId pageId,
                          UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                          int offset, long value, String reason) {
        byte[] image = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写任意字节 after-image，并追加对应 undo metadata delta。 */
    static void writeBytes(MiniTransaction mtr, PageGuard guard, PageId pageId,
                           UndoMetadataDeltaKind kind, long subjectId, int subIndex,
                           int offset, byte[] image, String reason) {
        requireMtr(mtr);
        if (guard == null) {
            throw new DatabaseValidationException("undo metadata page guard must not be null");
        }
        byte[] copy = requireImage(image);
        withUndoCategory(mtr, reason, () -> guard.writeBytes(offset, copy));
        append(mtr, pageId, kind, subjectId, subIndex, offset, copy, reason);
    }

    /**
     * 写完整 undo record 槽并追加 payload logical redo。数据流：构造 {@code [len u16][payload]} after-image →
     * 在 UNDO_PAGE_BYTES 分类下写入真实页 → 追加 {@link UndoRecordPayloadRecord}。恢复期只 patch 槽镜像，
     * 不重新执行 appendRecord，因此不会重复推进 undo page header。
     */
    static void writeRecordPayload(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                   TransactionId transactionId, UndoNo undoNo, int recordOffset,
                                   byte[] payload, String reason) {
        requireMtr(mtr);
        if (guard == null || pageId == null || transactionId == null || undoNo == null) {
            throw new DatabaseValidationException(
                    "undo record payload mtr/guard/pageId/transactionId/undoNo must not be null");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("undo record payload redo reason must not be blank");
        }
        if (payload == null) {
            throw new DatabaseValidationException("undo record payload must not be null");
        }
        if (payload.length > 0xFFFF) {
            throw new DatabaseValidationException("undo record payload too large: " + payload.length);
        }
        byte[] slotImage = new byte[Short.BYTES + payload.length];
        slotImage[0] = (byte) (payload.length >>> 8);
        slotImage[1] = (byte) payload.length;
        System.arraycopy(payload, 0, slotImage, Short.BYTES, payload.length);
        withUndoCategory(mtr, reason, () -> guard.writeBytes(recordOffset, slotImage));
        mtr.appendLogicalRedo(new UndoRecordPayloadRecord(pageId, transactionId, undoNo, recordOffset, slotImage),
                MtrRedoCategory.UNDO_PAGE_BYTES, reason);
    }

    /** 只追加逻辑 delta；调用方已负责在正确分类 scope 内完成物理写。 */
    static void append(MiniTransaction mtr, PageId pageId, UndoMetadataDeltaKind kind,
                       long subjectId, int subIndex, int offset, byte[] image, String reason) {
        requireMtr(mtr);
        if (pageId == null || kind == null) {
            throw new DatabaseValidationException("undo metadata redo pageId/kind must not be null");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("undo metadata redo reason must not be blank");
        }
        mtr.appendLogicalRedo(new UndoMetadataDeltaRecord(
                        pageId, kind, subjectId, subIndex, offset, requireImage(image)),
                MtrRedoCategory.UNDO_PAGE_BYTES, reason);
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static byte[] requireImage(byte[] image) {
        if (image == null || image.length == 0) {
            throw new DatabaseValidationException("undo metadata after-image must not be null or empty");
        }
        return image.clone();
    }
}
