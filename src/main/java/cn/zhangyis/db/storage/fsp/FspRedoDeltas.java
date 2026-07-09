package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.mtr.MtrRedoCategoryScope;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaRecord;

import java.nio.ByteBuffer;

/**
 * FSP 元数据逻辑 redo 收集小工具。FSP 仓储仍通过 {@link PageGuard} 改页并保留兼容期 {@code PAGE_BYTES}；
 * 本类负责把同一次写入登记为 {@link FspMetadataDeltaRecord}，并在写入期间把物理字节 redo 标成
 * {@link MtrRedoCategory#FSP_METADATA_BYTES} 便于审计。
 */
public final class FspRedoDeltas {

    private FspRedoDeltas() {
    }

    /** 在 FSP metadata 分类 scope 内执行一段页写入，确保 PageGuard listener 生成的 PAGE_BYTES 带本地分类。 */
    public static void withFspCategory(MiniTransaction mtr, String reason, Runnable action) {
        requireMtr(mtr);
        if (action == null) {
            throw new DatabaseValidationException("FSP redo category action must not be null");
        }
        try (MtrRedoCategoryScope ignored = mtr.enterRedoCategory(MtrRedoCategory.FSP_METADATA_BYTES, reason)) {
            action.run();
        }
    }

    /** 写 int 字段并追加对应 FSP metadata after-image delta。 */
    public static void writeInt(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                int offset, int value, String reason) {
        byte[] image = ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写 long 字段并追加对应 FSP metadata after-image delta。 */
    public static void writeLong(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                 FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                 int offset, long value, String reason) {
        byte[] image = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        writeBytes(mtr, guard, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写 fil_addr_t 字段并追加对应 FSP metadata after-image delta。 */
    public static void writeAddress(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                    FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                    int offset, FileAddress address, String reason) {
        if (address == null) {
            throw new DatabaseValidationException("FSP metadata file address must not be null");
        }
        byte[] image = new byte[Long.BYTES + Integer.BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(image);
        if (address.isNull()) {
            buffer.putLong(0L).putInt(0);
        } else {
            buffer.putLong(address.pageNo().value()).putInt(address.offset());
        }
        withFspCategory(mtr, reason, () -> address.writeTo(guard, offset));
        append(mtr, pageId, kind, subjectId, subIndex, offset, image, reason);
    }

    /** 写任意字节 after image，并追加对应 FSP metadata delta。 */
    public static void writeBytes(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                  FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                  int offset, byte[] image, String reason) {
        requireMtr(mtr);
        if (guard == null) {
            throw new DatabaseValidationException("FSP metadata page guard must not be null");
        }
        byte[] copy = requireImage(image);
        withFspCategory(mtr, reason, () -> guard.writeBytes(offset, copy));
        append(mtr, pageId, kind, subjectId, subIndex, offset, copy, reason);
    }

    /** 从已写入的 guard 读取 after image，并追加 FSP metadata delta。 */
    public static void recordAfterImage(MiniTransaction mtr, PageGuard guard, PageId pageId,
                                        FspMetadataDeltaKind kind, long subjectId, int subIndex,
                                        int offset, int length, String reason) {
        requireMtr(mtr);
        if (guard == null) {
            throw new DatabaseValidationException("FSP metadata page guard must not be null");
        }
        if (length <= 0) {
            throw new DatabaseValidationException("FSP metadata after-image length must be positive: " + length);
        }
        append(mtr, pageId, kind, subjectId, subIndex, offset, guard.readBytes(offset, length), reason);
    }

    /** 只追加逻辑 delta；调用方已负责在正确分类 scope 内完成物理写。 */
    public static void append(MiniTransaction mtr, PageId pageId, FspMetadataDeltaKind kind,
                              long subjectId, int subIndex, int offset, byte[] image, String reason) {
        requireMtr(mtr);
        if (pageId == null || kind == null) {
            throw new DatabaseValidationException("FSP metadata redo pageId/kind must not be null");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("FSP metadata redo reason must not be blank");
        }
        mtr.appendLogicalRedo(new FspMetadataDeltaRecord(
                        pageId, kind, subjectId, subIndex, offset, requireImage(image)),
                MtrRedoCategory.FSP_METADATA_BYTES, reason);
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static byte[] requireImage(byte[] image) {
        if (image == null || image.length == 0) {
            throw new DatabaseValidationException("FSP metadata after-image must not be null or empty");
        }
        return image.clone();
    }
}
