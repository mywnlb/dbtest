package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/** External undo payload 单页格式化与读取原语；跨页一致性由 {@link UndoPayloadStorage} 统一验证。 */
final class UndoPayloadPage {

    private UndoPayloadPage() {
    }

    /** 在刚 PAGE_INIT(UNDO_PAYLOAD) 的页中写入完整不可变 body 与 FIL 链链接。 */
    static void format(PageGuard guard, PageId pageId, long previousPageNo, long nextPageNo,
                       int chunkIndex, UndoSegmentHandle handle, TransactionId transactionId, UndoNo undoNo,
                       int totalLength, int pageCount, long wholeCrc32, byte[] chunk) {
        if (guard == null || pageId == null || handle == null || transactionId == null
                || undoNo == null || chunk == null) {
            throw new DatabaseValidationException("undo payload page format args must not be null");
        }
        if (chunkIndex < 0 || chunk.length == 0 || totalLength <= 0 || pageCount <= 0
                || chunkIndex >= pageCount || transactionId.isNone() || undoNo.isNone()
                || wholeCrc32 < 0 || wholeCrc32 > 0xFFFF_FFFFL) {
            throw new DatabaseValidationException("undo payload page format bounds invalid");
        }
        PageEnvelope.writeHeader(guard, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                previousPageNo, nextPageNo, 0L, PageType.UNDO_PAYLOAD));
        guard.writeInt(UndoPayloadPageLayout.MAGIC, UndoPayloadPageLayout.MAGIC_VALUE);
        guard.writeInt(UndoPayloadPageLayout.VERSION, UndoPayloadPageLayout.VERSION_VALUE);
        guard.writeInt(UndoPayloadPageLayout.CHUNK_INDEX, chunkIndex);
        guard.writeInt(UndoPayloadPageLayout.CHUNK_LENGTH, chunk.length);
        guard.writeLong(UndoPayloadPageLayout.SEGMENT_ID, handle.segmentId().value());
        guard.writeInt(UndoPayloadPageLayout.INODE_SLOT, handle.inodeSlot());
        guard.writeLong(UndoPayloadPageLayout.TRANSACTION_ID, transactionId.value());
        guard.writeLong(UndoPayloadPageLayout.UNDO_NO, undoNo.value());
        guard.writeInt(UndoPayloadPageLayout.TOTAL_LENGTH, totalLength);
        guard.writeInt(UndoPayloadPageLayout.PAGE_COUNT, pageCount);
        guard.writeInt(UndoPayloadPageLayout.WHOLE_CRC32, (int) wholeCrc32);
        guard.writeBytes(UndoPayloadPageLayout.DATA, chunk);
    }

    /** 读取并校验单页局部边界；链顺序、归属和整体 CRC 在上层聚合校验。 */
    static Snapshot read(PageGuard guard, PageId expectedPageId, int payloadCapacity) {
        try {
            FilePageHeader header = PageEnvelope.readHeader(guard);
            if (!header.spaceId().equals(expectedPageId.spaceId())
                    || header.pageNo() != expectedPageId.pageNo().value()
                    || header.pageType() != PageType.UNDO_PAYLOAD) {
                throw new UndoLogFormatException("external undo payload envelope mismatch at " + expectedPageId);
            }
            int magic = guard.readInt(UndoPayloadPageLayout.MAGIC);
            int version = guard.readInt(UndoPayloadPageLayout.VERSION);
            int chunkIndex = guard.readInt(UndoPayloadPageLayout.CHUNK_INDEX);
            int chunkLength = guard.readInt(UndoPayloadPageLayout.CHUNK_LENGTH);
            long segmentId = guard.readLong(UndoPayloadPageLayout.SEGMENT_ID);
            int inodeSlot = guard.readInt(UndoPayloadPageLayout.INODE_SLOT);
            long transactionId = guard.readLong(UndoPayloadPageLayout.TRANSACTION_ID);
            long undoNo = guard.readLong(UndoPayloadPageLayout.UNDO_NO);
            int totalLength = guard.readInt(UndoPayloadPageLayout.TOTAL_LENGTH);
            int pageCount = guard.readInt(UndoPayloadPageLayout.PAGE_COUNT);
            long crc32 = guard.readInt(UndoPayloadPageLayout.WHOLE_CRC32) & 0xFFFF_FFFFL;
            if (magic != UndoPayloadPageLayout.MAGIC_VALUE || version != UndoPayloadPageLayout.VERSION_VALUE) {
                throw new UndoLogFormatException("external undo payload magic/version mismatch at " + expectedPageId);
            }
            if (chunkIndex < 0 || chunkLength <= 0 || chunkLength > payloadCapacity || segmentId <= 0
                    || inodeSlot < 0 || transactionId <= 0 || undoNo <= 0 || totalLength <= 0 || pageCount <= 0) {
                throw new UndoLogFormatException("external undo payload body bounds invalid at " + expectedPageId);
            }
            return new Snapshot(header.prevPageNo(), header.nextPageNo(), chunkIndex,
                    SegmentId.of(segmentId), inodeSlot, TransactionId.of(transactionId), UndoNo.of(undoNo),
                    totalLength, pageCount, crc32,
                    guard.readBytes(UndoPayloadPageLayout.DATA, chunkLength));
        } catch (UndoLogFormatException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new UndoLogFormatException("cannot decode external undo payload page " + expectedPageId, error);
        }
    }

    /** 已复制出 PageGuard 生命周期的单页快照。 */
    record Snapshot(long previousPageNo, long nextPageNo, int chunkIndex, SegmentId segmentId, int inodeSlot,
                    TransactionId transactionId, UndoNo undoNo, int totalLength, int pageCount,
                    long wholeCrc32, byte[] chunk) {
    }
}
