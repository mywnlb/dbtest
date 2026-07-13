package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/** LOB 页格式化/读取访问器；所有校验都在持目标页 latch 时完成，不跨页保存 PageGuard。 */
final class LobPage {

    private LobPage() {
    }

    /**
     * 在已由 PAGE_INIT(BLOB) 创建的页中写入完整 body 和 FIL 链链接。整页写均被 MTR collector 收集为 PAGE_BYTES。
     */
    static void format(PageGuard guard, PageId pageId, long previousPageNo, long nextPageNo,
                       int chunkIndex, int pageCount, SegmentId segmentId, int inodeSlot,
                       int totalLength, long wholeCrc32, byte[] chunk) {
        if (guard == null || pageId == null || segmentId == null || chunk == null) {
            throw new DatabaseValidationException("LOB page format arguments must not be null");
        }
        PageEnvelope.writeHeader(guard, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                previousPageNo, nextPageNo, 0L, PageType.BLOB));
        guard.writeInt(LobPageLayout.MAGIC, LobPageLayout.MAGIC_VALUE);
        guard.writeInt(LobPageLayout.VERSION, LobPageLayout.VERSION_VALUE);
        guard.writeInt(LobPageLayout.CHUNK_INDEX, chunkIndex);
        guard.writeInt(LobPageLayout.CHUNK_LENGTH, chunk.length);
        guard.writeLong(LobPageLayout.SEGMENT_ID, segmentId.value());
        guard.writeInt(LobPageLayout.INODE_SLOT, inodeSlot);
        guard.writeInt(LobPageLayout.TOTAL_LENGTH, totalLength);
        guard.writeInt(LobPageLayout.WHOLE_CRC32, (int) wholeCrc32);
        guard.writeInt(LobPageLayout.PAGE_COUNT, pageCount);
        guard.writeBytes(LobPageLayout.DATA, chunk);
    }

    /** 读取并校验单页局部格式；跨页 index/link/CRC 不变量由 LobStorage 结合 reference 检查。 */
    static Snapshot read(PageGuard guard, PageId expectedPageId, int payloadCapacity) {
        try {
            FilePageHeader header = PageEnvelope.readHeader(guard);
            if (!header.spaceId().equals(expectedPageId.spaceId())
                    || header.pageNo() != expectedPageId.pageNo().value()
                    || header.pageType() != PageType.BLOB) {
                throw new LobPageCorruptedException("LOB page envelope mismatch at " + expectedPageId
                        + ": " + header);
            }
            int magic = guard.readInt(LobPageLayout.MAGIC);
            int version = guard.readInt(LobPageLayout.VERSION);
            int chunkIndex = guard.readInt(LobPageLayout.CHUNK_INDEX);
            int chunkLength = guard.readInt(LobPageLayout.CHUNK_LENGTH);
            long segmentId = guard.readLong(LobPageLayout.SEGMENT_ID);
            int inodeSlot = guard.readInt(LobPageLayout.INODE_SLOT);
            int totalLength = guard.readInt(LobPageLayout.TOTAL_LENGTH);
            long crc32 = guard.readInt(LobPageLayout.WHOLE_CRC32) & 0xFFFF_FFFFL;
            int pageCount = guard.readInt(LobPageLayout.PAGE_COUNT);
            if (magic != LobPageLayout.MAGIC_VALUE || version != LobPageLayout.VERSION_VALUE) {
                throw new LobPageCorruptedException("LOB magic/version mismatch at " + expectedPageId);
            }
            if (chunkIndex < 0 || chunkLength <= 0 || chunkLength > payloadCapacity
                    || segmentId <= 0 || inodeSlot < 0 || totalLength <= 0 || pageCount <= 0) {
                throw new LobPageCorruptedException("LOB body bounds invalid at " + expectedPageId);
            }
            return new Snapshot(header.prevPageNo(), header.nextPageNo(), chunkIndex, pageCount,
                    SegmentId.of(segmentId), inodeSlot, totalLength, crc32,
                    guard.readBytes(LobPageLayout.DATA, chunkLength));
        } catch (LobPageCorruptedException corrupted) {
            throw corrupted;
        } catch (RuntimeException decodeFailure) {
            throw new LobPageCorruptedException("cannot decode LOB page " + expectedPageId, decodeFailure);
        }
    }

    /** 已校验的单页快照；byte[] 只在当前 LOB 操作内部传递。 */
    record Snapshot(long previousPageNo, long nextPageNo, int chunkIndex, int pageCount,
                    SegmentId segmentId, int inodeSlot, int totalLength, long wholeCrc32, byte[] chunk) {
    }
}
