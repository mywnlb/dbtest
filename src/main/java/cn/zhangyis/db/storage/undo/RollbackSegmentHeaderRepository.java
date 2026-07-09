package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaKind;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * undo 表空间 page3 上 rollback segment header 的仓储（设计 §6.3，0.3）。经 MTR 持页 latch 读写 slot 目录
 * （slot -> insert-undo segment 首页），所有写都进 MTR redo（page3 物理 crash-safe）。形态仿
 * {@code SpaceHeaderRepository}：只管理页内布局读写，不分配页、不决定 slot 生命周期。
 *
 * <p>本片只持久 slot array；§6.3 的 history list base / cached·free segment list / lastTransactionNo 留后续片。
 */
public final class RollbackSegmentHeaderRepository {

    /** 受控页来源；只经 MTR 取 page3 的 PageGuard。 */
    private final BufferPool pool;
    /** 实例页大小；用于校验 slot array 不越过页尾。 */
    private final PageSize pageSize;

    public RollbackSegmentHeaderRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("rseg header repository pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    /** undo 表空间内 rseg header 固定页。 */
    public static PageId headerPage(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        return PageId.of(spaceId, PageNo.of(RollbackSegmentHeaderLayout.RSEG_HEADER_PAGE_NO));
    }

    /**
     * 格式化空 rseg header 页（page3）：写 FSP_HDR 之外的 RSEG_HEADER 信封 + magic/format/rsegId/slotCapacity +
     * 全槽置 {@code FIL_NULL}（空）。slotCapacity 必须为正且 slot array 不越过页尾 trailer。
     *
     * <p>经 {@code newPage} 清零并产 PAGE_INIT redo，再经写字段产 PAGE_BYTES redo——page3 物理 crash-safe。
     * undo 表空间创建与 truncate rebuild 都在同一 MTR 内调用本方法。
     */
    public void format(MiniTransaction mtr, SpaceId spaceId, RollbackSegmentId rsegId, int slotCapacity) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (rsegId == null) {
            throw new DatabaseValidationException("rollback segment id must not be null");
        }
        validateCapacity(slotCapacity);
        PageId pageId = headerPage(spaceId);
        PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.RSEG_HEADER);
        PageEnvelope.writeHeader(g, new FilePageHeader(spaceId, RollbackSegmentHeaderLayout.RSEG_HEADER_PAGE_NO,
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.RSEG_HEADER));
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.MAGIC,
                RollbackSegmentHeaderLayout.MAGIC_VALUE, "format rseg header magic");
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.FORMAT,
                RollbackSegmentHeaderLayout.FORMAT_VERSION, "format rseg header version");
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.RSEG_ID,
                rsegId.value(), "format rseg header id");
        UndoRedoDeltas.writeInt(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_HEADER_FIELD,
                rsegId.value(), 0, RollbackSegmentHeaderLayout.SLOT_CAPACITY,
                slotCapacity, "format rseg slot capacity");
        for (int i = 0; i < slotCapacity; i++) {
            UndoRedoDeltas.writeLong(mtr, g, pageId, UndoMetadataDeltaKind.RSEG_SLOT,
                    rsegId.value(), i, RollbackSegmentHeaderLayout.slotOffset(i),
                    FilePageHeader.FIL_NULL, "format empty rseg slot");
        }
    }

    /**
     * 写一个 slot：{@code firstPage != null} 登记其页号；{@code null} 清空（{@code FIL_NULL}）。X latch 下写，
     * 产 redo。先校验页头与 slot 越界，再写——避免把错误页号写进损坏的 rseg header。
     *
     * @param firstPage 该 slot 的 insert-undo segment 首页（须在同一 undo space）；{@code null} 表释放该 slot。
     */
    public void writeSlot(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot, PageId firstPage) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (slot == null) {
            throw new DatabaseValidationException("undo slot must not be null");
        }
        if (firstPage != null && !firstPage.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("rseg slot first page must be in the undo space: " + firstPage);
        }
        PageGuard g = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        int capacity = validateHeaderAndReadCapacity(g);
        int idx = slot.value();
        if (idx < 0 || idx >= capacity) {
            throw new UndoLogFormatException("rseg slot out of range: " + idx + " capacity=" + capacity);
        }
        long pageNo = firstPage == null ? FilePageHeader.FIL_NULL : firstPage.pageNo().value();
        long rsegId = g.readInt(RollbackSegmentHeaderLayout.RSEG_ID) & 0xFFFFFFFFL;
        UndoRedoDeltas.writeLong(mtr, g, g.pageId(), UndoMetadataDeltaKind.RSEG_SLOT,
                rsegId, idx, RollbackSegmentHeaderLayout.slotOffset(idx), pageNo, "write rseg slot first page");
    }

    /**
     * 读 rseg header 快照并校验：page type 必须 RSEG_HEADER、magic/format 正确、rsegId 与 slotCapacity 与
     * 期望（配置）一致——任一不符抛 {@link UndoLogFormatException}。占用槽收集为 {@code slot -> 首页}。
     *
     * @param expectedRsegId  期望 rseg id（配置）。
     * @param expectedCapacity 期望 slot 容量（配置）。
     */
    public RollbackSegmentHeaderSnapshot read(MiniTransaction mtr, SpaceId spaceId,
                                              RollbackSegmentId expectedRsegId, int expectedCapacity) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (expectedRsegId == null) {
            throw new DatabaseValidationException("expected rollback segment id must not be null");
        }
        PageGuard g = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.SHARED);
        int capacity = validateHeaderAndReadCapacity(g);
        if (capacity != expectedCapacity) {
            throw new UndoLogFormatException("rseg slotCapacity mismatch: disk=" + capacity
                    + " expected=" + expectedCapacity);
        }
        int rsegId = g.readInt(RollbackSegmentHeaderLayout.RSEG_ID);
        if (rsegId != expectedRsegId.value()) {
            throw new UndoLogFormatException("rseg id mismatch: disk=" + rsegId
                    + " expected=" + expectedRsegId.value());
        }
        Map<UndoSlotId, PageId> occupied = new LinkedHashMap<>();
        for (int i = 0; i < capacity; i++) {
            long pageNo = g.readLong(RollbackSegmentHeaderLayout.slotOffset(i));
            if (pageNo != FilePageHeader.FIL_NULL) {
                occupied.put(UndoSlotId.of(i), PageId.of(spaceId, PageNo.of(pageNo)));
            }
        }
        return new RollbackSegmentHeaderSnapshot(expectedRsegId, capacity, occupied);
    }

    /** 校验 page type/magic/format 并返回页上 slotCapacity。 */
    private int validateHeaderAndReadCapacity(PageGuard g) {
        FilePageHeader header = PageEnvelope.readHeader(g);
        if (header.pageType() != PageType.RSEG_HEADER) {
            throw new UndoLogFormatException("rseg header page type mismatch: " + header.pageType());
        }
        int magic = g.readInt(RollbackSegmentHeaderLayout.MAGIC);
        if (magic != RollbackSegmentHeaderLayout.MAGIC_VALUE) {
            throw new UndoLogFormatException("rseg header magic mismatch: " + Integer.toHexString(magic));
        }
        int format = g.readInt(RollbackSegmentHeaderLayout.FORMAT);
        if (format != RollbackSegmentHeaderLayout.FORMAT_VERSION) {
            throw new UndoLogFormatException("rseg header format mismatch: " + format);
        }
        int capacity = g.readInt(RollbackSegmentHeaderLayout.SLOT_CAPACITY);
        if (capacity <= 0) {
            throw new UndoLogFormatException("rseg header slotCapacity must be positive: " + capacity);
        }
        return capacity;
    }

    /** slotCapacity 必须为正，且 slot array 不越过页尾 trailer。 */
    private void validateCapacity(int slotCapacity) {
        if (slotCapacity <= 0) {
            throw new DatabaseValidationException("rseg slot capacity must be positive: " + slotCapacity);
        }
        int end = RollbackSegmentHeaderLayout.slotArrayEnd(slotCapacity);
        int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
        if (end > limit) {
            throw new DatabaseValidationException("rseg slot capacity " + slotCapacity
                    + " overflows page: needs " + end + " > " + limit);
        }
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireSpace(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
