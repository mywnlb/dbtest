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
     * 以 compare-and-set 语义认领一个磁盘空 slot。只有当前值为 {@code FIL_NULL} 才写入 first page；重复 claim
     * 或内存/磁盘 owner 漂移必须在覆盖旧 owner 前 fail-closed。
     *
     * @param mtr       持有 page3 X latch 并收集 redo 的短 MTR。
     * @param spaceId   rseg page3 所属 undo 表空间。
     * @param slot      要认领的稳定 slot 下标。
     * @param firstPage 新 undo segment 首页，必须属于同一 undo tablespace。
     */
    public void claimSlot(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot, PageId firstPage) {
        SlotWrite write = prepareSlotWrite(mtr, spaceId, slot, firstPage, "claim");
        long current = write.guard().readLong(write.offset());
        if (current != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("claim of occupied rseg slot " + slot.value()
                    + ": current first page=" + current);
        }
        writeSlotPageNo(mtr, write, firstPage.pageNo().value(), "claim rseg slot first page");
    }

    /**
     * 在物理 undo segment 分配前以 page3 S latch 预检槽为空。方法只读取页格式、容量和目标 slot，且无论成功或
     * 异常都在返回前释放 latch，使后续 FSP page0/page2 分配不形成 page3 -&gt; FSP 的逆序等待。持久 claim 仍须
     * 在创建段后用 {@link #claimSlot} 再做一次 X-latch CAS；本方法只把正常单进程冲突前移到无物理副作用阶段。
     *
     * @param mtr     当前业务 MTR；调用方必须在触碰其他页前调用本方法。
     * @param spaceId page3 所属 undo 表空间。
     * @param slot    已由内存 claim lease 预留的稳定槽号。
     * @throws UndoSlotOwnershipConflictException 磁盘槽已有 owner 时抛出。
     */
    public void requireSlotFree(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (slot == null) {
            throw new DatabaseValidationException("rseg slot preflight slot must not be null");
        }
        PageId pageId = headerPage(spaceId);
        PageGuard guard = mtr.getPage(pool, pageId, PageLatchMode.SHARED);
        try {
            int capacity = validateHeaderAndReadCapacity(guard);
            int idx = slot.value();
            if (idx < 0 || idx >= capacity) {
                throw new UndoLogFormatException("rseg slot out of range: " + idx + " capacity=" + capacity);
            }
            long current = guard.readLong(RollbackSegmentHeaderLayout.slotOffset(idx));
            if (current != FilePageHeader.FIL_NULL) {
                throw new UndoSlotOwnershipConflictException("preflight of occupied rseg slot " + idx
                        + ": current first page=" + current);
            }
        } finally {
            mtr.releaseLatch(pageId, guard);
        }
    }

    /**
     * 以 compare-and-set 语义清空 slot。只有磁盘 owner 精确等于 {@code expectedFirstPage} 才写 {@code FIL_NULL}，
     * 防止 stale rollback/purge 清掉已经被后续事务复用的 slot。
     *
     * @param mtr               finalization MTR；FSP drop 已在同一 MTR 中先执行。
     * @param spaceId           rseg page3 所属 undo 表空间。
     * @param slot              要清理的稳定 slot 下标。
     * @param expectedFirstPage 调用方预期仍占有该 slot 的 undo segment 首页。
     */
    public void clearSlot(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot, PageId expectedFirstPage) {
        SlotWrite write = prepareSlotWrite(mtr, spaceId, slot, expectedFirstPage, "clear");
        long current = write.guard().readLong(write.offset());
        long expected = expectedFirstPage.pageNo().value();
        if (current != expected) {
            throw new UndoLogFormatException("clear of stale rseg slot " + slot.value()
                    + ": expected first page=" + expected + ", current=" + current);
        }
        writeSlotPageNo(mtr, write, FilePageHeader.FIL_NULL, "clear rseg slot first page");
    }

    /** 校验公共参数、页格式与 slot 边界，并返回仍持 X latch 的写定位。 */
    private SlotWrite prepareSlotWrite(MiniTransaction mtr, SpaceId spaceId, UndoSlotId slot,
                                       PageId firstPage, String operation) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (slot == null || firstPage == null) {
            throw new DatabaseValidationException("rseg slot " + operation + " slot/first page must not be null");
        }
        if (!firstPage.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("rseg slot first page must be in the undo space: " + firstPage);
        }
        PageGuard guard = mtr.getPage(pool, headerPage(spaceId), PageLatchMode.EXCLUSIVE);
        int capacity = validateHeaderAndReadCapacity(guard);
        int idx = slot.value();
        if (idx < 0 || idx >= capacity) {
            throw new UndoLogFormatException("rseg slot out of range: " + idx + " capacity=" + capacity);
        }
        long rsegId = guard.readInt(RollbackSegmentHeaderLayout.RSEG_ID) & 0xFFFFFFFFL;
        return new SlotWrite(guard, idx, RollbackSegmentHeaderLayout.slotOffset(idx), rsegId);
    }

    /** 写 slot pageNo after-image；调用方已经完成 expected-owner CAS。 */
    private void writeSlotPageNo(MiniTransaction mtr, SlotWrite write, long pageNo, String reason) {
        UndoRedoDeltas.writeLong(mtr, write.guard(), write.guard().pageId(), UndoMetadataDeltaKind.RSEG_SLOT,
                write.rsegId(), write.slotIndex(), write.offset(), pageNo, reason);
    }

    /** 一次已校验 slot 写的页定位；生命周期绑定到调用方 MTR memo。 */
    private record SlotWrite(PageGuard guard, int slotIndex, int offset, long rsegId) {
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
