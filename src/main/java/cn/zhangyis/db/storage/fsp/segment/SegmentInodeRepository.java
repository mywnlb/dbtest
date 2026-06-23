package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;

/**
 * SegmentInode（page 2）仓储（设计 §6.4）。逻辑 segment 归属：分配/释放 inode 槽、读写 purpose/计数/extent list base/fragment 槽。
 * 首版单个 inode 页 page 2。三个 extent list 头为 FLST base，由 {@link Flst} 维护——repo 负责 allocateSlot/read 整 base
 * 与暴露 base 地址访问器。allocateSlot / requireFreeFragmentSlot 为查找型、非幂等。
 *
 * <p>简化点：单 inode 页 page 2；本切片 no-redo，写页只标脏、不产 redo，不声明 crash-safe（§15 推迟满足）。
 */
public final class SegmentInodeRepository {

    /** 受控页来源；inode entries 在 page 2，经 MTR.getPage 拿 page 2 的 PageGuard。 */
    private final BufferPool pool;

    /** 实例级页大小；决定 page 2 可容纳的 inode 槽数（maxInodesInPage）。 */
    private final PageSize pageSize;

    public SegmentInodeRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    private static PageId inodePage(SpaceId spaceId) {
        return PageId.of(spaceId, PageNo.of(2));
    }

    /** 扫首个 used==0 槽，写入 inode（used=1/segmentId/purpose、counts=0、三 list=EMPTY base、32 fragment 槽=0），返回槽下标。无空槽抛异常。 */
    public int allocateSlot(MiniTransaction mtr, SpaceId spaceId, SegmentId segmentId, SegmentPurpose purpose) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (segmentId == null || purpose == null) {
            throw new DatabaseValidationException("segmentId/purpose must not be null");
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("segment id 0 is reserved as empty owner sentinel");
        }
        long max = SegmentInodeLayout.maxInodesInPage(pageSize);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        for (int slot = 0; slot < max; slot++) {
            int base = SegmentInodeLayout.slotOffset(slot);
            if (g.readInt(base + SegmentInodeLayout.USED) == 0) {
                g.writeInt(base + SegmentInodeLayout.USED, 1);
                g.writeLong(base + SegmentInodeLayout.SEGMENT_ID, segmentId.value());
                g.writeInt(base + SegmentInodeLayout.PURPOSE, purpose.ordinal());
                g.writeLong(base + SegmentInodeLayout.USED_PAGE_COUNT, 0L);
                g.writeLong(base + SegmentInodeLayout.RESERVED_PAGE_COUNT, 0L);
                FlstBase.EMPTY.writeTo(g, base + SegmentInodeLayout.FREE_EXTENT_LIST_BASE);
                FlstBase.EMPTY.writeTo(g, base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE);
                FlstBase.EMPTY.writeTo(g, base + SegmentInodeLayout.FULL_EXTENT_LIST_BASE);
                for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
                    g.writeLong(SegmentInodeLayout.fragmentSlotOffset(slot, f), 0L);
                }
                return slot;
            }
        }
        throw new FspMetadataException("no free inode slot on page 2 (max " + max + ")");
    }

    public SegmentInode read(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        if (g.readInt(base + SegmentInodeLayout.USED) == 0) {
            throw new FspMetadataException("inode slot is free: " + inodeSlot);
        }
        return new SegmentInode(
                inodeSlot,
                SegmentId.of(g.readLong(base + SegmentInodeLayout.SEGMENT_ID)),
                decodePurpose(g.readInt(base + SegmentInodeLayout.PURPOSE)),
                g.readLong(base + SegmentInodeLayout.USED_PAGE_COUNT),
                g.readLong(base + SegmentInodeLayout.RESERVED_PAGE_COUNT),
                FlstBase.readFrom(g, base + SegmentInodeLayout.FREE_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, base + SegmentInodeLayout.FULL_EXTENT_LIST_BASE));
    }

    public void freeSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeBytes(base, new byte[SegmentInodeLayout.ENTRY_SIZE]);
    }

    public void setUsedPageCount(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, long value) {
        writeLongField(mtr, spaceId, inodeSlot, SegmentInodeLayout.USED_PAGE_COUNT, value);
    }

    public void setReservedPageCount(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, long value) {
        writeLongField(mtr, spaceId, inodeSlot, SegmentInodeLayout.RESERVED_PAGE_COUNT, value);
    }

    /** SEG_FREE 链 base 地址（page2 内 inode 槽偏移），供 Flst/2b 维护链。 */
    public FileAddress freeExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.FREE_EXTENT_LIST_BASE);
    }

    /** SEG_NOT_FULL 链 base 地址。 */
    public FileAddress notFullExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE);
    }

    /** SEG_FULL 链 base 地址。 */
    public FileAddress fullExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.FULL_EXTENT_LIST_BASE);
    }

    /** fragment 槽：值 0 → 空。 */
    public Optional<PageNo> getFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fragIdx) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        requireFrag(fragIdx);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        long raw = g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, fragIdx));
        return raw == 0 ? Optional.empty() : Optional.of(PageNo.of(raw));
    }

    public void setFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fragIdx, Optional<PageNo> pageNo) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        requireFrag(fragIdx);
        if (pageNo == null) {
            throw new DatabaseValidationException("fragment pageNo optional must not be null");
        }
        long raw = pageNo.map(PageNo::value).orElse(0L);
        if (raw == 0 && pageNo.isPresent()) {
            throw new DatabaseValidationException("page 0 is reserved as empty fragment sentinel");
        }
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, fragIdx), raw);
    }

    /** 返回首个空（值为 0）fragment 槽下标；满则抛 FspMetadataException。 */
    public int requireFreeFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
            if (g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, f)) == 0) {
                return f;
            }
        }
        throw new FspMetadataException("no free fragment slot in inode slot: " + inodeSlot);
    }

    /** 是否存在空 fragment 槽（值为 0），即该 segment 已用 fragment 页 &lt; 32。供分配层做 fragment vs extent 决策（S）。 */
    public boolean hasFreeFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
            if (g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, f)) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 扫描单 inode 页是否仍有已分配槽。undo truncate 只在所有 segment 已由 purge/dropSegment 回收后允许；
     * 该检查在 lifecycle X lease 内以 page2 S latch 执行，结果不会与新的 segment 分配交叉。
     *
     * @param mtr 当前维护 MTR。
     * @param spaceId undo 表空间。
     * @return 任一 USED!=0 返回 true。
     */
    public boolean hasAllocatedSlots(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        long max = SegmentInodeLayout.maxInodesInPage(pageSize);
        for (int slot = 0; slot < max; slot++) {
            if (g.readInt(SegmentInodeLayout.slotOffset(slot) + SegmentInodeLayout.USED) != 0) {
                return true;
            }
        }
        return false;
    }

    private void writeLongField(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fieldOffset, long value) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeLong(base + fieldOffset, value);
    }

    private static SegmentPurpose decodePurpose(int ordinal) {
        SegmentPurpose[] values = SegmentPurpose.values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new FspMetadataException("invalid segment purpose ordinal on disk: " + ordinal);
        }
        return values[ordinal];
    }

    private int requireSlot(SpaceId spaceId, int inodeSlot) {
        if (inodeSlot < 0 || inodeSlot >= SegmentInodeLayout.maxInodesInPage(pageSize)) {
            throw new DatabaseValidationException("inode slot out of range: " + inodeSlot);
        }
        return SegmentInodeLayout.slotOffset(inodeSlot);
    }

    private static void requireFrag(int fragIdx) {
        if (fragIdx < 0 || fragIdx >= SegmentInodeLayout.FRAGMENT_SLOT_COUNT) {
            throw new DatabaseValidationException("fragment index out of range: " + fragIdx);
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
