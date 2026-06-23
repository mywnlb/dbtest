package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptor;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.extent.ExtentState;
import cn.zhangyis.db.storage.fsp.extent.FreeExtentService;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * segment 侧空间分配/释放原语（设计 §7）：fragment 页分配（含 inode 记录）、给 segment 分配 extent、
 * 从 segment extent 分配页、释放页回收。维护 XDES state/owner/bitmap、segment FLST 链、inode 计数/fragment 槽一致。
 *
 * <p>锁序：每个公开 op 先 {@link #latchSpaceThenInode}（page0 X→page2 X，§18 顺序），后续 repo/Flst 读写在已持 X 上可重入，
 * 既不逆序也不触发 MTR 同页 S→X 拒绝。无空间以 {@link Optional#empty()} 表达；损坏/非法抛领域异常。本片 no-redo。
 */
public final class SegmentSpaceService {

    /** 受控页来源，用于跨页预闩。 */
    private final BufferPool pool;

    /** 实例级页大小；决定 extent 首页号。 */
    private final PageSize pageSize;

    /** SpaceHeader 仓储：全局链 base（释放回收时用）。 */
    private final SpaceHeaderRepository headerRepo;

    /** SegmentInode 仓储：segment 链 base、fragment 槽、计数。 */
    private final SegmentInodeRepository inodeRepo;

    /** XDES 仓储：extent state/owner/bitmap/节点地址。 */
    private final ExtentDescriptorRepository xdes;

    /** FLST 链表原语。 */
    private final Flst flst;

    /** 全局 free 服务：acquire/return/fragment 页。 */
    private final FreeExtentService freeExtents;

    public SegmentSpaceService(BufferPool pool, PageSize pageSize, SpaceHeaderRepository headerRepo,
                               SegmentInodeRepository inodeRepo, ExtentDescriptorRepository xdes,
                               Flst flst, FreeExtentService freeExtents) {
        if (pool == null || pageSize == null || headerRepo == null || inodeRepo == null
                || xdes == null || flst == null || freeExtents == null) {
            throw new DatabaseValidationException("SegmentSpaceService dependencies must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        this.inodeRepo = inodeRepo;
        this.xdes = xdes;
        this.flst = flst;
        this.freeExtents = freeExtents;
    }

    /** §18 锁序：先 page0 X 再 page2 X，建立顺序并使后续读取降级、避免同页 S→X。 */
    private void latchSpaceThenInode(MiniTransaction mtr, SpaceId spaceId) {
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
    }

    /**
     * 为 segment 分配一个 fragment 页：从全局 FREE_FRAG 取页，记入 inode 首个空 fragment 槽，usedPageCount+1。
     * 32 槽已满 → FspMetadataException（边界由 2c 用 &lt;32 决策规避）；无空间 → empty（未改 inode）。
     */
    public Optional<PageNo> allocateFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireArgs(mtr, spaceId);
        latchSpaceThenInode(mtr, spaceId);
        int slotIdx = inodeRepo.requireFreeFragmentSlot(mtr, spaceId, inodeSlot);
        Optional<PageId> pageOpt = freeExtents.allocateFragmentPage(mtr, spaceId);
        if (pageOpt.isEmpty()) {
            return Optional.empty();
        }
        PageNo pageNo = pageOpt.get().pageNo();
        inodeRepo.setFragmentPage(mtr, spaceId, inodeSlot, slotIdx, Optional.of(pageNo));
        bumpUsed(mtr, spaceId, inodeSlot, 1);
        return Optional.of(pageNo);
    }

    /** 给 segment 分配一个完整 extent：acquire FREE extent → 置 FSEG/owner=segId → 入该段 SEG_FREE 链。无空间 → empty。 */
    public Optional<ExtentId> assignExtentToSegment(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireArgs(mtr, spaceId);
        latchSpaceThenInode(mtr, spaceId);
        SegmentId segId = inodeRepo.read(mtr, spaceId, inodeSlot).segmentId();
        Optional<ExtentId> acq = freeExtents.acquireFreeExtent(mtr, spaceId);
        if (acq.isEmpty()) {
            return Optional.empty();
        }
        ExtentId ext = acq.get();
        xdes.writeState(mtr, ext, ExtentState.FSEG);
        xdes.writeOwner(mtr, ext, Optional.of(segId));
        flst.addLast(mtr, spaceId, inodeRepo.freeExtentListBaseAddr(spaceId, inodeSlot), xdes.listNodeAddr(ext));
        return Optional.of(ext);
    }

    /**
     * 从 segment 自有 extent 分配一个页：优先 SEG_NOT_FULL 头，否则 SEG_FREE 头；取首个空闲页置位。
     * 迁移：SEG_FREE→SEG_NOT_FULL（或满→SEG_FULL）、SEG_NOT_FULL→SEG_FULL（变满时）。无可用 extent → empty（2c 先 assign）。
     */
    public Optional<PageNo> allocatePageFromSegmentExtents(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireArgs(mtr, spaceId);
        latchSpaceThenInode(mtr, spaceId);
        FileAddress notFull = inodeRepo.notFullExtentListBaseAddr(spaceId, inodeSlot);
        FileAddress segFree = inodeRepo.freeExtentListBaseAddr(spaceId, inodeSlot);
        FileAddress segFull = inodeRepo.fullExtentListBaseAddr(spaceId, inodeSlot);
        FileAddress head = flst.getFirst(mtr, spaceId, notFull);
        boolean fromFree = false;
        if (head.isNull()) {
            head = flst.getFirst(mtr, spaceId, segFree);
            if (head.isNull()) {
                return Optional.empty();
            }
            fromFree = true;
        }
        ExtentId ext = xdes.extentIdOfNode(spaceId, head);
        OptionalInt idxOpt = xdes.firstFreePageIndex(mtr, ext);
        if (idxOpt.isEmpty()) {
            throw new FspMetadataException("segment extent on non-full list is full: " + ext.extentNo());
        }
        int idx = idxOpt.getAsInt();
        xdes.setPageAllocated(mtr, ext, idx, true);
        long pageNo = ext.firstPageNo(pageSize).value() + idx;
        FileAddress node = xdes.listNodeAddr(ext);
        if (fromFree) {
            flst.remove(mtr, spaceId, segFree, node);
            if (xdes.isFull(mtr, ext)) {
                flst.addLast(mtr, spaceId, segFull, node);
            } else {
                flst.addLast(mtr, spaceId, notFull, node);
            }
        } else if (xdes.isFull(mtr, ext)) {
            flst.remove(mtr, spaceId, notFull, node);
            flst.addLast(mtr, spaceId, segFull, node);
        }
        bumpUsed(mtr, spaceId, inodeSlot, 1);
        return Optional.of(PageNo.of(pageNo));
    }

    /**
     * 释放一个页并回收（设计 §7.4）。按 extent 状态分两路：
     * fragment extent（FREE_FRAG/FULL_FRAG）：清 bitmap + 清对应 inode fragment 槽 + 计数-1；原 FULL_FRAG→FREE_FRAG；全空→FSP_FREE。
     * segment extent（FSEG）：校 owner + 清 bitmap + 计数-1；原满→SEG_NOT_FULL；全空→摘段链 + 置 FREE/清 owner + FSP_FREE。
     * 系统 extent0 或 FREE/未分配区页 → FspMetadataException。
     */
    public void freePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, PageId pageId) {
        requireArgs(mtr, spaceId);
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        latchSpaceThenInode(mtr, spaceId);
        ExtentId ext = ExtentId.from(pageId, pageSize);
        if (ext.extentNo() == 0) {
            throw new FspMetadataException("cannot free a system-extent page: " + pageId.pageNo().value());
        }
        int idxInExtent = (int) (pageId.pageNo().value() - ext.firstPageNo(pageSize).value());
        ExtentDescriptor desc = xdes.read(mtr, ext);
        ExtentState state = desc.state();
        FileAddress node = xdes.listNodeAddr(ext);
        switch (state) {
            case FREE_FRAG, FULL_FRAG -> {
                xdes.setPageAllocated(mtr, ext, idxInExtent, false);
                clearFragmentSlot(mtr, spaceId, inodeSlot, pageId.pageNo());
                bumpUsed(mtr, spaceId, inodeSlot, -1);
                if (state == ExtentState.FULL_FRAG) {
                    flst.remove(mtr, spaceId, headerRepo.fullFragExtentListBaseAddr(spaceId), node);
                    xdes.writeState(mtr, ext, ExtentState.FREE_FRAG);
                    flst.addLast(mtr, spaceId, headerRepo.freeFragExtentListBaseAddr(spaceId), node);
                }
                if (xdes.isEmpty(mtr, ext)) {
                    flst.remove(mtr, spaceId, headerRepo.freeFragExtentListBaseAddr(spaceId), node);
                    freeExtents.returnFreeExtent(mtr, spaceId, ext);
                }
            }
            case FSEG -> {
                SegmentId segId = inodeRepo.read(mtr, spaceId, inodeSlot).segmentId();
                if (desc.ownerSegment().isEmpty() || !desc.ownerSegment().get().equals(segId)) {
                    throw new FspMetadataException("extent owner mismatch on free: extent " + ext.extentNo());
                }
                boolean wasFull = xdes.isFull(mtr, ext);
                xdes.setPageAllocated(mtr, ext, idxInExtent, false);
                bumpUsed(mtr, spaceId, inodeSlot, -1);
                FileAddress notFull = inodeRepo.notFullExtentListBaseAddr(spaceId, inodeSlot);
                FileAddress segFull = inodeRepo.fullExtentListBaseAddr(spaceId, inodeSlot);
                if (wasFull) {
                    flst.remove(mtr, spaceId, segFull, node);
                    flst.addLast(mtr, spaceId, notFull, node);
                }
                if (xdes.isEmpty(mtr, ext)) {
                    flst.remove(mtr, spaceId, notFull, node);
                    xdes.writeState(mtr, ext, ExtentState.FREE);
                    xdes.writeOwner(mtr, ext, Optional.empty());
                    flst.addLast(mtr, spaceId, headerRepo.freeExtentListBaseAddr(spaceId), node);
                }
            }
            default -> throw new FspMetadataException(
                    "cannot free page in extent state " + state + ": page " + pageId.pageNo().value());
        }
    }

    /** 扫 32 个 fragment 槽，清掉值等于 pageNo 的槽；找不到说明该页不是本段 fragment 页（损坏）。 */
    private void clearFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, PageNo pageNo) {
        for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
            Optional<PageNo> cur = inodeRepo.getFragmentPage(mtr, spaceId, inodeSlot, f);
            if (cur.isPresent() && cur.get().equals(pageNo)) {
                inodeRepo.setFragmentPage(mtr, spaceId, inodeSlot, f, Optional.empty());
                return;
            }
        }
        throw new FspMetadataException("fragment page not recorded in segment: " + pageNo.value());
    }

    /** 读现值并 +delta 写回 usedPageCount；delta<0 且现值<=0 视为计数损坏。 */
    private void bumpUsed(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, long delta) {
        long cur = inodeRepo.read(mtr, spaceId, inodeSlot).usedPageCount();
        if (delta < 0 && cur <= 0) {
            throw new FspMetadataException("usedPageCount underflow on inode slot " + inodeSlot);
        }
        inodeRepo.setUsedPageCount(mtr, spaceId, inodeSlot, cur + delta);
    }

    private static void requireArgs(MiniTransaction mtr, SpaceId spaceId) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }
}
