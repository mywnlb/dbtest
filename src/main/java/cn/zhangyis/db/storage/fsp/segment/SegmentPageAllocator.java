package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationDirection;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationRequest;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;

/**
 * Segment 页分配编排（设计 §7.2）。fragment 已用 &lt;32 → fragment 路径；否则 segment-extent 路径
 * （无可用 extent 则按 {@link ExtentAllocationPolicy} 一次取 1..4 个 extent 再试）。
 * <b>纯分配</b>：只在当前 currentSize 内分配，无空间返回 {@link Optional#empty()}，不扩文件、不抛 NoFreeSpace
 * （autoextend 在 DiskSpaceManager facade）。
 *
 * <p>锁序：allocatePage 开头预闩 page0 X→page2 X（§18），后续 hasFreeFragmentSlot/Flst.length（S 降级）与
 * 2b 原语（reentrant X）不逆序、不触发同页 S→X。本片 no-redo。
 */
public final class SegmentPageAllocator {

    private final BufferPool pool;
    private final PageSize pageSize;
    private final SpaceHeaderRepository headerRepo;
    private final SegmentInodeRepository inodeRepo;
    private final Flst flst;
    private final SegmentSpaceService segSpace;
    private final ExtentAllocationPolicy policy;

    public SegmentPageAllocator(BufferPool pool, PageSize pageSize, SpaceHeaderRepository headerRepo,
                                SegmentInodeRepository inodeRepo, Flst flst,
                                SegmentSpaceService segSpace, ExtentAllocationPolicy policy) {
        if (pool == null || pageSize == null || headerRepo == null || inodeRepo == null
                || flst == null || segSpace == null || policy == null) {
            throw new DatabaseValidationException("SegmentPageAllocator dependencies must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        this.inodeRepo = inodeRepo;
        this.flst = flst;
        this.segSpace = segSpace;
        this.policy = policy;
    }

    /**
     * 为 segment 分配一个页，仅当前 currentSize 内。fragment 槽未满走 fragment 路径，否则 extent 路径
     * （必要时按 policy 取 1..4 extent 再试）。无空间返回 empty（facade 负责 autoextend）。
     */
    public Optional<PageId> allocatePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        return allocatePage(mtr, spaceId, inodeSlot,
                ExtentAllocationDirection.NO_DIRECTION, Optional.empty(), 1L);
    }

    /**
     * 为 segment 分配一个页，并把上层提供的方向 hint 纳入 extent 批量获取策略。该入口只在 fragment 槽耗尽、
     * 且现有 segment extent 无空闲页时才使用 direction；fragment 路径和已有 extent 页分配保持原有顺序。
     *
     * @param mtr 当前 MTR。
     * @param spaceId 目标表空间。
     * @param inodeSlot segment inode 槽。
     * @param direction extent 选择方向。
     * @param hintPageNo 邻近页号。
     * @param pagesNeeded 本次上层操作预计最多需要的新页数，必须为正。
     * @return 已分配页，或当前 currentSize 内无空间。
     */
    public Optional<PageId> allocatePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot,
                                         ExtentAllocationDirection direction, Optional<PageNo> hintPageNo,
                                         long pagesNeeded) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        if (direction == null || hintPageNo == null) {
            throw new DatabaseValidationException("extent allocation direction/hint must not be null");
        }
        if (direction != ExtentAllocationDirection.NO_DIRECTION && hintPageNo.isEmpty()) {
            throw new DatabaseValidationException("directional extent allocation requires a hint page");
        }
        if (pagesNeeded <= 0L) {
            throw new DatabaseValidationException("pagesNeeded must be positive: " + pagesNeeded);
        }
        latchSpaceThenInode(mtr, spaceId);
        if (inodeRepo.hasFreeFragmentSlot(mtr, spaceId, inodeSlot)) {
            return segSpace.allocateFragmentPage(mtr, spaceId, inodeSlot)
                    .map(pageNo -> PageId.of(spaceId, pageNo));
        }
        Optional<PageNo> fromExtent = segSpace.allocatePageFromSegmentExtents(mtr, spaceId, inodeSlot);
        if (fromExtent.isPresent()) {
            return Optional.of(PageId.of(spaceId, fromExtent.get()));
        }
        SegmentInode inode = inodeRepo.read(mtr, spaceId, inodeSlot);
        long owned = ownedExtentCount(mtr, spaceId, inodeSlot);
        SpaceHeaderSnapshot header = headerRepo.readForUpdate(mtr, spaceId);
        ExtentAllocationRequest request = new ExtentAllocationRequest(
                inode.purpose(),
                hintPageNo,
                direction,
                pagesNeeded,
                owned,
                inode.usedPageCount(),
                header.currentSizeInPages(),
                1.0d,
                pageSize);
        int toAcquire = policy.extentsToAcquire(request);
        boolean assignedAny = false;
        for (int i = 0; i < toAcquire; i++) {
            if (segSpace.assignExtentToSegment(mtr, spaceId, inodeSlot, direction, hintPageNo).isPresent()) {
                assignedAny = true;
            } else {
                break;
            }
        }
        if (!assignedAny) {
            return Optional.empty();
        }
        return segSpace.allocatePageFromSegmentExtents(mtr, spaceId, inodeSlot)
                .map(pageNo -> PageId.of(spaceId, pageNo));
    }

    private long ownedExtentCount(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        return flst.length(mtr, spaceId, inodeRepo.freeExtentListBaseAddr(spaceId, inodeSlot))
                + flst.length(mtr, spaceId, inodeRepo.notFullExtentListBaseAddr(spaceId, inodeSlot))
                + flst.length(mtr, spaceId, inodeRepo.fullExtentListBaseAddr(spaceId, inodeSlot));
    }

    private void latchSpaceThenInode(MiniTransaction mtr, SpaceId spaceId) {
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
    }
}
