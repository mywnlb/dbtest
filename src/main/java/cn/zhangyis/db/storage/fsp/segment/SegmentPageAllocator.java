package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.flst.Flst;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
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
    private final SegmentInodeRepository inodeRepo;
    private final Flst flst;
    private final SegmentSpaceService segSpace;
    private final ExtentAllocationPolicy policy;

    public SegmentPageAllocator(BufferPool pool, SegmentInodeRepository inodeRepo, Flst flst,
                                SegmentSpaceService segSpace, ExtentAllocationPolicy policy) {
        if (pool == null || inodeRepo == null || flst == null || segSpace == null || policy == null) {
            throw new DatabaseValidationException("SegmentPageAllocator dependencies must not be null");
        }
        this.pool = pool;
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
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
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
        long owned = ownedExtentCount(mtr, spaceId, inodeSlot);
        int toAcquire = policy.extentsToAcquire(owned);
        boolean assignedAny = false;
        for (int i = 0; i < toAcquire; i++) {
            if (segSpace.assignExtentToSegment(mtr, spaceId, inodeSlot).isPresent()) {
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
