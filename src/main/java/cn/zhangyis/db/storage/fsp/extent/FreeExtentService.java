package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * 表空间全局空间生命周期（设计 §7、§8）：freeLimit 填充把原始文件页材料化为 FREE extent 入 FSP_FREE，
 * acquire/return FREE extent，从 FSP_FREE_FRAG 分配 fragment 页并维护 FREE↔FREE_FRAG↔FULL_FRAG 迁移。
 * 仅碰 page0；每个写 op 开头预闩 page0 X，使后续读取（getFirst/firstFreePageIndex）降级、避免同页 S→X。
 *
 * <p>简化点：只材料化整 extent（不处理 currentSize 非对齐尾页）；跳过系统 extent0；本片 no-redo，写页只标脏、
 * 不产 redo、不声明 crash-safe（设计 §15 推迟满足）。无空间以 {@link Optional#empty()} 表达，由 2c 决定 autoextend。
 */
public final class FreeExtentService {

    /** 受控页来源，用于跨方法的 page0 预闩。 */
    private final BufferPool pool;

    /** 实例级页大小；决定 pagesPerExtent 与 extent 首页号。 */
    private final PageSize pageSize;

    /** SpaceHeader 仓储：freeLimit/currentSize 读写、三个全局链 base 地址。 */
    private final SpaceHeaderRepository headerRepo;

    /** XDES 仓储：extent state/bitmap/节点地址。 */
    private final ExtentDescriptorRepository xdes;

    /** FLST 链表原语。 */
    private final Flst flst;

    public FreeExtentService(BufferPool pool, PageSize pageSize, SpaceHeaderRepository headerRepo,
                             ExtentDescriptorRepository xdes, Flst flst) {
        if (pool == null || pageSize == null || headerRepo == null || xdes == null || flst == null) {
            throw new DatabaseValidationException("FreeExtentService dependencies must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        this.xdes = xdes;
        this.flst = flst;
    }

    private void latchPage0(MiniTransaction mtr, SpaceId spaceId) {
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
    }

    /**
     * 材料化下一个非系统、整体在 currentSize 之内的 extent 到 FSP_FREE，并推进 freeLimit；越界返回 empty。
     * 数据流：读 header(freeLimit/currentSize/pageSize) → 跳过 extent0 → initFree + addLast(FSP_FREE) → 推进 freeLimit。
     */
    public Optional<ExtentId> fillFreeListStep(MiniTransaction mtr, SpaceId spaceId) {
        requireArgs(mtr, spaceId);
        latchPage0(mtr, spaceId);
        long pe = pageSize.pagesPerExtent();
        SpaceHeaderSnapshot h = headerRepo.read(mtr, spaceId);
        long currentSize = h.currentSizeInPages().value();
        long freeLimit = h.freeLimitPageNo().value();
        while (true) {
            long extentNo = freeLimit / pe;
            if ((extentNo + 1) * pe > currentSize) {
                return Optional.empty();
            }
            freeLimit += pe;
            headerRepo.setFreeLimitPageNo(mtr, spaceId, PageNo.of(freeLimit));
            if (extentNo == 0) {
                continue; // 系统 extent0 不入 free-list
            }
            ExtentId ext = ExtentId.of(spaceId, extentNo);
            xdes.initFree(mtr, ext);
            flst.addLast(mtr, spaceId, headerRepo.freeExtentListBaseAddr(spaceId), xdes.listNodeAddr(ext));
            return Optional.of(ext);
        }
    }

    /** 取一个可用 FREE extent：弹 FSP_FREE 头；空则先 fill 再弹；真满返回 empty。 */
    public Optional<ExtentId> acquireFreeExtent(MiniTransaction mtr, SpaceId spaceId) {
        requireArgs(mtr, spaceId);
        latchPage0(mtr, spaceId);
        FileAddress freeBase = headerRepo.freeExtentListBaseAddr(spaceId);
        FileAddress head = flst.getFirst(mtr, spaceId, freeBase);
        if (!head.isNull()) {
            ExtentId ext = xdes.extentIdOfNode(spaceId, head);
            flst.remove(mtr, spaceId, freeBase, head);
            return Optional.of(ext);
        }
        Optional<ExtentId> filled = fillFreeListStep(mtr, spaceId);
        if (filled.isEmpty()) {
            return Optional.empty();
        }
        FileAddress head2 = flst.getFirst(mtr, spaceId, freeBase);
        flst.remove(mtr, spaceId, freeBase, head2);
        return filled;
    }

    /** 回收一个 extent 为 FREE（initFree：state FREE/owner 0/bitmap 清/prev-next NULL）并入 FSP_FREE。调用方须先把它移出原链。 */
    public void returnFreeExtent(MiniTransaction mtr, SpaceId spaceId, ExtentId extentId) {
        requireArgs(mtr, spaceId);
        if (extentId == null) {
            throw new DatabaseValidationException("extent id must not be null");
        }
        latchPage0(mtr, spaceId);
        xdes.initFree(mtr, extentId); // extent0 会被 initFree 拒绝
        flst.addLast(mtr, spaceId, headerRepo.freeExtentListBaseAddr(spaceId), xdes.listNodeAddr(extentId));
    }

    /**
     * 从 FSP_FREE_FRAG 分配一个 fragment 页。无 FREE_FRAG 则从 FSP_FREE acquire 一个并置 FREE_FRAG。
     * 取首个空闲页置位；若 extent 因此变满，迁 FSP_FREE_FRAG→FSP_FULL_FRAG。无空间返回 empty。
     */
    public Optional<PageId> allocateFragmentPage(MiniTransaction mtr, SpaceId spaceId) {
        requireArgs(mtr, spaceId);
        latchPage0(mtr, spaceId);
        FileAddress ffBase = headerRepo.freeFragExtentListBaseAddr(spaceId);
        FileAddress head = flst.getFirst(mtr, spaceId, ffBase);
        ExtentId frag;
        if (!head.isNull()) {
            frag = xdes.extentIdOfNode(spaceId, head);
        } else {
            Optional<ExtentId> acq = acquireFreeExtent(mtr, spaceId);
            if (acq.isEmpty()) {
                return Optional.empty();
            }
            frag = acq.get();
            xdes.writeState(mtr, frag, ExtentState.FREE_FRAG);
            flst.addLast(mtr, spaceId, ffBase, xdes.listNodeAddr(frag));
        }
        OptionalInt idxOpt = xdes.firstFreePageIndex(mtr, frag);
        if (idxOpt.isEmpty()) {
            throw new FspMetadataException("FREE_FRAG extent unexpectedly full: " + frag.extentNo());
        }
        int idx = idxOpt.getAsInt();
        xdes.setPageAllocated(mtr, frag, idx, true);
        long pageNo = frag.firstPageNo(pageSize).value() + idx;
        if (xdes.isFull(mtr, frag)) {
            flst.remove(mtr, spaceId, ffBase, xdes.listNodeAddr(frag));
            xdes.writeState(mtr, frag, ExtentState.FULL_FRAG);
            flst.addLast(mtr, spaceId, headerRepo.fullFragExtentListBaseAddr(spaceId), xdes.listNodeAddr(frag));
        }
        return Optional.of(PageId.of(spaceId, PageNo.of(pageNo)));
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
