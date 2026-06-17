package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.fsp.DefaultExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.FileAddress;
import cn.zhangyis.db.storage.fsp.Flst;
import cn.zhangyis.db.storage.fsp.FlstBase;
import cn.zhangyis.db.storage.fsp.FreeExtentService;
import cn.zhangyis.db.storage.fsp.NoFreeSpaceException;
import cn.zhangyis.db.storage.fsp.SegmentInodeRepository;
import cn.zhangyis.db.storage.fsp.SegmentPageAllocator;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.fsp.SegmentSpaceService;
import cn.zhangyis.db.storage.fsp.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.nio.file.Path;
import java.util.Optional;

/**
 * 磁盘空间管理门面（设计 §13.1、§14 Facade）。聚合 fsp 仓储/服务/分配器 + fil PageStore，对上层提供
 * tablespace/segment 生命周期、页分配/释放（含 autoextend 重试）、用量查询。返回领域对象与句柄，不暴露内部页。
 *
 * <p>autoextend：allocator 仅在当前 currentSize 内分配，返回 empty 时本门面 {@code extend} 文件 + 更新 currentSize 后
 * 重试一次，仍无则 {@link NoFreeSpaceException}。本片 no-redo，不声明 crash-safe（§15 推迟满足）。
 */
public final class DiskSpaceManager {

    /** 新建表空间写入的 server 版本号（诊断用，固定值）。 */
    private static final int SERVER_VERSION = 80046;

    private final BufferPool pool;
    private final PageStore pageStore;
    private final PageSize pageSize;
    private final SpaceHeaderRepository headerRepo;
    private final ExtentDescriptorRepository xdes;
    private final SegmentInodeRepository inodeRepo;
    private final Flst flst;
    private final FreeExtentService freeExtents;
    private final SegmentSpaceService segSpace;
    private final SegmentPageAllocator allocator;

    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize) {
        if (pool == null || pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("DiskSpaceManager dependencies must not be null");
        }
        this.pool = pool;
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.headerRepo = new SpaceHeaderRepository(pool);
        this.xdes = new ExtentDescriptorRepository(pool, pageSize);
        this.inodeRepo = new SegmentInodeRepository(pool, pageSize);
        this.flst = new Flst(pool);
        this.freeExtents = new FreeExtentService(pool, pageSize, headerRepo, xdes, flst);
        this.segSpace = new SegmentSpaceService(pool, pageSize, headerRepo, inodeRepo, xdes, flst, freeExtents);
        this.allocator = new SegmentPageAllocator(pool, inodeRepo, flst, segSpace, new DefaultExtentAllocationPolicy());
    }

    /** 建表空间：物理建文件（fil）→ 初始化 page0 header（currentSize=initialSize、freeLimit=0、nextSegmentId=1、三链空）→ 保留系统 extent0。 */
    public void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        if (initialSizePages == null) {
            throw new DatabaseValidationException("initial size must not be null");
        }
        pageStore.create(spaceId, path, pageSize, initialSizePages);
        SpaceHeaderSnapshot fresh = new SpaceHeaderSnapshot(spaceId, pageSize, 0,
                initialSizePages, PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, SERVER_VERSION, 1L);
        headerRepo.initialize(mtr, fresh);
        xdes.reserveSystemExtent(mtr, spaceId);
    }

    /** 打开已存在表空间物理文件（fil）。 */
    public void openTablespace(SpaceId spaceId, Path path) {
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        pageStore.open(spaceId, path, pageSize);
    }

    /** 关闭表空间物理句柄（fil）。 */
    public void closeTablespace(SpaceId spaceId) {
        requireSpace(spaceId);
        pageStore.close(spaceId);
    }

    /** 建 segment：分配 segment id（page0）+ inode 槽（page2），返回句柄。 */
    public SegmentRef createSegment(MiniTransaction mtr, SpaceId spaceId, SegmentPurpose purpose) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (purpose == null) {
            throw new DatabaseValidationException("segment purpose must not be null");
        }
        long segId = headerRepo.allocateNextSegmentId(mtr, spaceId);
        int slot = inodeRepo.allocateSlot(mtr, spaceId, SegmentId.of(segId), purpose);
        return new SegmentRef(spaceId, slot, SegmentId.of(segId));
    }

    /**
     * 为 segment 分配一个页；当前空间不足则扩展文件一次再试，仍不足抛 NoFreeSpaceException。
     *
     * <p>分配成功后对该数据页做「页创建」：{@code mtr.newPage(X)} + {@link PageEnvelope#writeHeader}（type=ALLOCATED）
     * → 产 {@code PAGE_INIT(ALLOCATED)} + 信封 PAGE_BYTES，commit 盖 pageLSN。数据页 X latch 入 mtr memo，持到 commit
     * （commit 才盖 pageLSN，需其 guard）；故单 MTR 内批量分配 N 页会同时占 N 个数据页帧。
     */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref) {
        requireMtr(mtr);
        requireRef(ref);
        PageId allocated = doAllocatePage(mtr, ref);
        initAllocatedPage(mtr, allocated);
        return allocated;
    }

    /** 现有分配逻辑（fragment→extent，autoextend 一次重试），只决定页号、不碰数据页帧。 */
    private PageId doAllocatePage(MiniTransaction mtr, SegmentRef ref) {
        Optional<PageId> first = allocator.allocatePage(mtr, ref.spaceId(), ref.inodeSlot());
        if (first.isPresent()) {
            return first.get();
        }
        PageNo newSize = pageStore.extend(ref.spaceId());
        headerRepo.setCurrentSizeInPages(mtr, ref.spaceId(), newSize);
        Optional<PageId> second = allocator.allocatePage(mtr, ref.spaceId(), ref.inodeSlot());
        if (second.isPresent()) {
            return second.get();
        }
        throw new NoFreeSpaceException("no free space for segment " + ref.segmentId().value()
                + " in tablespace " + ref.spaceId().value());
    }

    /**
     * 页创建：{@code newPage(X)} 取零帧（驻留则重初始化）+ 写信封（type=ALLOCATED；pageLsn=0 由 commit 盖真值）。
     * 不自行 close guard（mtr memo 持有，commit 释放并盖 pageLSN）。
     */
    private void initAllocatedPage(MiniTransaction mtr, PageId p) {
        PageGuard g = mtr.newPage(pool, p, PageLatchMode.EXCLUSIVE, PageType.ALLOCATED);
        PageEnvelope.writeHeader(g, new FilePageHeader(
                p.spaceId(), p.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.ALLOCATED));
    }

    /** 释放一个属于该 segment 的页。 */
    public void freePage(MiniTransaction mtr, SegmentRef ref, PageId pageId) {
        requireMtr(mtr);
        requireRef(ref);
        segSpace.freePage(mtr, ref.spaceId(), ref.inodeSlot(), pageId);
    }

    /** 删除 segment：释放其全部 fragment 页与 extent（归还 FSP_FREE）后清空 inode 槽。 */
    public void dropSegment(MiniTransaction mtr, SegmentRef ref) {
        requireMtr(mtr);
        requireRef(ref);
        SpaceId spaceId = ref.spaceId();
        int slot = ref.inodeSlot();
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
        for (int f = 0; f < 32; f++) {
            Optional<PageNo> fragment = inodeRepo.getFragmentPage(mtr, spaceId, slot, f);
            if (fragment.isPresent()) {
                segSpace.freePage(mtr, spaceId, slot, PageId.of(spaceId, fragment.get()));
            }
        }
        releaseSegmentExtents(mtr, spaceId, inodeRepo.freeExtentListBaseAddr(spaceId, slot));
        releaseSegmentExtents(mtr, spaceId, inodeRepo.notFullExtentListBaseAddr(spaceId, slot));
        releaseSegmentExtents(mtr, spaceId, inodeRepo.fullExtentListBaseAddr(spaceId, slot));
        inodeRepo.freeSlot(mtr, spaceId, slot);
    }

    /** 用量快照。 */
    public SpaceUsage usage(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        SpaceHeaderSnapshot h = headerRepo.read(mtr, spaceId);
        return new SpaceUsage(h.currentSizeInPages(), h.freeLimitPageNo(), h.nextSegmentId());
    }

    /** 把一条 segment extent 链上的所有 extent 逐个摘下并归还 FSP_FREE。 */
    private void releaseSegmentExtents(MiniTransaction mtr, SpaceId spaceId, FileAddress base) {
        while (true) {
            FileAddress head = flst.getFirst(mtr, spaceId, base);
            if (head.isNull()) {
                break;
            }
            ExtentId ext = xdes.extentIdOfNode(spaceId, head);
            flst.remove(mtr, spaceId, base, head);
            freeExtents.returnFreeExtent(mtr, spaceId, ext);
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

    private static void requireRef(SegmentRef ref) {
        if (ref == null) {
            throw new DatabaseValidationException("segment ref must not be null");
        }
    }
}
