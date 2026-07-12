package cn.zhangyis.db.storage.api;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException;
import cn.zhangyis.db.storage.fil.exception.TablespaceUnavailableException;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;


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
import cn.zhangyis.db.storage.api.tablespace.PageZeroTablespaceMetadataLoader;
import cn.zhangyis.db.storage.fil.meta.CachingTablespaceRegistry;
import cn.zhangyis.db.storage.fil.io.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.SpaceFlags;
import cn.zhangyis.db.storage.fil.meta.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.fsp.extent.DefaultExtentAllocationPolicy;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationDirection;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.extent.FreeExtentService;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.exception.NoFreeSpaceException;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservation;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservationKind;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservationService;
import cn.zhangyis.db.storage.fsp.segment.SegmentInodeRepository;
import cn.zhangyis.db.storage.fsp.segment.SegmentInode;
import cn.zhangyis.db.storage.fsp.segment.SegmentPageAllocator;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.fsp.segment.SegmentSpaceService;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.mtr.MtrRedoCategory;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.redo.FspPageAllocationRecord;
import cn.zhangyis.db.storage.redo.FspPageFreeRecord;

import java.nio.file.Path;
import java.util.List;
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
    /** 表空间运行时注册表：建/开登记权威 metadata，后续空间管理 API 在 Task 8 经 require 做状态准入。 */
    private final TablespaceRegistry registry;
    private final SpaceHeaderRepository headerRepo;
    private final ExtentDescriptorRepository xdes;
    private final SegmentInodeRepository inodeRepo;
    private final Flst flst;
    private final FreeExtentService freeExtents;
    private final SegmentSpaceService segSpace;
    private final SegmentPageAllocator allocator;
    /** 多页操作预留服务：在真正分配 page 前预检并预扩容量，避免 split/undo grow 半途 ENOSPC。 */
    private final SpaceReservationService reservationService;

    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize) {
        this(pool, pageStore, pageSize, defaultRegistry(pageStore, pageSize));
    }

    /**
     * 创建共享 operation controller 的默认组合。生命周期服务、MTR manager 和本构造器应接收同一实例，
     * 使默认 page0 loader 与普通页访问/flush/truncate 形成真实互斥。
     */
    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize,
                            cn.zhangyis.db.storage.fil.access.TablespaceAccessController accessController) {
        this(pool, pageStore, pageSize, new CachingTablespaceRegistry(
                new PageZeroTablespaceMetadataLoader(pageStore, pageSize, accessController)));
    }

    /**
     * 注入 registry 的构造器。默认构造器使用 {@link CachingTablespaceRegistry} +
     * {@link PageZeroTablespaceMetadataLoader}，使直接 {@code PageStore.open} 后的 require 懒加载能从 page0 重建 metadata。
     *
     * @param pool Buffer Pool，供 FSP 仓储通过 MTR 访问 page0/page2/XDES。
     * @param pageStore 物理页访问门面，仍保持 registry-free。
     * @param pageSize 实例页大小。
     * @param registry 表空间运行时 metadata/状态注册表。
     */
    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize, TablespaceRegistry registry) {
        if (pool == null || pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("DiskSpaceManager dependencies must not be null");
        }
        if (registry == null) {
            throw new DatabaseValidationException("tablespace registry must not be null");
        }
        this.pool = pool;
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.registry = registry;
        this.headerRepo = new SpaceHeaderRepository(pool);
        this.xdes = new ExtentDescriptorRepository(pool, pageSize);
        this.inodeRepo = new SegmentInodeRepository(pool, pageSize);
        this.flst = new Flst(pool);
        this.freeExtents = new FreeExtentService(pool, pageSize, headerRepo, xdes, flst);
        this.segSpace = new SegmentSpaceService(pool, pageSize, headerRepo, inodeRepo, xdes, flst, freeExtents);
        this.allocator = new SegmentPageAllocator(pool, pageSize, headerRepo, inodeRepo, flst, segSpace,
                new DefaultExtentAllocationPolicy());
        this.reservationService = new SpaceReservationService(pageStore, pageSize, headerRepo, flst);
    }

    private static TablespaceRegistry defaultRegistry(PageStore pageStore, PageSize pageSize) {
        if (pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("DiskSpaceManager dependencies must not be null");
        }
        return new CachingTablespaceRegistry(new PageZeroTablespaceMetadataLoader(pageStore, pageSize));
    }

    /** 建表空间：默认 type=GENERAL，保持既有调用方无需改动。 */
    public void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages) {
        createTablespace(mtr, spaceId, path, initialSizePages, TablespaceType.GENERAL);
    }

    /**
     * 建表空间并登记 runtime metadata。数据流为：物理建文件 → 写 page0 SpaceHeader（type 编入 spaceFlags）→
     * 保留系统 extent0 → registry.replace 发布 NORMAL 快照。create 时 page0 可能还未刷盘，因此直接用建表参数构造 metadata，
     * 不走 page-0 raw loader。
     */
    public void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages,
                                 TablespaceType type) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        if (initialSizePages == null) {
            throw new DatabaseValidationException("initial size must not be null");
        }
        if (type == null) {
            throw new DatabaseValidationException("tablespace type must not be null");
        }
        pageStore.create(spaceId, path, pageSize, initialSizePages);
        SpaceHeaderSnapshot fresh = new SpaceHeaderSnapshot(spaceId, pageSize, TablespaceTypeFlags.encode(type),
                initialSizePages, PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, SERVER_VERSION, 1L);
        headerRepo.initialize(mtr, fresh);
        TablespaceState initialState = type == TablespaceType.UNDO
                ? TablespaceState.ACTIVE : TablespaceState.NORMAL;
        if (type == TablespaceType.UNDO) {
            headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
                    initialState, initialSizePages, 0L, initialSizePages, TablespaceState.ACTIVE));
        } else if (type == TablespaceType.GENERAL) {
            headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
                    TablespaceState.NORMAL, initialSizePages, 0L, initialSizePages, TablespaceState.NORMAL));
        }
        xdes.reserveSystemExtent(mtr, spaceId);
        registry.replace(tablespaceMetadata(spaceId, path, type, initialState, initialSizePages));
    }

    /**
     * 根据建表参数构造 registry 快照。currentSize 只作为运行时 metadata 初始值，后续 autoextend 的权威 size 仍在 page0。
     */
    private TablespaceMetadata tablespaceMetadata(SpaceId spaceId, Path path, TablespaceType type,
                                                   TablespaceState state, PageNo currentSize) {
        return new TablespaceMetadata(spaceId, "space-" + spaceId.value(), type, pageSize, state,
                List.of(DataFileDescriptor.single(path, PageNo.of(0), currentSize)),
                new SpaceFlags(TablespaceTypeFlags.encode(type)), currentSize, PageNo.of(0), 1L);
    }

    /** 打开已存在表空间：物理 open 后经 registry.open 从 page0 重建 metadata；注册失败时关闭物理句柄防半开。 */
    public void openTablespace(SpaceId spaceId, Path path) {
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        pageStore.open(spaceId, path, pageSize);
        try {
            registry.open(spaceId);
        } catch (RuntimeException e) {
            pageStore.close(spaceId);
            throw e;
        }
    }

    /**
     * recovery 启动打开表空间：物理 open 后走 requireForRecovery，不执行普通状态白名单，允许后续恢复流程读取损坏状态。
     */
    public void openTablespaceForRecovery(SpaceId spaceId, Path path) {
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        pageStore.open(spaceId, path, pageSize);
        try {
            registry.requireForRecovery(spaceId);
        } catch (RuntimeException e) {
            pageStore.close(spaceId);
            throw e;
        }
    }

    /** 关闭表空间物理句柄（fil）。 */
    public void closeTablespace(SpaceId spaceId) {
        requireSpace(spaceId);
        pageStore.close(spaceId);
    }

    /** 标记表空间 INACTIVE（运行时）：后续空间管理 API require 抛 {@link TablespaceUnavailableException}。 */
    public void markTablespaceInactive(SpaceId spaceId) {
        requireSpace(spaceId);
        registry.markInactive(spaceId);
    }

    /**
     * 标记表空间 CORRUPTED（运行时）：后续空间管理 API require 抛 {@link TablespaceCorruptedException}。
     * 该入口只发布当前进程 registry 状态，不写 page0 lifecycle marker；重启后是否仍损坏取决于权威 metadata。
     */
    public void markTablespaceCorrupted(SpaceId spaceId, String reason) {
        requireSpace(spaceId);
        registry.markCorrupted(spaceId, reason);
    }

    /**
     * 持久标记 GENERAL 表空间 CORRUPTED。数据流为：先在当前 MTR 内取得 ordinary access lease 并复核 registry 状态，
     * 再以 page0 X latch 读取 FSP header，校验该空间确为 GENERAL，写入 lifecycle marker(CORRUPTED)，最后发布
     * registry CORRUPTED。重启后 page0 loader 会恢复该状态，普通 require 继续拒绝访问；recovery open 仍可读取。
     *
     * @param mtr 当前活动 MTR，负责 page0 latch、redo 收集和异常路径资源释放。
     * @param spaceId 目标表空间。
     * @param reason 损坏原因，必须非空，进入 registry 诊断日志。
     */
    public void markTablespaceCorrupted(MiniTransaction mtr, SpaceId spaceId, String reason) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("corruption reason must not be blank");
        }
        requireOrdinaryAccess(mtr, spaceId);
        // 直接用 X latch 读取 page0，避免同一 MTR 内先 S 后 X 的升级禁令；随后 marker 与状态发布同属一个临界区。
        SpaceHeaderSnapshot snapshot = headerRepo.readForUpdate(mtr, spaceId);
        TablespaceType type = TablespaceTypeFlags.decode(snapshot.spaceFlags());
        if (type != TablespaceType.GENERAL) {
            throw new DatabaseValidationException(
                    "persistent corrupted marker is only supported for GENERAL tablespace: " + type);
        }
        headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
                TablespaceState.CORRUPTED,
                snapshot.currentSizeInPages(),
                0L,
                snapshot.currentSizeInPages(),
                TablespaceState.NORMAL));
        registry.markCorrupted(spaceId, reason);
    }

    /** 标记表空间 DISCARDED（运行时，仅转 registry 状态，不关闭文件）：后续 require 抛 {@link TablespaceNotFoundException}。 */
    public void discardTablespace(SpaceId spaceId) {
        requireSpace(spaceId);
        registry.markDiscarded(spaceId);
    }

    /**
     * 查询 runtime registry 中的表空间状态。该方法不触发 loader，避免诊断路径隐式打开或注册表空间。
     *
     * @param spaceId 表空间编号。
     * @return 当前运行时状态。
     */
    public TablespaceState tablespaceState(SpaceId spaceId) {
        requireSpace(spaceId);
        return registry.find(spaceId)
                .map(handle -> handle.tablespace().state())
                .orElseThrow(() -> new TablespaceNotFoundException("tablespace not registered: " + spaceId.value()));
    }

    /** 建 segment：分配 segment id（page0）+ inode 槽（page2），返回句柄。 */
    public SegmentRef createSegment(MiniTransaction mtr, SpaceId spaceId, SegmentPurpose purpose) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (purpose == null) {
            throw new DatabaseValidationException("segment purpose must not be null");
        }
        requireOrdinaryAccess(mtr, spaceId);
        long segId = headerRepo.allocateNextSegmentId(mtr, spaceId);
        int slot = inodeRepo.allocateSlot(mtr, spaceId, SegmentId.of(segId), purpose);
        return new SegmentRef(spaceId, slot, SegmentId.of(segId));
    }

    /**
     * 为一次可能创建多个 page 的操作预留表空间容量。数据流为：取得 ordinary access lease 并复核 registry 状态 →
     * SpaceReservationService 预扩物理文件/page0 currentSize → reservation 挂入 MTR memo 兜底释放。
     *
     * <p>调用方仍应使用 try-with-resources 缩短 reservation 生命周期；MTR memo 只负责异常路径和遗漏 close 的兜底。
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 目标表空间。
     * @param kind 预留类型。
     * @param pages 本操作最多创建的数据页数。
     * @param extents 本操作额外需要保底的完整 extent 数。
     * @return 可关闭的预留句柄。
     */
    public SpaceReservation reserveSpace(MiniTransaction mtr, SpaceId spaceId, SpaceReservationKind kind,
                                         long pages, long extents) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireOrdinaryAccess(mtr, spaceId);
        SpaceReservation reservation = reservationService.reserve(mtr, spaceId, kind, pages, extents);
        mtr.enlistResource(reservation);
        return reservation;
    }

    /**
     * 为 segment 分配一个页；当前空间不足则扩展文件一次再试，仍不足抛 NoFreeSpaceException。
     *
     * <p>分配成功后对该数据页做「页创建」：{@code mtr.newPage(X)} + {@link PageEnvelope#writeHeader}（type=ALLOCATED）
     * → 产 {@code PAGE_INIT(ALLOCATED)} + 信封 PAGE_BYTES，commit 盖 pageLSN。数据页 X latch 入 mtr memo，持到 commit
     * （commit 才盖 pageLSN，需其 guard）；故单 MTR 内批量分配 N 页会同时占 N 个数据页帧。
     */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref) {
        return allocatePage(mtr, ref, PageAllocationHint.none());
    }

    /**
     * 为 segment 分配一个页，并把方向 hint 传给 FSP extent 策略。旧无 hint 调用通过 {@link PageAllocationHint#none()}
     * 进入这里，保持 fragment→segment extent→autoextend 的既有行为。
     *
     * @param mtr 当前活动 MTR。
     * @param ref segment 句柄。
     * @param hint 页分配 hint；只影响“需要新 extent 时”选择和批量挂段，不直接指定返回页。
     * @return 已初始化为 ALLOCATED 的新页。
     */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref, PageAllocationHint hint) {
        requireMtr(mtr);
        requireRef(ref);
        if (hint == null) {
            throw new DatabaseValidationException("page allocation hint must not be null");
        }
        requireOrdinaryAccess(mtr, ref.spaceId());
        reservationService.consumePageIfReserved(mtr, ref.spaceId());
        AllocationResult allocation = doAllocatePage(mtr, ref, hint);
        mtr.appendLogicalRedo(new FspPageAllocationRecord(
                        allocation.pageId(), ref.inodeSlot(), ref.segmentId(), allocation.autoExtendRetry()),
                MtrRedoCategory.FSP_METADATA_BYTES,
                "FSP page allocation intent before PAGE_INIT");
        initAllocatedPage(mtr, allocation.pageId());
        return allocation.pageId();
    }

    /** 现有分配逻辑（fragment→extent，autoextend 一次重试），只决定页号、不碰数据页帧。 */
    private AllocationResult doAllocatePage(MiniTransaction mtr, SegmentRef ref, PageAllocationHint hint) {
        ExtentAllocationDirection direction = toFspDirection(hint.direction());
        Optional<PageId> first = allocator.allocatePage(mtr, ref.spaceId(), ref.inodeSlot(),
                direction, hint.hintPageNo(), hint.pagesNeeded());
        if (first.isPresent()) {
            return new AllocationResult(first.get(), false);
        }
        PageNo newSize = pageStore.extend(ref.spaceId());
        headerRepo.setCurrentSizeInPages(mtr, ref.spaceId(), newSize);
        Optional<PageId> second = allocator.allocatePage(mtr, ref.spaceId(), ref.inodeSlot(),
                direction, hint.hintPageNo(), hint.pagesNeeded());
        if (second.isPresent()) {
            return new AllocationResult(second.get(), true);
        }
        throw new NoFreeSpaceException("no free space for segment " + ref.segmentId().value()
                + " in tablespace " + ref.spaceId().value());
    }

    private static ExtentAllocationDirection toFspDirection(PageAllocationHint.Direction direction) {
        return switch (direction) {
            case NO_DIRECTION -> ExtentAllocationDirection.NO_DIRECTION;
            case UP -> ExtentAllocationDirection.UP;
            case DOWN -> ExtentAllocationDirection.DOWN;
        };
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

    /**
     * 单次页分配的内部结果。`autoExtendRetry` 只记录 facade 是否走过物理扩展后的第二次 allocator 尝试，
     * 不暴露 FSP 内部 fragment/extent 决策，避免 B+Tree/Undo 调用方依赖空间管理实现细节。
     */
    private record AllocationResult(PageId pageId, boolean autoExtendRetry) {
        private AllocationResult {
            if (pageId == null) {
                throw new DatabaseValidationException("allocated page id must not be null");
            }
        }
    }

    /** 释放一个属于该 segment 的页。 */
    public void freePage(MiniTransaction mtr, SegmentRef ref, PageId pageId) {
        requireMtr(mtr);
        requireRef(ref);
        requireOrdinaryAccess(mtr, ref.spaceId());
        appendPageFreeIntent(mtr, ref, pageId);
        segSpace.freePage(mtr, ref.spaceId(), ref.inodeSlot(), pageId);
    }

    /** 删除 segment：释放其全部 fragment 页与 extent（归还 FSP_FREE）后清空 inode 槽。 */
    public void dropSegment(MiniTransaction mtr, SegmentRef ref) {
        requireMtr(mtr);
        requireRef(ref);
        requireOrdinaryAccess(mtr, ref.spaceId());
        SpaceId spaceId = ref.spaceId();
        int slot = ref.inodeSlot();
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(2)), PageLatchMode.EXCLUSIVE);
        for (int f = 0; f < 32; f++) {
            Optional<PageNo> fragment = inodeRepo.getFragmentPage(mtr, spaceId, slot, f);
            if (fragment.isPresent()) {
                PageId pageId = PageId.of(spaceId, fragment.get());
                appendPageFreeIntent(mtr, ref, pageId);
                segSpace.freePage(mtr, spaceId, slot, pageId);
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
        requireOrdinaryAccess(mtr, spaceId);
        SpaceHeaderSnapshot h = headerRepo.read(mtr, spaceId);
        return new SpaceUsage(h.currentSizeInPages(), h.freeLimitPageNo(), h.nextSegmentId());
    }

    /**
     * 在 drop 写 MTR 前只读物化 fragment/extent 规模。数据只来自 inode page2：校验 segment identity，扫描 32 个
     * fragment 槽，并汇总三条 extent list 的持久 length；不遍历 XDES、不修改 FSP、不持有跨返回的 latch。
     */
    public SegmentDropPlan inspectDropSegmentPlan(MiniTransaction mtr, SegmentRef ref) {
        requireMtr(mtr);
        requireRef(ref);
        requireOrdinaryAccess(mtr, ref.spaceId());
        SegmentInode inode = inodeRepo.read(mtr, ref.spaceId(), ref.inodeSlot());
        if (!inode.segmentId().equals(ref.segmentId())) {
            throw new FspMetadataException(
                    "segment drop plan identity mismatch: expected=" + ref.segmentId().value()
                            + ", current=" + inode.segmentId().value());
        }
        long fragments = 0;
        for (int slot = 0; slot < 32; slot++) {
            if (inodeRepo.getFragmentPage(mtr, ref.spaceId(), ref.inodeSlot(), slot).isPresent()) {
                fragments++;
            }
        }
        try {
            long extents = Math.addExact(inode.freeExtentList().length(), inode.notFullExtentList().length());
            extents = Math.addExact(extents, inode.fullExtentList().length());
            return new SegmentDropPlan(fragments, extents, inode.usedPageCount());
        } catch (ArithmeticException error) {
            throw new FspMetadataException(
                    "segment extent count overflows for " + ref.segmentId().value(), error);
        }
    }

    private static void appendPageFreeIntent(MiniTransaction mtr, SegmentRef ref, PageId pageId) {
        mtr.appendLogicalRedo(new FspPageFreeRecord(pageId, ref.inodeSlot(), ref.segmentId()),
                MtrRedoCategory.FSP_METADATA_BYTES,
                "FSP page free intent before metadata PAGE_BYTES compatibility redo");
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

    /**
     * 先把表空间共享 lease 收进 MTR，再重新检查运行时状态。若调用在 truncate X lease 后排队，
     * 醒来时会看到最终 ACTIVE/INACTIVE 状态，不会沿用截断前已经通过的陈旧检查结果。
     */
    private void requireOrdinaryAccess(MiniTransaction mtr, SpaceId spaceId) {
        mtr.acquireTablespaceLease(spaceId);
        registry.require(spaceId);
    }
}
