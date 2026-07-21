package cn.zhangyis.db.storage.api;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException;
import cn.zhangyis.db.storage.fil.exception.TablespaceUnavailableException;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;


import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
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
import cn.zhangyis.db.storage.fsp.exception.SpaceReservationExceededException;
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
 * 表空间、段、区和页分配的稳定存储门面，对上层隐藏 FSP 元数据页与 FIL 文件句柄。
 *
 * <p>职责边界：本类把 B+Tree、Undo、LOB 和物理 DDL 的领域请求编排为 {@code storage.fsp} 元数据修改、
 * {@link BufferPool} 页访问与 {@link PageStore} 文件动作；返回 {@link SegmentRef}、{@link PageId}、
 * {@link SpaceReservation} 等稳定领域对象，不向调用方暴露 page0、XDES、INODE、FLST 或裸字节。</p>
 *
 * <p>一致性边界：除纯文件打开/关闭和运行时状态发布外，空间管理写操作都要求调用方传入活动
 * {@link MiniTransaction}。方法先把 tablespace shared operation lease 纳入 MTR memo，再复核
 * {@link TablespaceRegistry} 状态；FSP page fix/latch、redo 收集和 reservation 也由同一 MTR 按 LIFO 释放。
 * 本类不持有数据库事务行锁，也不能跨 MTR 保存 page guard。</p>
 *
 * <p>权威状态：page0 的 FSP/lifecycle 数据与 page2 inode、XDES/FLST 是可恢复的持久状态；registry 只保存当前进程的
 * 准入快照。普通路径拒绝不可用、损坏、截断中或已丢弃的空间，恢复打开路径才允许读取这些中间状态。</p>
 *
 * <p>自动扩展：allocator 在 page0 当前逻辑大小内首次分配失败后，本门面只执行一次物理扩展，更新 page0
 * {@code currentSizeInPages} 并重试；仍失败则抛 {@link NoFreeSpaceException}。页分配/释放 intent、FSP metadata delta
 * 与新页初始化进入同一 MTR redo batch，崩溃恢复可据此重放元数据并把物理文件容量对齐到恢复出的逻辑边界。</p>
 *
 * <p>与 MySQL/InnoDB 的差异：当前实现面向单文件教学型 tablespace，autoextend 委托 {@link PageStore} 的既定策略且
 * 每次调用最多重试一次；尚未实现完整 InnoDB 多文件 system/general tablespace 与后台空间整理策略。</p>
 */
public final class DiskSpaceManager {

    /**
     * 新建 page0 写入的教学引擎格式版本，仅供恢复与诊断识别创建方，不参与 SQL server 版本协商。
     * 修改该值不能替代磁盘格式升级或兼容迁移。
     */
    private static final int SERVER_VERSION = 80046;

    /**
     * FSP 仓储访问 page0、XDES、INODE 与新数据页的统一缓存入口；page fix/latch 必须归当前 MTR memo 所有。
     */
    private final BufferPool pool;

    /**
     * 表空间文件创建、打开、扩展和关闭的纯物理协作者；不理解 segment/extent 状态，也不代替 registry 做准入判断。
     */
    private final PageStore pageStore;

    /**
     * 当前引擎实例的固定页大小，用于 extent 几何和磁盘偏移计算；必须与 Buffer Pool、PageStore 及 page0 格式一致。
     */
    private final PageSize pageSize;

    /**
     * 表空间运行时 metadata 与 lifecycle 准入快照；持久权威仍在 page0/字典，普通空间管理必须在取得 lease 后调用
     * {@link TablespaceRegistry#require(SpaceId)} 复核，避免沿用 truncate/drop 前的陈旧状态。
     */
    private final TablespaceRegistry registry;

    /** page0 FSP header/lifecycle 的仓储，维护逻辑容量、segment id high-water 与全局 extent list 入口。 */
    private final SpaceHeaderRepository headerRepo;

    /** XDES 元数据仓储，维护 extent owner、状态与 page bitmap；所有写入受当前 MTR 的 X latch 保护。 */
    private final ExtentDescriptorRepository xdes;

    /** page2 segment inode 仓储，维护 segment identity、用途、fragment 槽和三类 extent list。 */
    private final SegmentInodeRepository inodeRepo;

    /** 持久 FLST 原语，负责 page0、XDES 与 inode 中双向链表节点的原子摘挂。 */
    private final Flst flst;

    /** 全局空闲 extent 协作者，在 FSP_FREE/FREE_FRAG/FULL_FRAG 之间迁移 extent。 */
    private final FreeExtentService freeExtents;

    /** segment 空间协作者，联合 inode、XDES 和 FLST 完成页归属与 extent list 状态转换。 */
    private final SegmentSpaceService segSpace;

    /**
     * fragment→已有 segment extent→新 extent 的页号选择器；只修改 FSP 归属，不负责物理文件扩展和数据页格式化。
     */
    private final SegmentPageAllocator allocator;

    /**
     * 多页操作的进程内容量承诺账本；其内部显式锁只保护 reservation 计数，不跨 Buffer Pool latch 或文件扩展等待，
     * 返回句柄同时登记到 MTR memo，确保异常路径释放未消费配额。
     */
    private final SpaceReservationService reservationService;

    /**
     * 使用默认懒加载 registry 创建门面。该组合适合不需要与 truncate/flush 共享 operation controller 的低层场景；
     * 正式引擎应优先注入共享 {@link TablespaceRegistry} 或 {@link TablespaceAccessController}。
     *
     * @param pool FSP 元数据和新页的缓存访问入口；不能为 {@code null}
     * @param pageStore 已配置页大小策略的物理文件入口；不能为 {@code null}
     * @param pageSize 当前实例唯一页大小；不能为 {@code null}
     * @throws DatabaseValidationException 任一依赖为空时抛出，构造失败不会创建或打开表空间
     */
    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize) {
        this(pool, pageStore, pageSize, defaultRegistry(pageStore, pageSize));
    }

    /**
     * 使用共享 operation controller 创建懒加载 registry。生命周期服务、MTR manager 与本门面应注入同一 controller，
     * 使 page0 metadata loader、普通 MTR 页访问、flush 与 truncate/drop 在同一 tablespace lease 域内互斥。
     *
     * @param pool FSP 元数据和新页的缓存访问入口；不能为 {@code null}
     * @param pageStore 表空间物理文件入口；不能为 {@code null}
     * @param pageSize 当前实例唯一页大小；不能为 {@code null}
     * @param accessController 与 MTR、flush、生命周期服务共享的表空间 operation lease 控制器；不能为 {@code null}
     * @throws DatabaseValidationException 任一依赖为空时抛出，不会留下部分初始化的门面对象
     */
    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize,
                            TablespaceAccessController accessController) {
        this(pool, pageStore, pageSize, new CachingTablespaceRegistry(
                new PageZeroTablespaceMetadataLoader(pageStore, pageSize, accessController)));
    }

    /**
     * 注入 registry 并组装 FSP 协作者。调用方负责保证 registry 的 metadata loader 与当前 PageStore/pageSize 配套；
     * 否则运行时准入快照可能与实际文件格式不一致。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在发布任何字段前校验 Buffer Pool、PageStore、PageSize 和 registry，避免构造出只具备部分空间管理能力的对象。</li>
     *     <li>以共享 Buffer Pool/pageSize 依次组装 page0、XDES、INODE、FLST、extent、segment、allocator 与 reservation
     *     协作者，使全部空间元数据写入都汇入调用方 MTR，而不是各自创建隐藏事务。</li>
     * </ol>
     *
     * @param pool Buffer Pool，供 FSP 仓储通过 MTR 访问 page0/page2/XDES。
     * @param pageStore 物理页访问门面，仍保持 registry-free。
     * @param pageSize 实例页大小。
     * @param registry 表空间运行时 metadata/状态注册表。
     * @throws DatabaseValidationException 任一依赖为空时抛出，不会发布部分初始化状态
     */
    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize, TablespaceRegistry registry) {
        // 1. 所有协作者都必须在字段发布前存在，避免运行期才暴露缺失的 IO 或状态准入能力。
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

        // 2. 仓储和服务共享同一 Buffer Pool/pageSize，调用时只使用上层传入的 MTR 管理 latch、redo 与释放顺序。
        this.headerRepo = new SpaceHeaderRepository(pool);
        this.xdes = new ExtentDescriptorRepository(pool, pageSize);
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.inodeRepo = new SegmentInodeRepository(pool, pageSize);
        this.flst = new Flst(pool);
        this.freeExtents = new FreeExtentService(pool, pageSize, headerRepo, xdes, flst);
        this.segSpace = new SegmentSpaceService(pool, pageSize, headerRepo, inodeRepo, xdes, flst, freeExtents);
        this.allocator = new SegmentPageAllocator(pool, pageSize, headerRepo, inodeRepo, flst, segSpace,
                new DefaultExtentAllocationPolicy());
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.reservationService = new SpaceReservationService(pageStore, pageSize, headerRepo, flst);
    }

    /**
     * 为便捷构造器创建从 page0 懒加载 metadata 的进程内 registry，不额外持有数据文件句柄所有权。
     *
     * @param pageStore metadata loader 读取 page0 与文件路径的物理入口；不能为 {@code null}
     * @param pageSize 校验 page0 envelope 与磁盘偏移时使用的实例页大小；不能为 {@code null}
     * @return 尚未加载任何 tablespace 的缓存 registry
     * @throws DatabaseValidationException 任一依赖为空时抛出
     */
    private static TablespaceRegistry defaultRegistry(PageStore pageStore, PageSize pageSize) {
        if (pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("DiskSpaceManager dependencies must not be null");
        }
        return new CachingTablespaceRegistry(new PageZeroTablespaceMetadataLoader(pageStore, pageSize));
    }

    /**
     * 以 GENERAL 类型创建单文件表空间，完整物理初始化和失败边界与五参重载一致。
     *
     * @param mtr 承载 page0、XDES 与 lifecycle redo 的活动 MTR；不能为 {@code null}
     * @param spaceId 待创建且尚未被 PageStore 占用的稳定空间标识；不能为 {@code null}
     * @param path 新数据文件路径；文件必须尚不存在且不能为 {@code null}
     * @param initialSizePages 初始物理容量，必须足以容纳固定 FSP 管理页；不能为 {@code null}
     * @throws DatabaseValidationException 任一参数为空或下游拒绝初始容量时抛出
     * @throws DatabaseRuntimeException 文件创建或 FSP 元数据初始化失败时抛出；调用方负责清理可能已创建的文件
     */
    public void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages) {
        createTablespace(mtr, spaceId, path, initialSizePages, TablespaceType.GENERAL);
    }

    /**
     * 创建并初始化单文件表空间，把可恢复 FSP 元数据和运行时准入快照绑定到同一 {@link SpaceId}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验 MTR、空间标识、文件路径、初始容量与类型；失败发生在任何文件或页副作用之前。</li>
     *     <li>通过 PageStore 创建并零填充物理文件。该文件动作不受 MTR rollback 管理，后续初始化失败时由 DDL/recovery
     *     协调层关闭并清理受控命名文件。</li>
     *     <li>在当前 MTR 中初始化 page0 FSP header、page1 IBUF_BITMAP 和 page2 INODE 信封，并按
     *     GENERAL/UNDO 写入 NORMAL/ACTIVE lifecycle marker；type 同时编码进 space flags，供重启时恢复。</li>
     *     <li>在 XDES 中保留包含固定管理页的 system extent0，防止普通 segment 把 page0/page2 等元数据页重新分配。</li>
     *     <li>直接依据建表参数发布 registry 快照，不读取尚未刷盘的 page0。该发布不等于持久提交，调用方仍须成功
     *     commit/flush 当前 MTR 后，才能把表空间暴露给普通上层访问。</li>
     * </ol>
     *
     * @param mtr 收集 FSP/lifecycle redo 并持有管理页 latch 的活动 MTR；不能为 {@code null}
     * @param spaceId 新表空间的稳定物理标识；不能为 {@code null}，且不得与已打开文件冲突
     * @param path 新单文件表空间路径；不能为 {@code null}，文件必须尚不存在
     * @param initialSizePages 初始物理和 page0 逻辑容量；不能为 {@code null}，并须容纳固定管理页
     * @param type GENERAL 或 UNDO 等受支持的表空间类型；决定 space flags 与初始 lifecycle 状态
     * @throws DatabaseValidationException 参数为空、类型或容量不合法时抛出，不应据此发布字典 binding
     * @throws DatabaseRuntimeException 文件创建、page0/XDES 初始化或 registry 发布失败时抛出；原始异常保持为 cause
     */
    public void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages,
                                 TablespaceType type) {
        // 1. 在创建文件前完成门面可判定的契约校验，避免无效请求留下空文件或半初始化 page0。
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

        // 2. 先建立零填充物理容量；该 FILE 动作不属于 MTR undo，后续失败由上层 DDL/recovery 负责清理。
        pageStore.create(spaceId, path, pageSize, initialSizePages);

        // 3. 在同一 MTR 中写 page0 FSP/lifecycle，并初始化固定 page1/page2 信封；否则 inode 内容虽有
        // checksum，却会保留零 spaceId/pageNo/type，离线 scrub 与 crash recovery 无法证明页面归属。
        SpaceHeaderSnapshot fresh = new SpaceHeaderSnapshot(spaceId, pageSize, TablespaceTypeFlags.encode(type),
                initialSizePages, PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, SERVER_VERSION, 1L);
        headerRepo.initialize(mtr, fresh);
        PageGuard ibufPage = mtr.newPage(pool, PageId.of(spaceId, PageNo.of(1)),
                PageLatchMode.EXCLUSIVE, PageType.IBUF_BITMAP);
        PageEnvelope.writeHeader(ibufPage, new FilePageHeader(
                spaceId, 1L, FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.IBUF_BITMAP));
        PageGuard inodePage = mtr.newPage(pool, PageId.of(spaceId, PageNo.of(2)),
                PageLatchMode.EXCLUSIVE, PageType.INODE);
        PageEnvelope.writeHeader(inodePage, new FilePageHeader(
                spaceId, 2L, FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INODE));
        TablespaceState initialState = type == TablespaceType.UNDO
                ? TablespaceState.ACTIVE : TablespaceState.NORMAL;
        if (type == TablespaceType.UNDO) {
            headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
                    initialState, initialSizePages, 0L, initialSizePages, TablespaceState.ACTIVE));
        } else if (type == TablespaceType.GENERAL) {
            headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
                    TablespaceState.NORMAL, initialSizePages, 0L, initialSizePages, TablespaceState.NORMAL));
        }

        // 4. extent0 含固定 FSP 管理页，必须先在 XDES 标为系统保留，普通 segment 分配器才可安全进入该空间。
        xdes.reserveSystemExtent(mtr, spaceId);

        // 5. page0 尚未 durable，直接用已校验参数发布运行时快照；调用方须在 MTR commit/flush 后再发布上层可见性。
        registry.replace(tablespaceMetadata(spaceId, path, type, initialState, initialSizePages));
    }

    /**
     * 根据建表参数构造 registry 快照。currentSize 只作为运行时 metadata 初始值，后续 autoextend 的权威 size 仍在 page0。
     *
     * @param spaceId registry key 与 page0 identity；不能为 {@code null}
     * @param path 唯一数据文件的规范化路径，由 PageStore 负责最终路径校验
     * @param type 从建表请求写入 space flags 的表空间类型
     * @param state 与 lifecycle marker 一致的初始运行时状态
     * @param currentSize page0 和物理文件创建时采用的初始页数
     * @return 可供 registry 原子替换的不可变 metadata 快照；它不是持久 page0 的替代品
     */
    private TablespaceMetadata tablespaceMetadata(SpaceId spaceId, Path path, TablespaceType type,
                                                   TablespaceState state, PageNo currentSize) {
        return new TablespaceMetadata(spaceId, "space-" + spaceId.value(), type, pageSize, state,
                List.of(DataFileDescriptor.single(path, PageNo.of(0), currentSize)),
                new SpaceFlags(TablespaceTypeFlags.encode(type)), currentSize, PageNo.of(0), 1L);
    }

    /**
     * 打开已有单文件表空间，并从 page0 重建包含真实 lifecycle 状态的运行时 metadata。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验空间标识与路径，禁止用空 identity 打开无法登记的文件。</li>
     *     <li>PageStore 校验文件存在、长度按页对齐并登记物理句柄；此阶段不解析 page0 业务语义。</li>
     *     <li>registry loader 读取并校验 page0 envelope/checksum/FSP/lifecycle，再原样发布 NORMAL、CORRUPTED 等真实状态；
     *     本方法不把状态伪装成可用，后续普通空间管理仍须经 {@code registry.require} 白名单准入。加载失败时立即关闭
     *     刚打开的物理句柄，防止半开文件泄漏。</li>
     * </ol>
     *
     * @param spaceId 文件 page0 应声明的稳定空间标识；不能为 {@code null}
     * @param path 已存在且页对齐的单文件表空间路径；不能为 {@code null}
     * @throws DatabaseValidationException 参数为空时抛出，不会打开文件
     * @throws DatabaseRuntimeException 文件打开、page0 格式/identity 校验或 registry 发布失败时抛出；失败句柄会被关闭
     */
    public void openTablespace(SpaceId spaceId, Path path) {
        // 1. identity/path 必须在物理 open 前合法，后续 registry 才能把 page0 内容绑定到确定的运行时 key。
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }

        // 2. PageStore 只打开物理句柄并校验文件几何，不在 FIL 层解释 FSP 或 lifecycle 状态。
        pageStore.open(spaceId, path, pageSize);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        try {
            // 3. loader 校验 page0 并发布真实 lifecycle；普通可用性留给后续 require 判定，异常时关闭物理句柄。
            registry.open(spaceId);
        } catch (RuntimeException e) {
            pageStore.close(spaceId);
            throw e;
        }
    }

    /**
     * 为启动恢复打开已有表空间，允许 recovery 编排读取普通路径会拒绝的 lifecycle 中间态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验恢复配置提供的 space identity 与路径，避免把未知文件挂到空 key。</li>
     *     <li>PageStore 打开物理文件并建立按页 IO 句柄。</li>
     *     <li>{@link TablespaceRegistry#requireForRecovery(SpaceId)} 仍校验 page0 格式与 identity，但跳过普通状态白名单，
     *     允许 CORRUPTED/TRUNCATING 等状态交给后续 redo、truncate 或 DDL recovery；失败时关闭物理句柄。</li>
     * </ol>
     *
     * @param spaceId 恢复配置与 page0 应共同声明的空间标识；不能为 {@code null}
     * @param path 待恢复且已存在的表空间文件；不能为 {@code null}
     * @throws DatabaseValidationException 参数为空时抛出，不会打开文件
     * @throws DatabaseRuntimeException 文件打开或 recovery metadata 校验失败时抛出；失败句柄会被关闭
     */
    public void openTablespaceForRecovery(SpaceId spaceId, Path path) {
        // 1. 恢复配置必须提供确定的物理 identity/path，避免 loader 把错误文件发布到 registry。
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }

        // 2. 先建立物理 IO 句柄，page0 loader 才能读取权威 FSP/lifecycle metadata。
        pageStore.open(spaceId, path, pageSize);
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        try {
            // 3. recovery require 允许中间状态但不放松格式/identity 校验；失败统一关闭刚打开的句柄。
            registry.requireForRecovery(spaceId);
        } catch (RuntimeException e) {
            pageStore.close(spaceId);
            throw e;
        }
    }

    /**
     * 关闭 PageStore 中的表空间物理句柄，不修改 page0 lifecycle，也不替代 DROP/truncate 的排空协议。
     * 调用方必须先阻止新 MTR 进入并处理 Buffer Pool 中的 dirty/fixed frame；本方法只执行 FIL 关闭动作。
     *
     * @param spaceId 已打开且完成上层排空的表空间标识；不能为 {@code null}
     * @throws DatabaseValidationException 标识为空时抛出
     * @throws DatabaseRuntimeException 文件尚被占用或底层句柄关闭失败时抛出，调用方应保持 fail-closed 状态
     */
    public void closeTablespace(SpaceId spaceId) {
        requireSpace(spaceId);
        pageStore.close(spaceId);
    }

    /**
     * 判断运行期 registry 是否仍持有目标表空间句柄，不触发 page0 加载或文件打开。
     *
     * <p>该只读证据供 recovery-unavailable 的离线文件操作建立“不得绕过在线句柄”的前置条件；返回
     * {@code false} 只说明本进程没有发布该空间，不证明路径一定存在或文件内容可信。</p>
     *
     * @param spaceId 待检查的稳定表空间标识；不得为 {@code null}
     * @return registry 当前含有任意生命周期状态句柄时为 {@code true}
     * @throws DatabaseValidationException 标识为空时抛出
     */
    public boolean isTablespaceOpen(SpaceId spaceId) {
        requireSpace(spaceId);
        return registry.isOpen(spaceId);
    }

    /**
     * 仅在当前进程把表空间标记为 INACTIVE，使后续普通空间管理准入抛 {@link TablespaceUnavailableException}。
     * 本方法不写 page0、不关闭文件；需要跨重启保持状态时必须由 UNDO lifecycle 编排写持久 marker。
     *
     * @param spaceId 已登记表空间的稳定标识；不能为 {@code null}
     * @throws DatabaseValidationException 标识为空时抛出
     * @throws TablespaceNotFoundException registry 中不存在该空间时抛出，调用方应重新执行 discovery/open
     */
    public void markTablespaceInactive(SpaceId spaceId) {
        requireSpace(spaceId);
        registry.markInactive(spaceId);
    }

    /**
     * 标记表空间 CORRUPTED（运行时）：后续空间管理 API require 抛 {@link TablespaceCorruptedException}。
     * 该入口只发布当前进程 registry 状态，不写 page0 lifecycle marker；重启后是否仍损坏取决于权威 metadata。
     *
     * @param spaceId 已登记表空间的稳定标识；不能为 {@code null}
     * @param reason 可供日志、诊断与恢复决策使用的具体损坏原因
     * @throws DatabaseValidationException 标识为空或 registry 拒绝空白原因时抛出
     * @throws TablespaceNotFoundException registry 中不存在该空间时抛出
     */
    public void markTablespaceCorrupted(SpaceId spaceId, String reason) {
        requireSpace(spaceId);
        registry.markCorrupted(spaceId, reason);
    }

    /**
     * 在当前 MTR 中写 GENERAL 表空间的 CORRUPTED lifecycle marker，并立即让本进程普通路径 fail-closed。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验 MTR、空间标识与诊断原因，避免无效请求改变运行时准入。</li>
     *     <li>把 shared operation lease 登记进 MTR memo 后重新执行普通 registry require，确保不会跨越并发
     *     truncate/drop 使用陈旧的 NORMAL 快照。</li>
     *     <li>以 page0 X latch 读取权威 FSP header 并确认类型为 GENERAL；不先拿 S latch，避免同一 MTR 发生 latch upgrade。</li>
     *     <li>写 CORRUPTED lifecycle redo/页修改后发布 registry CORRUPTED。registry 立即阻断新访问，即使后续 commit
     *     失败也保持保守 fail-closed；跨重启状态只有在调用方成功 commit/flush 后才由 page0 marker 保证。</li>
     * </ol>
     *
     * @param mtr 当前活动 MTR，负责 page0 latch、redo 收集和异常路径资源释放。
     * @param spaceId 目标表空间。
     * @param reason 损坏原因，必须非空，进入 registry 诊断日志。
     * @throws DatabaseValidationException 参数为空、原因空白或目标不是 GENERAL 时抛出
     * @throws TablespaceNotFoundException 表空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 表空间处于不允许普通写入的 lifecycle 状态时抛出
     * @throws TablespaceCorruptedException registry 已将该空间标为损坏、无需重复走普通写路径时抛出
     */
    public void markTablespaceCorrupted(MiniTransaction mtr, SpaceId spaceId, String reason) {
        // 1. 参数和诊断原因先于 lease/page latch 校验，失败时不改变 registry 或 page0。
        requireMtr(mtr);
        requireSpace(spaceId);
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("corruption reason must not be blank");
        }

        // 2. shared operation lease 进入 MTR memo 后再复核 runtime state，消除与 truncate/drop 的状态先检后等待竞态。
        requireOrdinaryAccess(mtr, spaceId);

        // 3. 直接用 X latch 读取 page0 并校验 GENERAL，避免同一 MTR 内先 S 后 X 的升级禁令。
        SpaceHeaderSnapshot snapshot = headerRepo.readForUpdate(mtr, spaceId);
        TablespaceType type = TablespaceTypeFlags.decode(snapshot.spaceFlags());
        if (type != TablespaceType.GENERAL) {
            throw new DatabaseValidationException(
                    "persistent corrupted marker is only supported for GENERAL tablespace: " + type);
        }

        // 4. 先收集持久 marker 修改，再发布 runtime CORRUPTED；失败时保持 latch/lease 由 MTR 统一释放。
        headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
                TablespaceState.CORRUPTED,
                snapshot.currentSizeInPages(),
                0L,
                snapshot.currentSizeInPages(),
                TablespaceState.NORMAL));
        registry.markCorrupted(spaceId, reason);
    }

    /**
     * 仅把当前进程 registry 状态转换为 DISCARDED，使后续普通 require 按对象不存在处理。
     * 本方法不写 page0、不关闭句柄、不删除文件；它适用于调用方已经拥有其他持久恢复依据的清理路径。
     *
     * @param spaceId 已登记表空间的稳定标识；不能为 {@code null}
     * @throws DatabaseValidationException 标识为空时抛出
     * @throws TablespaceNotFoundException registry 中不存在目标空间时抛出
     */
    public void discardTablespace(SpaceId spaceId) {
        requireSpace(spaceId);
        registry.markDiscarded(spaceId);
    }

    /**
     * 在 DROP 已持有的独占 operation lease 内，为 GENERAL 表空间记录可恢复的 DISCARDED 意图。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 DROP marker MTR 与目标空间 identity，不接受无所有者的持久状态修改。</li>
     *     <li>让 MTR 取得可重入 shared lease 并在 lease 后复核 NORMAL，确保 marker 不与普通 allocation 或 truncate 交叉。</li>
     *     <li>以 page0 X latch 读取当前逻辑大小和类型，只允许 GENERAL 进入该 DROP 协议。</li>
     *     <li>写 DISCARDED lifecycle redo/页修改并发布 registry DISCARDED。调用方必须在 marker commit、redo/data durable
     *     之后才能关闭或删除文件；删除失败时 recovery 可依据 page0 marker 继续清理。</li>
     * </ol>
     *
     * @param mtr DROP marker 的写 MTR。
     * @param spaceId 目标 GENERAL 表空间。
     * @throws DatabaseValidationException 参数为空或目标不是 GENERAL 时抛出
     * @throws TablespaceNotFoundException 表空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 表空间不允许普通 lifecycle 写入时抛出
     * @throws TablespaceCorruptedException 表空间已损坏、不能继续普通 DROP marker 路径时抛出
     */
    public void markTablespaceDiscarded(MiniTransaction mtr, SpaceId spaceId) {
        // 1. marker 必须归属明确 MTR/SpaceId，失败时不得接触 registry 或 page0。
        requireMtr(mtr);
        requireSpace(spaceId);

        // 2. DROP X lease 内的 shared lease 是可重入的；lease 后 require 保证观察到最新 runtime state。
        requireOrdinaryAccess(mtr, spaceId);

        // 3. page0 X latch 保护类型、逻辑大小和 lifecycle marker，非 GENERAL 空间不得复用该删除协议。
        SpaceHeaderSnapshot snapshot = headerRepo.readForUpdate(mtr, spaceId);
        TablespaceType type = TablespaceTypeFlags.decode(snapshot.spaceFlags());
        if (type != TablespaceType.GENERAL) {
            throw new DatabaseValidationException(
                    "persistent discarded marker is only supported for GENERAL tablespace: " + type);
        }

        // 4. marker 进入当前 MTR redo 后发布 runtime DISCARDED；物理关闭/删除必须等待上层持久化屏障。
        headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
                TablespaceState.DISCARDED, snapshot.currentSizeInPages(), 0L,
                snapshot.currentSizeInPages(), TablespaceState.NORMAL));
        registry.markDiscarded(spaceId);
    }

    /**
     * 将已通过外部文件 identity 校验的 DISCARDED GENERAL 表空间恢复为 NORMAL。
     * 该入口只供 IMPORT/recovery 使用，不经过普通状态准入；成功后 page0 spaceVersion 单调递增。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void restoreTablespace(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        SpaceHeaderSnapshot snapshot = headerRepo.readForUpdate(mtr, spaceId);
        TablespaceType type = TablespaceTypeFlags.decode(snapshot.spaceFlags());
        if (type != TablespaceType.GENERAL) {
            throw new DatabaseValidationException("only GENERAL tablespace can be restored from DISCARD");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        TablespaceLifecycleHeader lifecycle = headerRepo.readLifecycle(mtr, spaceId)
                .orElseThrow(() -> new DatabaseRuntimeException("discarded tablespace lacks lifecycle marker"));
        if (lifecycle.state() != TablespaceState.DISCARDED) {
            throw new DatabaseValidationException("tablespace is not DISCARDED: " + lifecycle.state());
        }
        headerRepo.bumpSpaceVersion(mtr, spaceId, snapshot.spaceVersion());
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        headerRepo.writeLifecycle(mtr, spaceId, new TablespaceLifecycleHeader(
                TablespaceState.NORMAL, snapshot.currentSizeInPages(), 0L,
                snapshot.currentSizeInPages(), TablespaceState.NORMAL));
    }

    /** 重新从已 force 的 page0 加载 runtime metadata；只用于 IMPORT/recovery 的完成阶段。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     */
    public void refreshTablespaceMetadata(SpaceId spaceId) {
        requireSpace(spaceId);
        registry.refresh(spaceId);
    }

    /**
     * 查询 runtime registry 中的表空间状态。该方法不触发 loader，避免诊断路径隐式打开或注册表空间。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 SpaceId，但不获取 operation lease、不读取 page0，也不改变 registry cache。</li>
     *     <li>只查询已经发布的运行时句柄并复制当前 state；未命中明确按“未登记”失败，不把磁盘状态猜测为 NORMAL。</li>
     * </ol>
     *
     * @param spaceId 表空间编号。
     * @return 当前运行时状态。
     * @throws DatabaseValidationException 标识为空时抛出
     * @throws TablespaceNotFoundException 本方法不触发 loader，目标尚未登记时抛出
     */
    public TablespaceState tablespaceState(SpaceId spaceId) {
        // 1. 诊断查询只验证 identity，不隐式取得 lease 或触发 page0 loader。
        requireSpace(spaceId);

        // 2. registry 已发布快照是本方法唯一数据源；未登记必须显式失败，不能伪造默认状态。
        return registry.find(spaceId)
                .map(handle -> handle.tablespace().state())
                .orElseThrow(() -> new TablespaceNotFoundException("tablespace not registered: " + spaceId.value()));
    }

    /**
     * 在目标表空间创建空 segment，并返回后续页分配所需的稳定 inode identity。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR、SpaceId 与用途，避免消耗 segment id high-water 后才发现无效请求。</li>
     *     <li>取得 shared operation lease 并复核普通准入，防止在 truncate/drop 状态切换期间创建新 owner。</li>
     *     <li>先在 page0 单调分配 segment id，再在 page2 写入带用途的空 inode 槽；两处修改同属当前 MTR，返回的
     *     {@link SegmentRef} 同时携带 space、slot 与 id，后续写入必须复核这组 identity。</li>
     * </ol>
     *
     * @param mtr 收集 page0/page2 redo 并按顺序持有 X latch 的活动 MTR；不能为 {@code null}
     * @param spaceId 已打开且允许普通空间管理的目标表空间；不能为 {@code null}
     * @param purpose segment 的唯一用途，决定后续 leaf/non-leaf/LOB/UNDO 分配策略；不能为 {@code null}
     * @return 指向已初始化 inode 槽的稳定 segment 句柄，尚不包含任何业务页
     * @throws DatabaseValidationException 参数为空时抛出，不消耗 segment identity
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间处于非普通访问状态时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws FspMetadataException segment id 或 inode 槽无法安全分配时抛出，调用方应回滚当前 MTR
     */
    public SegmentRef createSegment(MiniTransaction mtr, SpaceId spaceId, SegmentPurpose purpose) {
        // 1. 参数校验必须早于 page0 high-water 修改，避免非法用途留下无意义的 segment id 间隙。
        requireMtr(mtr);
        requireSpace(spaceId);
        if (purpose == null) {
            throw new DatabaseValidationException("segment purpose must not be null");
        }

        // 2. operation lease 进入 MTR memo 后复核 registry，阻止 lifecycle 已切换的空间继续增加 owner。
        requireOrdinaryAccess(mtr, spaceId);

        // 3. 先分配单调 identity，再写带用途的 inode；两页 latch/redo 由同一 MTR 统一提交或释放。
        long segId = headerRepo.allocateNextSegmentId(mtr, spaceId);
        int slot = inodeRepo.allocateSlot(mtr, spaceId, SegmentId.of(segId), purpose);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return new SegmentRef(spaceId, slot, SegmentId.of(segId));
    }

    /**
     * 为一次多页操作建立进程内容量承诺，在首次实际页分配前完成必要的物理扩容与 page0 逻辑容量推进。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR 与 SpaceId，保证 reservation 能绑定到明确的物理工作单元和目标空间。</li>
     *     <li>取得 shared operation lease 并复核普通 registry 状态，禁止为 truncate/drop/corrupted 空间承诺新容量。</li>
     *     <li>reservation service 校验 kind/pages/extents，读取 page0/FLST 容量快照和进程内已承诺计数；不足时先扩展
     *     物理文件，再在当前 MTR 中推进 page0 {@code currentSizeInPages}，最后发布容量账本记录。</li>
     *     <li>把返回句柄登记到 MTR memo，使 commit/rollback 异常路径能释放剩余承诺；调用方仍应使用
     *     try-with-resources 缩短正常路径的 reservation 生命周期。</li>
     * </ol>
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 目标表空间。
     * @param kind NORMAL/UNDO/CLEANING/BLOB 等容量用途，供账本与诊断区分工作负载；不能为 {@code null}
     * @param pages 本操作最多创建的数据页数；必须非负，并与 extents 至少一个大于零
     * @param extents 本操作额外需要保底的完整 extent 数；必须非负，并与 pages 至少一个大于零
     * @return 绑定当前 MTR/SpaceId 的活动 RAII 句柄；关闭后未消费承诺立即归还，不能再次用于页配额消费
     * @throws DatabaseValidationException 参数为空、预算为负/全零或容量计算溢出时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间处于非普通访问状态时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws FspMetadataException page0/FLST 容量元数据不一致时抛出，调用方必须放弃当前 MTR
     * @throws DatabaseRuntimeException 物理扩容或 reservation 账本发布失败时抛出，物理文件不会在此处自动缩回
     */
    public SpaceReservation reserveSpace(MiniTransaction mtr, SpaceId spaceId, SpaceReservationKind kind,
                                         long pages, long extents) {
        // 1. reservation 必须绑定有效 MTR/SpaceId；kind 和数值预算由 service 在读取容量前统一校验。
        requireMtr(mtr);
        requireSpace(spaceId);

        // 2. lease 后复核 lifecycle，避免为并发 truncate/drop 已切换的空间发布新容量承诺。
        requireOrdinaryAccess(mtr, spaceId);

        // 3. 在不持容量账本锁等待 page latch 的前提下读取/扩展容量，再由 service 短锁发布进程内承诺。
        SpaceReservation reservation = reservationService.reserve(mtr, spaceId, kind, pages, extents);

        // 4. 句柄进入 MTR memo 兜底释放；正常调用方仍可提前 close，SpaceReservation 自身保证幂等。
        mtr.enlistResource(reservation);
        return reservation;
    }

    /**
     * 使用无方向 hint 为 segment 分配并初始化一个 ALLOCATED 页，保留 fragment→extent→单次 autoextend 的默认策略。
     *
     * @param mtr 承载 FSP、allocation intent 和 PAGE_INIT redo 的活动 MTR；不能为 {@code null}
     * @param ref 目标 segment 的稳定 space/slot/id 句柄；不能为 {@code null}
     * @return 已归属目标 segment、信封类型为 ALLOCATED 且由当前 MTR 持有 X latch 的新页标识
     * @throws DatabaseValidationException 参数为空时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间不允许普通分配时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws SpaceReservationExceededException 当前 MTR 对该空间存在 reservation 但页配额已耗尽时抛出
     * @throws NoFreeSpaceException 当前容量分配失败且单次 autoextend 后仍无页可用时抛出
     * @throws FspMetadataException segment identity、inode、XDES 或 FLST 元数据不一致时抛出
     */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref) {
        return allocatePage(mtr, ref, PageAllocationHint.none());
    }

    /**
     * 为 segment 分配一个 ALLOCATED 页，并把方向、邻近页和页需求 hint 传给新 extent 选择策略。
     * hint 不影响 fragment 槽和已有 segment extent 的优先级，也不能强制返回指定页号。
     *
     * @param mtr 当前活动 MTR。
     * @param ref segment 句柄。
     * @param hint 页分配 hint；只影响“需要新 extent 时”选择和批量挂段，不直接指定返回页。
     * @return 已归属目标 segment、完成 PAGE_INIT(ALLOCATED) 且由当前 MTR 持有 X latch 的新页标识
     * @throws DatabaseValidationException MTR、segment 或 hint 为空时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间不允许普通分配时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws SpaceReservationExceededException 当前 MTR 的 reservation 页配额耗尽时抛出
     * @throws NoFreeSpaceException 单次 autoextend 后仍无法分配时抛出
     * @throws FspMetadataException FSP owner/list/bitmap 不一致时抛出，调用方应放弃当前 MTR
     */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref, PageAllocationHint hint) {
        return allocatePage(mtr, ref, hint, PageType.ALLOCATED);
    }

    /**
     * 使用无方向 hint 为专用数据结构分配页，并直接写入受门面允许的目标页类型。
     * 当前只允许 ALLOCATED 与 BLOB；调用方必须在同一 MTR 内继续完成具体 body 格式，不能提前发布页引用。
     *
     * @param mtr 承载 FSP 与 PAGE_INIT redo 的活动 MTR；不能为 {@code null}
     * @param ref 目标 segment 的稳定句柄；不能为 {@code null}
     * @param pageType 初始 envelope 类型，只允许 ALLOCATED 或 BLOB；不能为 {@code null}
     * @return 已归属 segment 并写入目标 envelope 类型的新页标识
     * @throws DatabaseValidationException 参数为空或 pageType 不在允许集合时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间不允许普通分配时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws SpaceReservationExceededException 当前 MTR 的 reservation 页配额耗尽时抛出
     * @throws NoFreeSpaceException 单次 autoextend 后仍无法分配时抛出
     * @throws FspMetadataException FSP 元数据损坏或 segment identity 不一致时抛出
     */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref, PageType pageType) {
        return allocatePage(mtr, ref, PageAllocationHint.none(), pageType);
    }

    /**
     * 带方向 hint 与物理页类型的统一页分配入口，把容量承诺、FSP 归属、autoextend、逻辑 redo 和新页信封纳入同一 MTR。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR、segment、hint 和 pageType，只允许通用 ALLOCATED 或由 LOB 格式化路径使用的 BLOB。</li>
     *     <li>取得 tablespace shared operation lease 并复核普通 registry 状态，阻止 lifecycle 切换后的新页分配。</li>
     *     <li>若当前 MTR/SpaceId 有活动 reservation，则在进入 FSP page latch 前原子消费一个页配额；配额耗尽直接失败，
     *     没有 reservation 的兼容调用保持放行。</li>
     *     <li>按 fragment、已有 segment extent、新 extent 顺序选择页；逻辑容量不足时物理扩展一次、推进 page0 后重试。</li>
     *     <li>在 PAGE_INIT 前追加携带 segment identity 与 autoextend 分支的 allocation intent，再创建零页、写 envelope；
     *     新页 X latch/fix 留在 MTR memo，commit 分配 pageLSN 后才释放。专用页调用方须在同一 MTR 完成 body 格式化。</li>
     * </ol>
     *
     * @param mtr 当前活动且尚未进入 commit 的 MTR；不能为 {@code null}
     * @param ref 目标 segment 的 space/inode slot/segment id 句柄；不能为 {@code null}
     * @param hint 新 extent 选择方向、邻近页和预期页数；不能为 {@code null}
     * @param pageType 新页 envelope 类型，只允许 ALLOCATED 或 BLOB；不能为 {@code null}
     * @return 已更新 FSP 归属、追加 allocation intent 并完成 PAGE_INIT 的新页标识
     * @throws DatabaseValidationException 参数为空或 pageType 不受通用分配入口支持时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间处于非普通访问状态时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws SpaceReservationExceededException 已存在 reservation 但页配额耗尽时抛出，FSP 尚未修改
     * @throws NoFreeSpaceException 首次分配和单次 autoextend 重试均失败时抛出
     * @throws FspMetadataException segment/inode/XDES/FLST 元数据不一致时抛出，调用方必须放弃当前 MTR
     */
    public PageId allocatePage(MiniTransaction mtr, SegmentRef ref, PageAllocationHint hint, PageType pageType) {
        // 1. 先验证门面契约，尤其限制 page type，避免 FSP 已分配后才发现调用方试图绕过专用格式化入口。
        requireMtr(mtr);
        requireRef(ref);
        if (hint == null || pageType == null) {
            throw new DatabaseValidationException("page allocation hint/type must not be null");
        }
        if (pageType != PageType.ALLOCATED && pageType != PageType.BLOB) {
            throw new DatabaseValidationException("generic segment allocation cannot initialize page type: " + pageType);
        }

        // 2. lease 后复核 registry lifecycle，避免从 truncate/drop 已切换状态的空间继续取得新页。
        requireOrdinaryAccess(mtr, ref.spaceId());

        // 3. reservation 消费只触碰当前 MTR 的原子 quota，不持全局账本锁等待后续 FSP page latch。
        reservationService.consumePageIfReserved(mtr, ref.spaceId());

        // 4. FSP 先建立 segment→page 归属；必要时只 autoextend 一次并同步推进 page0 逻辑容量。
        AllocationResult allocation = doAllocatePage(mtr, ref, hint);

        // 5. allocation intent 必须排在 PAGE_INIT 前进入同一 redo batch，随后新页 guard 留给 commit 盖 pageLSN。
        mtr.appendLogicalRedo(new FspPageAllocationRecord(
                        allocation.pageId(), ref.inodeSlot(), ref.segmentId(), allocation.autoExtendRetry()),
                MtrRedoCategory.FSP_METADATA_BYTES,
                "FSP page allocation intent before PAGE_INIT");
        initAllocatedPage(mtr, allocation.pageId(), pageType);
        return allocation.pageId();
    }

    /**
     * 在 FSP 元数据内选择页号；首次失败时扩展物理文件并推进 page0 后仅重试一次，不创建数据页 frame。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>把稳定 API hint 转换为 FSP 方向，在当前 page0 逻辑容量内尝试 fragment→已有 extent→新 extent 分配。</li>
     *     <li>无可用页时调用 PageStore autoextend，并把返回的物理页数写入当前 MTR 的 page0 metadata。</li>
     *     <li>以相同 hint 重试一次；成功结果标记 autoextend 分支供 redo/recovery 解释，仍失败则明确抛空间不足，
     *     不循环扩容，也不在此处缩回已经扩展的物理文件。</li>
     * </ol>
     *
     * @param mtr 已取得普通 tablespace lease、负责 FSP metadata redo 的活动 MTR
     * @param ref 已校验的目标 segment 句柄
     * @param hint 已校验的页分配局部性提示
     * @return 新页 identity 与是否经历 autoextend 重试的内部结果；尚未创建数据页 frame
     * @throws NoFreeSpaceException 单次扩展后的第二次分配仍失败时抛出
     * @throws FspMetadataException segment/extent 元数据不一致或 page0 容量无法推进时抛出
     * @throws DatabaseRuntimeException 物理文件扩展失败时抛出，调用方必须放弃当前 MTR
     */
    private AllocationResult doAllocatePage(MiniTransaction mtr, SegmentRef ref, PageAllocationHint hint) {
        // 1. API hint 只在 FSP 需要新 extent 时生效；首次尝试严格限制在 page0 当前逻辑容量内。
        ExtentAllocationDirection direction = toFspDirection(hint.direction());
        Optional<PageId> first = allocator.allocatePage(mtr, ref.spaceId(), ref.inodeSlot(),
                direction, hint.hintPageNo(), hint.pagesNeeded());
        if (first.isPresent()) {
            return new AllocationResult(first.get(), false);
        }

        // 2. 首次无页可用才扩展单文件，并在同一 MTR 推进 page0，使恢复能把物理容量重对齐到逻辑边界。
        PageNo newSize = pageStore.extend(ref.spaceId());
        headerRepo.setCurrentSizeInPages(mtr, ref.spaceId(), newSize);

        // 3. 相同策略只重试一次；结果携带 autoextend 标记，仍失败则保留扩容并由上层处理明确 ENOSPC。
        Optional<PageId> second = allocator.allocatePage(mtr, ref.spaceId(), ref.inodeSlot(),
                direction, hint.hintPageNo(), hint.pagesNeeded());
        if (second.isPresent()) {
            return new AllocationResult(second.get(), true);
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        throw new NoFreeSpaceException("no free space for segment " + ref.segmentId().value()
                + " in tablespace " + ref.spaceId().value());
    }

    /**
     * 把 storage.api 的稳定方向枚举映射为 FSP 内部策略枚举，防止上层依赖 fsp 包类型。
     *
     * @param direction 已由 {@link PageAllocationHint} 保证非空的 API 方向
     * @return 与输入一一对应的 FSP extent 搜索方向，不改变 hint 的邻近页或页需求
     */
    private static ExtentAllocationDirection toFspDirection(PageAllocationHint.Direction direction) {
        return switch (direction) {
            case NO_DIRECTION -> ExtentAllocationDirection.NO_DIRECTION;
            case UP -> ExtentAllocationDirection.UP;
            case DOWN -> ExtentAllocationDirection.DOWN;
        };
    }

    /**
     * 为已完成 FSP 归属的新页创建零初始化 frame，并写入统一文件页信封；不解释专用 body 格式。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>通过 {@code mtr.newPage(X)} 创建或重初始化 frame，PAGE_INIT redo 与 X latch/fix 一并登记到 MTR。</li>
     *     <li>写入 space/page identity、空 sibling 指针和目标 type；pageLSN 暂为 0，commit 取得 batch end LSN 后统一盖戳。
     *     本方法不能提前 close guard，否则 commit 无法安全发布 pageLSN/dirty 状态。</li>
     * </ol>
     *
     * @param mtr 已追加 allocation intent、拥有新页生命周期的活动 MTR
     * @param p 已归属 segment 且尚未向上层发布的新页标识
     * @param pageType ALLOCATED 或 BLOB 的初始 envelope 类型
     */
    private void initAllocatedPage(MiniTransaction mtr, PageId p, PageType pageType) {
        // 1. newPage 以 X latch 返回零帧并把 PAGE_INIT/fix/latch 归入当前 MTR memo。
        PageGuard g = mtr.newPage(pool, p, PageLatchMode.EXCLUSIVE, pageType);

        // 2. 先建立可校验信封，pageLSN 留给 commit 盖真实 end LSN；guard 不在本方法关闭。
        PageEnvelope.writeHeader(g, new FilePageHeader(
                p.spaceId(), p.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, pageType));
    }

    /**
     * 在专用写路径进入页分配/释放前，以持久 inode 复核 segment identity 与用途，并预先建立 page0→page2 锁顺序。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR、segment 句柄和预期用途，不接受缺失 owner identity 的专用写入。</li>
     *     <li>取得 tablespace shared operation lease 并复核普通准入，阻止在 lifecycle 切换后继续写 segment。</li>
     *     <li>先取得较小 PageId 的 page0 X latch，再由 inode repository 取得 page2 X latch；直接使用 X 是因为后续 FSP
     *     更新同样需要写 page2，提前拿 S 会违反 MTR 禁止 latch upgrade 的约束。</li>
     *     <li>比较 inode 中的 segment id 与 purpose；不一致时抛元数据异常，page0/page2 latch 仍由调用方 MTR 的
     *     commit/rollback 路径统一释放，禁止继续使用该句柄。</li>
     * </ol>
     *
     * @param mtr 后续专用写入将复用其 page0/page2 X latch 的活动 MTR；不能为 {@code null}
     * @param ref 待验证的稳定 space/inode slot/segment id 句柄；不能为 {@code null}
     * @param expectedPurpose 调用路径要求的唯一用途，例如 LOB 或 UNDO；不能为 {@code null}
     * @throws DatabaseValidationException 参数为空时抛出，不读取 FSP 页
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间不允许普通访问时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws FspMetadataException inode identity 或 purpose 与句柄不一致时抛出，调用方必须放弃本次写入
     */
    public void requireSegmentPurposeForWrite(MiniTransaction mtr, SegmentRef ref, SegmentPurpose expectedPurpose) {
        // 1. 所有 identity/purpose 参数必须先合法，避免无意义地占用 tablespace lease 或管理页 latch。
        requireMtr(mtr);
        requireRef(ref);
        if (expectedPurpose == null) {
            throw new DatabaseValidationException("expected segment purpose must not be null");
        }

        // 2. lease 后复核 registry，确保 inode 校验观察的是仍允许普通写入的表空间。
        requireOrdinaryAccess(mtr, ref.spaceId());

        // 3. 按 page0→page2 全序取得 X latch，避免仅持 page2 后反向等待 page0；两者进入 MTR memo。
        mtr.getPage(pool, PageId.of(ref.spaceId(), PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        SegmentInode inode = inodeRepo.readExclusive(mtr, ref.spaceId(), ref.inodeSlot());

        // 4. 持久 inode 是 identity/purpose 权威；不匹配即 fail-closed，latch 由上层结束 MTR 时释放。
        if (!inode.segmentId().equals(ref.segmentId()) || inode.purpose() != expectedPurpose) {
            throw new FspMetadataException("segment identity/purpose mismatch: expected="
                    + ref.segmentId().value() + "/" + expectedPurpose + ", actual="
                    + inode.segmentId().value() + "/" + inode.purpose());
        }
    }

    /**
     * 单次页分配的内部结果。{@code autoExtendRetry} 只记录 facade 是否走过物理扩展后的第二次 allocator 尝试，
     * 不暴露 FSP 内部 fragment/extent 决策，避免 B+Tree/Undo 调用方依赖空间管理实现细节。
     *
     * @param pageId 已建立 FSP 归属、尚未执行 PAGE_INIT 的新页标识
     * @param autoExtendRetry {@code true} 表示首次分配失败后扩展物理文件并在第二次尝试取得该页
     */
    private record AllocationResult(PageId pageId, boolean autoExtendRetry) {
        private AllocationResult {
            if (pageId == null) {
                throw new DatabaseValidationException("allocated page id must not be null");
            }
        }
    }

    /**
     * 回收一个已从上层结构安全摘除、且确认不再被事务可见性需要的 segment 页，只修改 FSP 归属元数据。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR 与 segment 句柄；pageId 的 space/owner 合法性由 redo record 与 SegmentSpaceService 继续验证。</li>
     *     <li>取得 shared operation lease 并复核普通准入，禁止在 truncate/drop 状态中并发归还页。</li>
     *     <li>先追加携带 segment identity 的 free intent，再更新 inode/XDES/FLST；extent 状态可能从 FULL→NOT_FULL，
     *     全空的非 fragment extent 会归还 FSP_FREE。所有修改进入当前 MTR，物理文件不会因此缩短。</li>
     * </ol>
     *
     * <p>本方法不执行 B+Tree sibling/parent unlink、不判断 MVCC/Purge 安全点，也不排空或失效 Buffer Pool frame；
     * 调用方必须在进入本方法前完成这些上层协议，提交后不得再通过旧 PageId 访问该逻辑页。</p>
     *
     * @param mtr 承载 free intent 与 FSP metadata redo 的活动 MTR；不能为 {@code null}
     * @param ref 释放前仍拥有目标页的 segment 句柄；不能为 {@code null}
     * @param pageId 已从上层结构摘除、待归还 FSP 的物理页标识；必须属于 ref.spaceId
     * @throws DatabaseValidationException 任一 identity 参数为空时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间不允许普通访问时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws FspMetadataException 页不属于目标 segment、已重复释放或 inode/XDES/FLST 不一致时抛出
     */
    public void freePage(MiniTransaction mtr, SegmentRef ref, PageId pageId) {
        // 1. MTR/segment identity 必须先存在；pageId 的 owner/space 由 intent 与 FSP service 做权威校验。
        requireMtr(mtr);
        requireRef(ref);

        // 2. lease 后复核 lifecycle，防止与 truncate/drop 的 FSP 重建或文件删除交叉。
        requireOrdinaryAccess(mtr, ref.spaceId());

        // 3. free intent 排在 metadata mutation 前进入同一 MTR，随后由 FSP 更新 bitmap、inode 计数和 extent list。
        appendPageFreeIntent(mtr, ref, pageId);
        segSpace.freePage(mtr, ref.spaceId(), ref.inodeSlot(), pageId);
    }

    /**
     * 物理回收一个已由上层停止引用且内容无需保留的 segment，最终释放 inode identity。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR/segment 并取得 ordinary access lease，阻止 lifecycle 切换期间开始整段回收。</li>
     *     <li>按 page0→page2 顺序取得 X latch，固定全局 FSP list 与目标 inode，避免 fragment/extent 归还时锁顺序反转。</li>
     *     <li>扫描固定 32 个 fragment 槽；对每个存在页先追加 free intent，再更新 inode/XDES/全局 fragment list。</li>
     *     <li>依次排空 SEG_FREE、SEG_NOT_FULL、SEG_FULL 三条 extent list，把每个 extent 摘链后归还 FSP_FREE。</li>
     *     <li>所有归属都清空后释放 inode 槽，使旧 {@link SegmentRef} 永久失效；失败时不得继续复用该 MTR 或旧句柄。</li>
     * </ol>
     *
     * <p>调用方必须预先停止 B+Tree/Undo/LOB 对该 segment 的访问并处理 Buffer Pool/事务可见性；本方法不删除
     * tablespace 文件，也不负责 DROP 的 durable lifecycle marker。</p>
     *
     * @param mtr 承载全部 fragment/extent/inode 元数据修改与 redo 的活动 MTR；不能为 {@code null}
     * @param ref 已确认可物理销毁的 segment identity；不能为 {@code null}
     * @throws DatabaseValidationException 参数为空时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间不允许普通访问时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws FspMetadataException fragment、extent list、owner 或 inode 状态不一致时抛出，调用方应进入恢复/诊断路径
     */
    public void dropSegment(MiniTransaction mtr, SegmentRef ref) {
        // 1. segment 回收必须归属活动 MTR，并在 tablespace lease 后确认当前 lifecycle 仍允许普通 FSP 修改。
        requireMtr(mtr);
        requireRef(ref);
        requireOrdinaryAccess(mtr, ref.spaceId());
        SpaceId spaceId = ref.spaceId();
        int slot = ref.inodeSlot();

        // 2. 先 page0 后 page2 取得 X latch，固定全局/segment list base，后续循环不得反向获取更小管理页。
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)), PageLatchMode.EXCLUSIVE);
        mtr.getPage(pool, PageId.of(spaceId, PageNo.of(2)), PageLatchMode.EXCLUSIVE);

        // 3. fragment 页逐个记录 free intent 并归还；空槽跳过，不能把 FIL_NULL 当作真实 PageNo。
        for (int f = 0; f < 32; f++) {
            Optional<PageNo> fragment = inodeRepo.getFragmentPage(mtr, spaceId, slot, f);
            if (fragment.isPresent()) {
                PageId pageId = PageId.of(spaceId, fragment.get());
                appendPageFreeIntent(mtr, ref, pageId);
                segSpace.freePage(mtr, spaceId, slot, pageId);
            }
        }

        // 4. 三类 segment extent 都必须摘链并归还全局 FSP_FREE，避免遗留 owner 指针或容量泄漏。
        releaseSegmentExtents(mtr, spaceId, inodeRepo.freeExtentListBaseAddr(spaceId, slot));
        releaseSegmentExtents(mtr, spaceId, inodeRepo.notFullExtentListBaseAddr(spaceId, slot));
        releaseSegmentExtents(mtr, spaceId, inodeRepo.fullExtentListBaseAddr(spaceId, slot));

        // 5. 只有 fragment 与三条 extent list 全空后才释放 inode slot；此后旧 SegmentRef 不得再次使用。
        inodeRepo.freeSlot(mtr, spaceId, slot);
    }

    /**
     * 读取 page0 的逻辑空间使用快照，不扫描物理文件、XDES 或进程内 reservation 账本。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR/SpaceId 并在 shared operation lease 后复核普通 registry 状态。</li>
     *     <li>以 page0 S latch 读取同一时刻的 current size、free limit 与 next segment id。</li>
     *     <li>复制为不可变 {@link SpaceUsage} 后返回；page latch/fix 仍由当前 MTR 生命周期释放，返回对象不持资源。</li>
     * </ol>
     *
     * @param mtr 负责 page0 S latch/fix 生命周期的活动 MTR；不能为 {@code null}
     * @param spaceId 待查询且允许普通访问的表空间；不能为 {@code null}
     * @return page0 权威逻辑容量与 high-water 快照，不包含物理文件实际长度或未消费 reservation
     * @throws DatabaseValidationException 参数为空时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间不允许普通访问时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws FspMetadataException page0 无法通过格式、identity 或边界校验时抛出
     */
    public SpaceUsage usage(MiniTransaction mtr, SpaceId spaceId) {
        // 1. 查询也必须进入 operation lease 并在 lease 后复核状态，避免与 truncate 重建 page0 交叉。
        requireMtr(mtr);
        requireSpace(spaceId);
        requireOrdinaryAccess(mtr, spaceId);

        // 2. page0 S latch 下读取三个相互关联的权威字段，禁止拆成跨时刻的独立查询。
        SpaceHeaderSnapshot h = headerRepo.read(mtr, spaceId);

        // 3. 返回纯值快照，不把 page guard、repository 或 registry handle 泄漏给调用方。
        return new SpaceUsage(h.currentSizeInPages(), h.freeLimitPageNo(), h.nextSegmentId());
    }

    /**
     * 在 drop 写 MTR 前只读物化 fragment/extent 规模，用于精确估算后续整段释放的 redo admission。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR/segment，在 operation lease 后复核普通访问状态，防止对正在 truncate/drop 的 inode 规划预算。</li>
     *     <li>以 page2 S latch 读取 inode，并用 segment id 复核 slot 未被释放后复用；不修改 FSP，也不遍历 XDES。</li>
     *     <li>扫描固定 32 个 fragment 槽，统计真实 page identity 数量；再读取 SEG_FREE/NOT_FULL/FULL 三条 list 的
     *     持久 length 与 inode used-page counter。</li>
     *     <li>用精确加法构造不可变 {@link SegmentDropPlan}；计数溢出或 identity 损坏时 fail-closed。返回前不主动
     *     延长任何 latch/fix 生命周期，调用方应结束该只读 MTR，再开始独立 drop 写 MTR。</li>
     * </ol>
     *
     * @param mtr 只读规划 MTR，负责 page2 S latch/fix 的获取与释放；不能为 {@code null}
     * @param ref 待规划 segment 的稳定 space/slot/id 句柄；不能为 {@code null}
     * @return fragment 数、三类 extent 总数和 used-page counter 的不可变预算快照，不持有 page 资源
     * @throws DatabaseValidationException 参数为空时抛出
     * @throws TablespaceNotFoundException 目标空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 目标空间不允许普通读取时抛出
     * @throws TablespaceCorruptedException 目标空间已损坏时抛出
     * @throws FspMetadataException inode identity 不匹配或 extent 计数溢出时抛出，禁止据此低估 drop redo
     */
    public SegmentDropPlan inspectDropSegmentPlan(MiniTransaction mtr, SegmentRef ref) {
        // 1. 规划必须绑定只读 MTR，并在 tablespace lease 后观察稳定且允许普通访问的 inode。
        requireMtr(mtr);
        requireRef(ref);
        requireOrdinaryAccess(mtr, ref.spaceId());

        // 2. 持久 inode 是 slot identity 与三条 extent list 长度的权威，旧 SegmentRef 必须在这里被拒绝。
        SegmentInode inode = inodeRepo.read(mtr, ref.spaceId(), ref.inodeSlot());
        if (!inode.segmentId().equals(ref.segmentId())) {
            throw new FspMetadataException(
                    "segment drop plan identity mismatch: expected=" + ref.segmentId().value()
                            + ", current=" + inode.segmentId().value());
        }

        // 3. fragment 使用固定 32 槽逐一统计，避免把空槽或 inode used-page counter 误当成 fragment 数。
        long fragments = 0;
        for (int slot = 0; slot < 32; slot++) {
            if (inodeRepo.getFragmentPage(mtr, ref.spaceId(), ref.inodeSlot(), slot).isPresent()) {
                fragments++;
            }
        }

        // 4. 三条持久 list length 用 exact arithmetic 汇总；溢出视为 metadata 损坏，不能返回低估预算。
        try {
            long extents = Math.addExact(inode.freeExtentList().length(), inode.notFullExtentList().length());
            extents = Math.addExact(extents, inode.fullExtentList().length());
            return new SegmentDropPlan(fragments, extents, inode.usedPageCount());
        } catch (ArithmeticException error) {
            throw new FspMetadataException(
                    "segment extent count overflows for " + ref.segmentId().value(), error);
        }
    }

    /**
     * 在 FSP bitmap/inode/list 修改前追加逻辑 free intent，使 recovery 能识别被归还页及其原 segment identity。
     *
     * @param mtr 将 intent 与后续 metadata delta 放入同一 redo batch 的活动 MTR
     * @param ref 释放前拥有目标页的 segment identity
     * @param pageId 即将从 segment 归属中移除的物理页标识
     */
    private static void appendPageFreeIntent(MiniTransaction mtr, SegmentRef ref, PageId pageId) {
        mtr.appendLogicalRedo(new FspPageFreeRecord(pageId, ref.inodeSlot(), ref.segmentId()),
                MtrRedoCategory.FSP_METADATA_BYTES,
                "FSP page free intent before metadata PAGE_BYTES compatibility redo");
    }

    /**
     * 排空一条 segment extent list，把每个持久节点先从 owner 链摘除，再归还全局 FSP_FREE。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>反复读取当前链首；空地址表示该类 extent 已全部归还，循环结束。</li>
     *     <li>由 XDES node address 解析 extent identity，先从 segment FLST 摘链，再清理 owner/bitmap 状态并挂回
     *     FSP_FREE；顺序不能颠倒，否则恢复或并发诊断可能同时观察到两个 owner。</li>
     * </ol>
     *
     * @param mtr 已持有 page0/page2 X latch、收集 FLST/XDES metadata delta 的活动 MTR
     * @param spaceId segment 与全局 FSP_FREE 所属表空间
     * @param base 待排空的 SEG_FREE、SEG_NOT_FULL 或 SEG_FULL 持久 list base address
     * @throws FspMetadataException FLST node、XDES identity 或 owner 状态不一致时抛出
     */
    private void releaseSegmentExtents(MiniTransaction mtr, SpaceId spaceId, FileAddress base) {
        // 1. 每轮重新读取持久链首；前一轮摘链后 base 已更新，不能沿用陈旧 next 指针跨 repository 调用。
        while (true) {
            FileAddress head = flst.getFirst(mtr, spaceId, base);
            if (head.isNull()) {
                break;
            }

            // 2. 先从 segment owner 链摘除，再由 FreeExtentService 清理 XDES owner 并挂回全局 FSP_FREE。
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
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>通过 MTR 获取 tablespace shared operation lease，lease 进入 memo 并在 MTR 结束时按 LIFO 释放。</li>
     *     <li>仅在 lease 已授予后调用 registry.require，重新判定 NORMAL/ACTIVE 等普通可用状态；若等待期间 lifecycle
     *     已变化则 fail-closed，不触碰任何 FSP page。</li>
     * </ol>
     *
     * @param mtr 当前空间管理操作的活动 MTR，拥有 lease 生命周期
     * @param spaceId 待复核普通访问资格的表空间标识
     * @throws TablespaceNotFoundException 空间未登记或已丢弃时抛出
     * @throws TablespaceUnavailableException 空间处于 INACTIVE/TRUNCATING 等不可普通访问状态时抛出
     * @throws TablespaceCorruptedException 空间已损坏时抛出
     */
    private void requireOrdinaryAccess(MiniTransaction mtr, SpaceId spaceId) {
        // 1. lease 先进入 MTR memo；任何后续异常都由 MTR 释放，不能在条件分支中遗漏 unlock/release。
        mtr.acquireTablespaceLease(spaceId);

        // 2. lease 后重新读取 runtime state，消除“先检查通过、等待 X lease、醒来后继续使用旧状态”的竞态。
        registry.require(spaceId);
    }
}
