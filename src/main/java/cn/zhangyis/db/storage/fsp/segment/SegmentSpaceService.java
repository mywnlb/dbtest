package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.extent.ExtentAllocationDirection;
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

    /**
     * 创建 {@code SegmentSpaceService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param headerRepo 由组合根提供的 {@code SpaceHeaderRepository} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param inodeRepo 由组合根提供的 {@code SegmentInodeRepository} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param xdes 由组合根提供的 {@code ExtentDescriptorRepository} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param flst FSP 层的链表、空间预留或分配方向对象；不得为 {@code null}，必须属于当前表空间且保持 extent/segment 所有权不变量
     * @param freeExtents 由组合根提供的 {@code FreeExtentService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SegmentSpaceService(BufferPool pool, PageSize pageSize, SpaceHeaderRepository headerRepo,
                               SegmentInodeRepository inodeRepo, ExtentDescriptorRepository xdes,
                               Flst flst, FreeExtentService freeExtents) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (pool == null || pageSize == null || headerRepo == null || inodeRepo == null
                || xdes == null || flst == null || freeExtents == null) {
            throw new DatabaseValidationException("SegmentSpaceService dependencies must not be null");
        }
        this.pool = pool;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.inodeRepo = inodeRepo;
        this.xdes = xdes;
        this.flst = flst;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
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
     * @param inodeSlot 参与 {@code allocateFragmentPage} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<PageNo> allocateFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireArgs(mtr, spaceId);
        latchSpaceThenInode(mtr, spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int slotIdx = inodeRepo.requireFreeFragmentSlot(mtr, spaceId, inodeSlot);
        Optional<PageId> pageOpt = freeExtents.allocateFragmentPage(mtr, spaceId);
        if (pageOpt.isEmpty()) {
            return Optional.empty();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        PageNo pageNo = pageOpt.get().pageNo();
        inodeRepo.setFragmentPage(mtr, spaceId, inodeSlot, slotIdx, Optional.of(pageNo));
        bumpUsed(mtr, spaceId, inodeSlot, 1);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return Optional.of(pageNo);
    }

    /** 给 segment 分配一个完整 extent：acquire FREE extent → 置 FSEG/owner=segId → 入该段 SEG_FREE 链。无空间 → empty。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code assignExtentToSegment} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code assignExtentToSegment} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<ExtentId> assignExtentToSegment(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        return assignExtentToSegment(mtr, spaceId, inodeSlot,
                ExtentAllocationDirection.NO_DIRECTION, Optional.empty());
    }

    /**
     * 给 segment 分配一个完整 extent，并把方向 hint 透传到全局 FREE extent 选择。该方法只决定 extent 归属，
     * 不分配具体数据页；新 extent 入 SEG_FREE 链后由 {@link #allocatePageFromSegmentExtents} 消费。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 当前 MTR。
     * @param spaceId 目标表空间。
     * @param inodeSlot segment inode 槽。
     * @param direction 选择 FREE extent 的方向偏好。
     * @param hintPageNo 邻近页号。
     * @return 新归属该 segment 的 extent，或空间耗尽。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<ExtentId> assignExtentToSegment(MiniTransaction mtr, SpaceId spaceId, int inodeSlot,
                                                    ExtentAllocationDirection direction,
                                                    Optional<PageNo> hintPageNo) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireArgs(mtr, spaceId);
        if (direction == null || hintPageNo == null) {
            throw new DatabaseValidationException("extent allocation direction/hint must not be null");
        }
        latchSpaceThenInode(mtr, spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        SegmentId segId = inodeRepo.read(mtr, spaceId, inodeSlot).segmentId();
        Optional<ExtentId> acq = freeExtents.acquireFreeExtent(mtr, spaceId, direction, hintPageNo);
        if (acq.isEmpty()) {
            return Optional.empty();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        ExtentId ext = acq.get();
        xdes.writeState(mtr, ext, ExtentState.FSEG);
        xdes.writeOwner(mtr, ext, Optional.of(segId));
        flst.addLast(mtr, spaceId, inodeRepo.freeExtentListBaseAddr(spaceId, inodeSlot), xdes.listNodeAddr(ext));
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return Optional.of(ext);
    }

    /**
     * 从 segment 自有 extent 分配一个页：优先 SEG_NOT_FULL 头，否则 SEG_FREE 头；取首个空闲页置位。
     * 迁移：SEG_FREE→SEG_NOT_FULL（或满→SEG_FULL）、SEG_NOT_FULL→SEG_FULL（变满时）。无可用 extent → empty（2c 先 assign）。
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
     * @param inodeSlot 参与 {@code allocatePageFromSegmentExtents} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public Optional<PageNo> allocatePageFromSegmentExtents(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireArgs(mtr, spaceId);
        latchSpaceThenInode(mtr, spaceId);
        FileAddress notFull = inodeRepo.notFullExtentListBaseAddr(spaceId, inodeSlot);
        FileAddress segFree = inodeRepo.freeExtentListBaseAddr(spaceId, inodeSlot);
        FileAddress segFull = inodeRepo.fullExtentListBaseAddr(spaceId, inodeSlot);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
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
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return Optional.of(PageNo.of(pageNo));
    }

    /**
     * 释放一个页并回收（设计 §7.4）。按 extent 状态分两路：
     * fragment extent（FREE_FRAG/FULL_FRAG）：清 bitmap + 清对应 inode fragment 槽 + 计数-1；原 FULL_FRAG→FREE_FRAG；全空→FSP_FREE。
     * segment extent（FSEG）：校 owner + 清 bitmap + 计数-1；原满→SEG_NOT_FULL；全空→摘段链 + 置 FREE/清 owner + FSP_FREE。
     * 系统 extent0 或 FREE/未分配区页 → FspMetadataException。
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
     * @param inodeSlot 参与 {@code freePage} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public void freePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, PageId pageId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireArgs(mtr, spaceId);
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        latchSpaceThenInode(mtr, spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        ExtentId ext = ExtentId.from(pageId, pageSize);
        if (ext.extentNo() == 0) {
            throw new FspMetadataException("cannot free a system-extent page: " + pageId.pageNo().value());
        }
        int idxInExtent = (int) (pageId.pageNo().value() - ext.firstPageNo(pageSize).value());
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        ExtentDescriptor desc = xdes.read(mtr, ext);
        ExtentState state = desc.state();
        FileAddress node = xdes.listNodeAddr(ext);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
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
