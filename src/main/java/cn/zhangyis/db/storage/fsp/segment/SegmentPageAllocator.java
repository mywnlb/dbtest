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

    /**
     * 本对象持有的 {@code pool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final BufferPool pool;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;
    /**
     * 本对象持有的 {@code headerRepo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SpaceHeaderRepository headerRepo;
    /**
     * 本对象持有的 {@code inodeRepo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SegmentInodeRepository inodeRepo;
    /**
     * 构造时绑定的表空间双向链表访问器 {@code flst}；所属表空间在生命周期内固定，extent/segment 分配路径依赖它保持链表链接一致。
     */
    private final Flst flst;
    /**
     * 本对象持有的 {@code segSpace} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SegmentSpaceService segSpace;
    /**
     * 本对象持有的 {@code policy} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final ExtentAllocationPolicy policy;

    /**
     * 创建 {@code SegmentPageAllocator}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
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
     * @param flst FSP 层的链表、空间预留或分配方向对象；不得为 {@code null}，必须属于当前表空间且保持 extent/segment 所有权不变量
     * @param segSpace 由组合根提供的 {@code SegmentSpaceService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param policy 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SegmentPageAllocator(BufferPool pool, PageSize pageSize, SpaceHeaderRepository headerRepo,
                                SegmentInodeRepository inodeRepo, Flst flst,
                                SegmentSpaceService segSpace, ExtentAllocationPolicy policy) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (pool == null || pageSize == null || headerRepo == null || inodeRepo == null
                || flst == null || segSpace == null || policy == null) {
            throw new DatabaseValidationException("SegmentPageAllocator dependencies must not be null");
        }
        this.pool = pool;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.pageSize = pageSize;
        this.headerRepo = headerRepo;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.inodeRepo = inodeRepo;
        this.flst = flst;
        this.segSpace = segSpace;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.policy = policy;
    }

    /**
     * 为 segment 分配一个页，仅当前 currentSize 内。fragment 槽未满走 fragment 路径，否则 extent 路径
     * （必要时按 policy 取 1..4 extent 再试）。无空间返回 empty（facade 负责 autoextend）。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code allocatePage} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<PageId> allocatePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        return allocatePage(mtr, spaceId, inodeSlot,
                ExtentAllocationDirection.NO_DIRECTION, Optional.empty(), 1L);
    }

    /**
     * 为 segment 分配一个页，并把上层提供的方向 hint 纳入 extent 批量获取策略。该入口只在 fragment 槽耗尽、
     * 且现有 segment extent 无空闲页时才使用 direction；fragment 路径和已有 extent 页分配保持原有顺序。
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
     * @param direction extent 选择方向。
     * @param hintPageNo 邻近页号。
     * @param pagesNeeded 本次上层操作预计最多需要的新页数，必须为正。
     * @return 已分配页，或当前 currentSize 内无空间。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<PageId> allocatePage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot,
                                         ExtentAllocationDirection direction, Optional<PageNo> hintPageNo,
                                         long pagesNeeded) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
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
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
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
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
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
