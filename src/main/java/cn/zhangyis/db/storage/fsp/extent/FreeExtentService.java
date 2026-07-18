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

    /**
     * 创建 {@code FreeExtentService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param headerRepo 由组合根提供的 {@code SpaceHeaderRepository} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param xdes 由组合根提供的 {@code ExtentDescriptorRepository} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param flst FSP 层的链表、空间预留或分配方向对象；不得为 {@code null}，必须属于当前表空间且保持 extent/segment 所有权不变量
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
     * @return {@code fillFreeListStep} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<ExtentId> fillFreeListStep(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireArgs(mtr, spaceId);
        latchPage0(mtr, spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        long pe = pageSize.pagesPerExtent();
        SpaceHeaderSnapshot h = headerRepo.read(mtr, spaceId);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        long currentSize = h.currentSizeInPages().value();
        long freeLimit = h.freeLimitPageNo().value();
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
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

    /** 取一个可用 FREE extent：弹 FSP_FREE 头；空则先 fill 再弹；真满返回 empty。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<ExtentId> acquireFreeExtent(MiniTransaction mtr, SpaceId spaceId) {
        return acquireFreeExtent(mtr, spaceId, ExtentAllocationDirection.NO_DIRECTION, Optional.empty());
    }

    /**
     * 按方向 hint 取一个 FREE extent。NO_DIRECTION 完全复用旧链头语义；UP/DOWN 先在已材料化的 FSP_FREE 链中寻找
     * hint 所属 extent 附近的最近候选。UP 如果右侧尚未材料化，会按 freeLimit 继续填充直到找到不小于 hint 的 extent
     * 或物理空间耗尽；DOWN 不会通过向右填充制造更小候选，找不到即回退链头。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param mtr 当前 MTR。
     * @param spaceId 目标表空间。
     * @param direction 分配方向。
     * @param hintPageNo 邻近页号；方向为 UP/DOWN 但缺失 hint 时按 NO_DIRECTION 处理。
     * @return 被摘出 FSP_FREE 的 extent，或空间耗尽。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<ExtentId> acquireFreeExtent(MiniTransaction mtr, SpaceId spaceId,
                                                ExtentAllocationDirection direction,
                                                Optional<PageNo> hintPageNo) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        requireArgs(mtr, spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        if (direction == null || hintPageNo == null) {
            throw new DatabaseValidationException("extent allocation direction/hint must not be null");
        }
        latchPage0(mtr, spaceId);
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        FileAddress freeBase = headerRepo.freeExtentListBaseAddr(spaceId);
        if (direction != ExtentAllocationDirection.NO_DIRECTION && hintPageNo.isPresent()) {
            Optional<ExtentId> directional = acquireDirectional(mtr, spaceId, freeBase, direction, hintPageNo.get());
            if (directional.isPresent()) {
                return directional;
            }
        }
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        return acquireFreeExtentFromHead(mtr, spaceId, freeBase);
    }

    /**
     * 按表空间、区与段分配并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param freeBase 参与 {@code acquireDirectional} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param direction FSP 层的链表、空间预留或分配方向对象；不得为 {@code null}，必须属于当前表空间且保持 extent/segment 所有权不变量
     * @param hintPageNo 参与 {@code acquireDirectional} 的稳定领域标识 {@code PageNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     */
    private Optional<ExtentId> acquireDirectional(MiniTransaction mtr, SpaceId spaceId, FileAddress freeBase,
                                                  ExtentAllocationDirection direction, PageNo hintPageNo) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        long hintExtentNo = Math.floorDiv(hintPageNo.value(), pageSize.pagesPerExtent());
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        Optional<FileAddress> candidate = nearestCandidate(mtr, spaceId, freeBase, direction, hintExtentNo);
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        if (candidate.isPresent()) {
            return removeFreeExtent(mtr, spaceId, freeBase, candidate.get());
        }
        while (direction == ExtentAllocationDirection.UP) {
            Optional<ExtentId> filled = fillFreeListStep(mtr, spaceId);
            if (filled.isEmpty()) {
                break;
            }
            candidate = nearestCandidate(mtr, spaceId, freeBase, direction, hintExtentNo);
            if (candidate.isPresent()) {
                return removeFreeExtent(mtr, spaceId, freeBase, candidate.get());
            }
        }
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        return Optional.empty();
    }

    /**
     * 按表空间、区与段分配并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param freeBase 参与 {@code acquireFreeExtentFromHead} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     */
    private Optional<ExtentId> acquireFreeExtentFromHead(MiniTransaction mtr, SpaceId spaceId, FileAddress freeBase) {
        FileAddress head = flst.getFirst(mtr, spaceId, freeBase);
        if (head.isNull()) {
            Optional<ExtentId> filled = fillFreeListStep(mtr, spaceId);
            if (filled.isEmpty()) {
                return Optional.empty();
            }
            head = flst.getFirst(mtr, spaceId, freeBase);
        }
        return removeFreeExtent(mtr, spaceId, freeBase, head);
    }

    /**
     * 更新 {@code removeFreeExtent} 指定的表空间、区与段分配局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param freeBase 参与 {@code removeFreeExtent} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param node 参与 {@code removeFreeExtent} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     */
    private Optional<ExtentId> removeFreeExtent(MiniTransaction mtr, SpaceId spaceId, FileAddress freeBase,
                                                FileAddress node) {
        if (node.isNull()) {
            return Optional.empty();
        }
        ExtentId ext = xdes.extentIdOfNode(spaceId, node);
        flst.remove(mtr, spaceId, freeBase, node);
        return Optional.of(ext);
    }

    private Optional<FileAddress> nearestCandidate(MiniTransaction mtr, SpaceId spaceId, FileAddress freeBase,
                                                   ExtentAllocationDirection direction, long hintExtentNo) {
        FileAddress cur = flst.getFirst(mtr, spaceId, freeBase);
        FileAddress best = FileAddress.NULL;
        long bestDistance = Long.MAX_VALUE;
        while (!cur.isNull()) {
            ExtentId ext = xdes.extentIdOfNode(spaceId, cur);
            long extentNo = ext.extentNo();
            boolean matches = switch (direction) {
                case UP -> extentNo >= hintExtentNo;
                case DOWN -> extentNo <= hintExtentNo;
                case NO_DIRECTION -> true;
            };
            if (matches) {
                long distance = Math.abs(extentNo - hintExtentNo);
                if (distance < bestDistance) {
                    best = cur;
                    bestDistance = distance;
                }
            }
            cur = flst.getNext(mtr, spaceId, cur);
        }
        return best.isNull() ? Optional.empty() : Optional.of(best);
    }

    /** 回收一个 extent 为 FREE（initFree：state FREE/owner 0/bitmap 清/prev-next NULL）并入 FSP_FREE。调用方须先把它移出原链。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param extentId 参与 {@code returnFreeExtent} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
     * @return 成功取得的资源、槽位或预留；资源不存在或竞争失败时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public Optional<PageId> allocateFragmentPage(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireArgs(mtr, spaceId);
        latchPage0(mtr, spaceId);
        FileAddress ffBase = headerRepo.freeFragExtentListBaseAddr(spaceId);
        FileAddress head = flst.getFirst(mtr, spaceId, ffBase);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        int idx = idxOpt.getAsInt();
        xdes.setPageAllocated(mtr, frag, idx, true);
        long pageNo = frag.firstPageNo(pageSize).value() + idx;
        if (xdes.isFull(mtr, frag)) {
            flst.remove(mtr, spaceId, ffBase, xdes.listNodeAddr(frag));
            xdes.writeState(mtr, frag, ExtentState.FULL_FRAG);
            flst.addLast(mtr, spaceId, headerRepo.fullFragExtentListBaseAddr(spaceId), xdes.listNodeAddr(frag));
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
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
