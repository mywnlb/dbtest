package cn.zhangyis.db.storage.fsp.segment;
import cn.zhangyis.db.storage.fsp.FspRedoDeltas;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;

/**
 * SegmentInode（page 2）仓储（设计 §6.4）。逻辑 segment 归属：分配/释放 inode 槽、读写 purpose/计数/extent list base/fragment 槽。
 * 首版单个 inode 页 page 2。三个 extent list 头为 FLST base，由 {@link Flst} 维护——repo 负责 allocateSlot/read 整 base
 * 与暴露 base 地址访问器。allocateSlot / requireFreeFragmentSlot 为查找型、非幂等。
 *
 * <p>简化点：单 inode 页 page 2；本切片 no-redo，写页只标脏、不产 redo，不声明 crash-safe（§15 推迟满足）。
 */
public final class SegmentInodeRepository {

    /** 受控页来源；inode entries 在 page 2，经 MTR.getPage 拿 page 2 的 PageGuard。 */
    private final BufferPool pool;

    /** 实例级页大小；决定 page 2 可容纳的 inode 槽数（maxInodesInPage）。 */
    private final PageSize pageSize;

    /**
     * 创建 {@code SegmentInodeRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SegmentInodeRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    private static PageId inodePage(SpaceId spaceId) {
        return PageId.of(spaceId, PageNo.of(2));
    }

    /** 扫首个 used==0 槽，写入 inode（used=1/segmentId/purpose、counts=0、三 list=EMPTY base、32 fragment 槽=0），返回槽下标。无空槽抛异常。
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
     * @param segmentId 参与 {@code allocateSlot} 的稳定领域标识 {@code SegmentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param purpose 选择 {@code allocateSlot} 分支的 {@code SegmentPurpose} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code allocateSlot} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public int allocateSlot(MiniTransaction mtr, SpaceId spaceId, SegmentId segmentId, SegmentPurpose purpose) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        if (segmentId == null || purpose == null) {
            throw new DatabaseValidationException("segmentId/purpose must not be null");
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("segment id 0 is reserved as empty owner sentinel");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        long max = SegmentInodeLayout.maxInodesInPage(pageSize);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        for (int slot = 0; slot < max; slot++) {
            int base = SegmentInodeLayout.slotOffset(slot);
            if (g.readInt(base + SegmentInodeLayout.USED) == 0) {
                int slotIndex = slot;
                int slotBase = base;
                FspRedoDeltas.withFspCategory(mtr, "allocate segment inode slot", () -> {
                    g.writeInt(slotBase + SegmentInodeLayout.USED, 1);
                    g.writeLong(slotBase + SegmentInodeLayout.SEGMENT_ID, segmentId.value());
                    g.writeInt(slotBase + SegmentInodeLayout.PURPOSE, purpose.ordinal());
                    g.writeLong(slotBase + SegmentInodeLayout.USED_PAGE_COUNT, 0L);
                    g.writeLong(slotBase + SegmentInodeLayout.RESERVED_PAGE_COUNT, 0L);
                    FlstBase.EMPTY.writeTo(g, slotBase + SegmentInodeLayout.FREE_EXTENT_LIST_BASE);
                    FlstBase.EMPTY.writeTo(g, slotBase + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE);
                    FlstBase.EMPTY.writeTo(g, slotBase + SegmentInodeLayout.FULL_EXTENT_LIST_BASE);
                    for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
                        g.writeLong(SegmentInodeLayout.fragmentSlotOffset(slotIndex, f), 0L);
                    }
                });
                FspRedoDeltas.recordAfterImage(mtr, g, inodePage(spaceId), FspMetadataDeltaKind.INODE_SLOT_IMAGE,
                        slotIndex, 0, slotBase, SegmentInodeLayout.ENTRY_SIZE, "allocate segment inode slot image");
                return slot;
            }
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        throw new FspMetadataException("no free inode slot on page 2 (max " + max + ")");
    }

    /**
     * 定位并读取表空间、区与段分配领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code read} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code read} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    public SegmentInode read(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        return read(mtr, spaceId, inodeSlot, PageLatchMode.SHARED);
    }

    /**
     * 为马上会修改 segment 账本的调用方读取 inode，并从一开始持有 X latch，避免同一 MTR 随后分配/释放时
     * 在 page2 上做不允许的 S→X 升级。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code readExclusive} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code readExclusive} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    public SegmentInode readExclusive(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        return read(mtr, spaceId, inodeSlot, PageLatchMode.EXCLUSIVE);
    }

    /**
     * 定位并读取表空间、区与段分配领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code read} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code read} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    private SegmentInode read(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, PageLatchMode mode) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), mode);
        if (g.readInt(base + SegmentInodeLayout.USED) == 0) {
            throw new FspMetadataException("inode slot is free: " + inodeSlot);
        }
        return new SegmentInode(
                inodeSlot,
                SegmentId.of(g.readLong(base + SegmentInodeLayout.SEGMENT_ID)),
                decodePurpose(g.readInt(base + SegmentInodeLayout.PURPOSE)),
                g.readLong(base + SegmentInodeLayout.USED_PAGE_COUNT),
                g.readLong(base + SegmentInodeLayout.RESERVED_PAGE_COUNT),
                FlstBase.readFrom(g, base + SegmentInodeLayout.FREE_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, base + SegmentInodeLayout.FULL_EXTENT_LIST_BASE));
    }

    /**
     * 释放本方法拥有的表空间、区与段分配资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code freeSlot} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     */
    public void freeSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        FspRedoDeltas.writeBytes(mtr, g, inodePage(spaceId), FspMetadataDeltaKind.INODE_SLOT_IMAGE,
                inodeSlot, 0, base, new byte[SegmentInodeLayout.ENTRY_SIZE], "free segment inode slot");
    }

    /**
     * 更新 {@code setUsedPageCount} 指定的表空间、区与段分配局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code setUsedPageCount} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param value 由 {@code setUsedPageCount} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    public void setUsedPageCount(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, long value) {
        writeLongField(mtr, spaceId, inodeSlot, SegmentInodeLayout.USED_PAGE_COUNT, value);
    }

    /**
     * 更新 {@code setReservedPageCount} 指定的表空间、区与段分配局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code setReservedPageCount} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param value 由 {@code setReservedPageCount} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    public void setReservedPageCount(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, long value) {
        writeLongField(mtr, spaceId, inodeSlot, SegmentInodeLayout.RESERVED_PAGE_COUNT, value);
    }

    /** SEG_FREE 链 base 地址（page2 内 inode 槽偏移），供 Flst/2b 维护链。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code freeExtentListBaseAddr} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code freeExtentListBaseAddr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress freeExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.FREE_EXTENT_LIST_BASE);
    }

    /** SEG_NOT_FULL 链 base 地址。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code notFullExtentListBaseAddr} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code notFullExtentListBaseAddr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress notFullExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.NOT_FULL_EXTENT_LIST_BASE);
    }

    /** SEG_FULL 链 base 地址。
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code fullExtentListBaseAddr} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code fullExtentListBaseAddr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress fullExtentListBaseAddr(SpaceId spaceId, int inodeSlot) {
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        return FileAddress.of(PageNo.of(2), base + SegmentInodeLayout.FULL_EXTENT_LIST_BASE);
    }

    /** fragment 槽：值 0 → 空。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code getFragmentPage} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param fragIdx 参与 {@code getFragmentPage} 的原始数值身份 {@code fragIdx}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @return {@code getFragmentPage} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<PageNo> getFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fragIdx) {
        requireMtr(mtr);
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        requireFrag(fragIdx);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        long raw = g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, fragIdx));
        return raw == 0 ? Optional.empty() : Optional.of(PageNo.of(raw));
    }

    /**
     * 更新 {@code setFragmentPage} 指定的表空间、区与段分配局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
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
     * @param inodeSlot 参与 {@code setFragmentPage} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param fragIdx 参与 {@code setFragmentPage} 的原始数值身份 {@code fragIdx}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param pageNo 可选的 {@code pageNo}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void setFragmentPage(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fragIdx, Optional<PageNo> pageNo) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireSpace(spaceId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        requireSlot(spaceId, inodeSlot);
        requireFrag(fragIdx);
        if (pageNo == null) {
            throw new DatabaseValidationException("fragment pageNo optional must not be null");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        long raw = pageNo.map(PageNo::value).orElse(0L);
        if (raw == 0 && pageNo.isPresent()) {
            throw new DatabaseValidationException("page 0 is reserved as empty fragment sentinel");
        }
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        FspRedoDeltas.writeLong(mtr, g, inodePage(spaceId), FspMetadataDeltaKind.INODE_FRAGMENT_SLOT,
                inodeSlot, fragIdx, SegmentInodeLayout.fragmentSlotOffset(inodeSlot, fragIdx), raw,
                "set inode fragment slot");
    }

    /** 返回首个空（值为 0）fragment 槽下标；满则抛 FspMetadataException。
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
     * @param inodeSlot 参与 {@code requireFreeFragmentSlot} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code requireFreeFragmentSlot} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public int requireFreeFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
            if (g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, f)) == 0) {
                return f;
            }
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        throw new FspMetadataException("no free fragment slot in inode slot: " + inodeSlot);
    }

    /** 是否存在空 fragment 槽（值为 0），即该 segment 已用 fragment 页 &lt; 32。供分配层做 fragment vs extent 决策（S）。
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
     * @param inodeSlot 参与 {@code hasFreeFragmentSlot} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code hasFreeFragmentSlot} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean hasFreeFragmentSlot(MiniTransaction mtr, SpaceId spaceId, int inodeSlot) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        requireSpace(spaceId);
        requireSlot(spaceId, inodeSlot);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        for (int f = 0; f < SegmentInodeLayout.FRAGMENT_SLOT_COUNT; f++) {
            if (g.readLong(SegmentInodeLayout.fragmentSlotOffset(inodeSlot, f)) == 0) {
                return true;
            }
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return false;
    }

    /**
     * 扫描单 inode 页是否仍有已分配槽。undo truncate 只在所有 segment 已由 purge/dropSegment 回收后允许；
     * 该检查在 lifecycle X lease 内以 page2 S latch 执行，结果不会与新的 segment 分配交叉。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 当前维护 MTR。
     * @param spaceId undo 表空间。
     * @return 任一 USED!=0 返回 true。
     */
    public boolean hasAllocatedSlots(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.SHARED);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        long max = SegmentInodeLayout.maxInodesInPage(pageSize);
        for (int slot = 0; slot < max; slot++) {
            if (g.readInt(SegmentInodeLayout.slotOffset(slot) + SegmentInodeLayout.USED) != 0) {
                return true;
            }
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return false;
    }

    /**
     * 校验输入与当前状态后修改表空间、区与段分配领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param inodeSlot 参与 {@code writeLongField} 的零基位置 {@code inodeSlot}；必须非负且小于所属页面、集合或持久结构的容量
     * @param fieldOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param value 由 {@code writeLongField} 转换或编码的原始 {@code long} 值；超出目标值对象或持久格式范围时以领域异常拒绝
     */
    private void writeLongField(MiniTransaction mtr, SpaceId spaceId, int inodeSlot, int fieldOffset, long value) {
        requireMtr(mtr);
        requireSpace(spaceId);
        int base = requireSlot(spaceId, inodeSlot);
        PageGuard g = mtr.getPage(pool, inodePage(spaceId), PageLatchMode.EXCLUSIVE);
        FspRedoDeltas.writeLong(mtr, g, inodePage(spaceId), FspMetadataDeltaKind.INODE_FIELD,
                inodeSlot, fieldOffset, base + fieldOffset, value, "write inode long field");
    }

    /**
     * 从稳定表示解码表空间、区与段分配领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * @param ordinal 参与 {@code decodePurpose} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code decodePurpose} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    private static SegmentPurpose decodePurpose(int ordinal) {
        SegmentPurpose[] values = SegmentPurpose.values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new FspMetadataException("invalid segment purpose ordinal on disk: " + ordinal);
        }
        return values[ordinal];
    }

    private int requireSlot(SpaceId spaceId, int inodeSlot) {
        if (inodeSlot < 0 || inodeSlot >= SegmentInodeLayout.maxInodesInPage(pageSize)) {
            throw new DatabaseValidationException("inode slot out of range: " + inodeSlot);
        }
        return SegmentInodeLayout.slotOffset(inodeSlot);
    }

    private static void requireFrag(int fragIdx) {
        if (fragIdx < 0 || fragIdx >= SegmentInodeLayout.FRAGMENT_SLOT_COUNT) {
            throw new DatabaseValidationException("fragment index out of range: " + fragIdx);
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
}
