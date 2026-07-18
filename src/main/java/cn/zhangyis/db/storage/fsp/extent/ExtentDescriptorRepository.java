package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.FspRedoDeltas;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;


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
import cn.zhangyis.db.storage.mtr.MiniTransaction;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * XDES（extent descriptor）仓储（设计 §6.3）。首版 XDES entries 内嵌 page 0；按 ExtentId.extentNo 定位 slot。
 * 物理空间账本：extent 状态 / 归属 segment / list-node 指针 / page 分配位图。读写经 page 0 latch（写 X）。
 * extent 0 是系统保留 extent，不能走普通 initFree/free-list 分配路径，也不能被普通 XDES mutator 改写。
 * extentNo 超 page 0 首批容量 → FspMetadataException（首版不支持独立 XDES 管理页）。
 *
 * <p>简化点：1 位/页 bitmap（不分 used/clean 两位）；本切片 no-redo，写页只标脏、不产 redo（设计 §15 redo 规则推迟满足），
 * 未来 redo 切片接入 {@code UPDATE_XDES} 后才能 crash-safe。
 */
public final class ExtentDescriptorRepository {

    /** 受控页来源；XDES entries 内嵌 page 0，经 MTR.getPage 拿 page 0 的 PageGuard。 */
    private final BufferPool pool;

    /** 实例级页大小；决定每 extent 有效页数（pagesPerExtent）与 page 0 首批 XDES 容量。 */
    private final PageSize pageSize;

    /**
     * 创建 {@code ExtentDescriptorRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public ExtentDescriptorRepository(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    private int entryOffset(ExtentId extentId) {
        requireExtent(extentId);
        if (extentId.extentNo() >= ExtentDescriptorLayout.maxEntriesInPage0(pageSize)) {
            throw new FspMetadataException("extent beyond first XDES region not supported: " + extentId.extentNo());
        }
        return ExtentDescriptorLayout.entryOffset(extentId.extentNo());
    }

    private PageId page0(ExtentId extentId) {
        return PageId.of(extentId.spaceId(), PageNo.of(0));
    }

    /**
     * 定位并读取表空间、区与段分配领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
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
     * @param extentId 参与 {@code read} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code read} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     */
    public ExtentDescriptor read(MiniTransaction mtr, ExtentId extentId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        int base = entryOffset(extentId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        ExtentState state = decodeState(g.readInt(base + ExtentDescriptorLayout.STATE));
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        long owner = g.readLong(base + ExtentDescriptorLayout.OWNER_SEGMENT);
        FileAddress prev = FileAddress.readFrom(g, base + ExtentDescriptorLayout.PREV);
        FileAddress next = FileAddress.readFrom(g, base + ExtentDescriptorLayout.NEXT);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return new ExtentDescriptor(extentId, state, owner, prev, next);
    }

    /** 该 extent 的 FLST 链节点起址 = page0 内 entry 的 prev 字段偏移；供 Flst/分配层把 extent 入链。
     *
     * @param extentId 参与 {@code listNodeAddr} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code listNodeAddr} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public FileAddress listNodeAddr(ExtentId extentId) {
        int base = entryOffset(extentId);
        return FileAddress.of(PageNo.of(0), base + ExtentDescriptorLayout.PREV);
    }

    /** 反向：由链节点地址还原 ExtentId。节点必在 page0、偏移须按 ENTRY_SIZE 对齐，否则视为页上链指针损坏。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param nodeAddr 参与 {@code extentIdOfNode} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code extentIdOfNode} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public ExtentId extentIdOfNode(SpaceId spaceId, FileAddress nodeAddr) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        if (nodeAddr == null || nodeAddr.isNull()) {
            throw new DatabaseValidationException("node address must be concrete");
        }
        if (nodeAddr.pageNo().value() != 0) {
            throw new FspMetadataException("xdes list node must be on page 0: " + nodeAddr);
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        int rel = nodeAddr.offset() - SpaceHeaderLayout.XDES_BASE - ExtentDescriptorLayout.PREV;
        if (rel < 0 || rel % ExtentDescriptorLayout.ENTRY_SIZE != 0) {
            throw new FspMetadataException("misaligned xdes list node offset: " + nodeAddr.offset());
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return ExtentId.of(spaceId, rel / ExtentDescriptorLayout.ENTRY_SIZE);
    }

    /** extent 内首个未分配页下标（S）；满则 empty。仅扫前 pagesPerExtent 位。
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
     * @param extentId 参与 {@code firstFreePageIndex} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code firstFreePageIndex} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public OptionalInt firstFreePageIndex(MiniTransaction mtr, ExtentId extentId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        int base = entryOffset(extentId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int pe = pageSize.pagesPerExtent();
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        byte[] bm = g.readBytes(base + ExtentDescriptorLayout.BITMAP, ExtentDescriptorLayout.BITMAP_BYTES);
        for (int i = 0; i < pe; i++) {
            if ((bm[i / 8] & (1 << (i % 8))) == 0) {
                return OptionalInt.of(i);
            }
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return OptionalInt.empty();
    }

    /** extent 内已分配页数（S），仅统计前 pagesPerExtent 位。
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
     * @param extentId 参与 {@code allocatedPageCount} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code allocatedPageCount} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    public int allocatedPageCount(MiniTransaction mtr, ExtentId extentId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        int base = entryOffset(extentId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int pe = pageSize.pagesPerExtent();
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        byte[] bm = g.readBytes(base + ExtentDescriptorLayout.BITMAP, ExtentDescriptorLayout.BITMAP_BYTES);
        int count = 0;
        for (int i = 0; i < pe; i++) {
            if ((bm[i / 8] & (1 << (i % 8))) != 0) {
                count++;
            }
        }
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return count;
    }

    /** extent 是否所有页已分配。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param extentId 参与 {@code isFull} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code isFull} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean isFull(MiniTransaction mtr, ExtentId extentId) {
        return allocatedPageCount(mtr, extentId) == pageSize.pagesPerExtent();
    }

    /** extent 是否全空。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param extentId 参与 {@code isEmpty} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code isEmpty} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean isEmpty(MiniTransaction mtr, ExtentId extentId) {
        return allocatedPageCount(mtr, extentId) == 0;
    }

    /** 重置普通 extent 为零态（FREE/无主/NULL/bitmap 清零）。extent0 系统保留，禁止普通初始化。
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
     * @param extentId 参与 {@code initFree} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    public void initFree(MiniTransaction mtr, ExtentId extentId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        if (extentId != null && extentId.extentNo() == 0) {
            throw new FspMetadataException("extent 0 is system-reserved; use reserveSystemExtent");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeStateImage(mtr, g, extentId, base, ExtentState.FREE, "initialize FREE extent state");
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        writeOwnerImage(mtr, g, extentId, base, 0L, "initialize FREE extent owner");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.PREV,
                FileAddress.NULL, ExtentDescriptorLayout.PREV, "initialize FREE extent prev");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.NEXT,
                FileAddress.NULL, ExtentDescriptorLayout.NEXT, "initialize FREE extent next");
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        FspRedoDeltas.writeBytes(mtr, g, page0(extentId), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), 0, base + ExtentDescriptorLayout.BITMAP,
                new byte[ExtentDescriptorLayout.BITMAP_BYTES], "initialize FREE extent bitmap");
    }

    /** 初始化/修复 extent0 系统保留状态：page0..3 固定管理页标记已分配，避免普通 allocator 误用。
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
    public void reserveSystemExtent(MiniTransaction mtr, SpaceId spaceId) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        ExtentId extentId = ExtentId.of(spaceId, 0);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeStateImage(mtr, g, extentId, base, ExtentState.FSEG_FRAG, "reserve system extent state");
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        writeOwnerImage(mtr, g, extentId, base, 0L, "reserve system extent owner");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.PREV,
                FileAddress.NULL, ExtentDescriptorLayout.PREV, "reserve system extent prev");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.NEXT,
                FileAddress.NULL, ExtentDescriptorLayout.NEXT, "reserve system extent next");
        FspRedoDeltas.writeBytes(mtr, g, page0(extentId), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), 0, base + ExtentDescriptorLayout.BITMAP,
                new byte[ExtentDescriptorLayout.BITMAP_BYTES], "reserve system extent bitmap");
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        for (int page = 0; page < 4; page++) {
            int byteOffset = base + ExtentDescriptorLayout.BITMAP + page / 8;
            byte b = g.readBytes(byteOffset, 1)[0];
            FspRedoDeltas.writeBytes(mtr, g, page0(extentId), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                    extentId.extentNo(), page / 8, byteOffset,
                    new byte[] {(byte) (b | (1 << (page % 8)))}, "reserve system extent allocated bit");
        }
    }

    /**
     * 校验输入与当前状态后修改表空间、区与段分配领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param extentId 参与 {@code writeState} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param state 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void writeState(MiniTransaction mtr, ExtentId extentId, ExtentState state) {
        requireMtr(mtr);
        if (state == null) {
            throw new DatabaseValidationException("extent state must not be null");
        }
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeStateImage(mtr, g, extentId, base, state, "write XDES state");
    }

    /**
     * 校验输入与当前状态后修改表空间、区与段分配领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
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
     * @param extentId 参与 {@code writeOwner} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param owner 可选的 {@code owner}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void writeOwner(MiniTransaction mtr, ExtentId extentId, Optional<SegmentId> owner) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        if (owner == null) {
            throw new DatabaseValidationException("owner optional must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        requireOrdinaryMutableExtent(extentId);
        long raw = owner.map(SegmentId::value).orElse(0L);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        if (raw == 0 && owner.isPresent()) {
            throw new DatabaseValidationException("segment id 0 is reserved as XDES owner sentinel");
        }
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        writeOwnerImage(mtr, g, extentId, base, raw, "write XDES owner");
    }

    /**
     * 校验输入与当前状态后修改表空间、区与段分配领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param extentId 参与 {@code writePrev} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param prev 参与 {@code writePrev} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void writePrev(MiniTransaction mtr, ExtentId extentId, FileAddress prev) {
        writeAddr(mtr, extentId, ExtentDescriptorLayout.PREV, prev);
    }

    /**
     * 校验输入与当前状态后修改表空间、区与段分配领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param extentId 参与 {@code writeNext} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param next 参与 {@code writeNext} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void writeNext(MiniTransaction mtr, ExtentId extentId, FileAddress next) {
        writeAddr(mtr, extentId, ExtentDescriptorLayout.NEXT, next);
    }

    /**
     * 判断 {@code isPageAllocated} 所表达的表空间、区与段分配条件；方法只读取稳定状态，并用返回值报告是否满足条件。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param extentId 参与 {@code isPageAllocated} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param pageIndexInExtent 参与 {@code isPageAllocated} 的零基位置 {@code pageIndexInExtent}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code isPageAllocated} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean isPageAllocated(MiniTransaction mtr, ExtentId extentId, int pageIndexInExtent) {
        requireMtr(mtr);
        int base = entryOffset(extentId);
        requireBitIndex(pageIndexInExtent);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.SHARED);
        byte b = g.readBytes(base + ExtentDescriptorLayout.BITMAP + pageIndexInExtent / 8, 1)[0];
        return (b & (1 << (pageIndexInExtent % 8))) != 0;
    }

    /**
     * 更新 {@code setPageAllocated} 指定的表空间、区与段分配局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
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
     * @param extentId 参与 {@code setPageAllocated} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param pageIndexInExtent 参与 {@code setPageAllocated} 的零基位置 {@code pageIndexInExtent}；必须非负且小于所属页面、集合或持久结构的容量
     * @param allocated 当前对象是否处于首次创建、首次写入或复用分支；该事实决定初始化、redo 和失败补偿路径
     */
    public void setPageAllocated(MiniTransaction mtr, ExtentId extentId, int pageIndexInExtent, boolean allocated) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        requireBitIndex(pageIndexInExtent);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        int byteOffset = base + ExtentDescriptorLayout.BITMAP + pageIndexInExtent / 8;
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        int mask = 1 << (pageIndexInExtent % 8);
        byte b = g.readBytes(byteOffset, 1)[0];
        byte nb = (byte) (allocated ? (b | mask) : (b & ~mask));
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        FspRedoDeltas.writeBytes(mtr, g, page0(extentId), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), pageIndexInExtent / 8, byteOffset, new byte[] {nb},
                "set XDES page allocation bit");
    }

    /**
     * 校验输入与当前状态后修改表空间、区与段分配领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param extentId 参与 {@code writeAddr} 的稳定领域标识 {@code ExtentId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param fieldOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param addr 参与 {@code writeAddr} 的稳定领域标识 {@code FileAddress}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void writeAddr(MiniTransaction mtr, ExtentId extentId, int fieldOffset, FileAddress addr) {
        requireMtr(mtr);
        if (addr == null) {
            throw new DatabaseValidationException("file address must not be null (use FileAddress.NULL)");
        }
        requireOrdinaryMutableExtent(extentId);
        int base = entryOffset(extentId);
        PageGuard g = mtr.getPage(pool, page0(extentId), PageLatchMode.EXCLUSIVE);
        writeAddressImage(mtr, g, extentId, base + fieldOffset, addr, fieldOffset, "write XDES list address");
    }

    private void writeStateImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int base,
                                 ExtentState state, String reason) {
        FspRedoDeltas.writeInt(mtr, guard, page0(extentId), FspMetadataDeltaKind.XDES_FIELD,
                extentId.extentNo(), ExtentDescriptorLayout.STATE,
                base + ExtentDescriptorLayout.STATE, state.ordinal(), reason);
    }

    private void writeOwnerImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int base,
                                 long owner, String reason) {
        FspRedoDeltas.writeLong(mtr, guard, page0(extentId), FspMetadataDeltaKind.XDES_FIELD,
                extentId.extentNo(), ExtentDescriptorLayout.OWNER_SEGMENT,
                base + ExtentDescriptorLayout.OWNER_SEGMENT, owner, reason);
    }

    private void writeAddressImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int offset,
                                   FileAddress address, int fieldOffset, String reason) {
        FspRedoDeltas.writeAddress(mtr, guard, page0(extentId), FspMetadataDeltaKind.XDES_FIELD,
                extentId.extentNo(), fieldOffset, offset, address, reason);
    }

    /**
     * 从稳定表示解码表空间、区与段分配领域值；先校验边界、标识与长度，损坏输入以领域异常拒绝。
     *
     * @param ordinal 参与 {@code decodeState} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code decodeState} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws FspMetadataException 空间或字典元数据不完整、越界或与权威状态冲突时抛出；调用方不得继续分配或覆盖原始元数据
     */
    private static ExtentState decodeState(int ordinal) {
        ExtentState[] values = ExtentState.values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new FspMetadataException("invalid extent state ordinal on disk: " + ordinal);
        }
        return values[ordinal];
    }

    private void requireBitIndex(int idx) {
        if (idx < 0 || idx >= pageSize.pagesPerExtent()) {
            throw new DatabaseValidationException("page index in extent out of range: " + idx);
        }
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireExtent(ExtentId extentId) {
        if (extentId == null) {
            throw new DatabaseValidationException("extent id must not be null");
        }
    }

    private static void requireOrdinaryMutableExtent(ExtentId extentId) {
        requireExtent(extentId);
        if (extentId.extentNo() == 0) {
            throw new FspMetadataException("extent 0 is system-reserved; use reserveSystemExtent");
        }
    }
}
