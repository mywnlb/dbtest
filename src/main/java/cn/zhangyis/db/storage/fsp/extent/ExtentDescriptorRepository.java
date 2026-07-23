package cn.zhangyis.db.storage.fsp.extent;

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
import cn.zhangyis.db.storage.fsp.FspRedoDeltas;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaKind;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * XDES（extent descriptor）仓储（设计 §6.3）。首批 entries 保持内嵌 page0，超出兼容容量后由
 * {@link ExtentManagementRegionLayout} 定位到独立 XDES primary/overflow 页。
 * 物理空间账本包含 extent 状态、segment owner、FLST node 与 page bitmap；所有访问先取得 per-space page0 gate，
 * 再访问实际 descriptor 页，使跨页 FLST writer 仍由同一物理临界区排斥。
 *
 * <p>教学简化：bitmap 仍为 1 位/页，不区分 InnoDB 的 free/clean 双位；写入通过既有 FSP metadata delta
 * 进入 redo。entry stride 保持 68 字节，但只消费 {@code ceil(pagesPerExtent/8)} 个有效 bitmap 字节，避免
 * 64KB 页最后一个兼容槽的无效 padding 进入 FIL trailer。</p>
 */
public final class ExtentDescriptorRepository {

    /** 受控页来源；所有 page0/独立 XDES 页都经 MTR 获取 guard。 */
    private final BufferPool pool;

    /** 实例级页大小；决定每 extent 有效页数（pagesPerExtent）与 page 0 首批 XDES 容量。 */
    private final PageSize pageSize;

    /** page0 兼容槽、独立页和管理 extent 的唯一寻址规则。 */
    private final ExtentManagementRegionLayout layout;

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
        this.layout = new ExtentManagementRegionLayout(pageSize);
    }

    /** @return 本仓储使用的不可变管理区布局，供同一 FSP 组合根注入 FLST/initializer。 */
    public ExtentManagementRegionLayout layout() {
        return layout;
    }

    private ExtentDescriptorLocation location(ExtentId extentId) {
        requireExtent(extentId);
        return layout.locate(extentId);
    }

    private PageId page0(ExtentId extentId) {
        return PageId.of(extentId.spaceId(), PageNo.of(0));
    }

    /**
     * 先取得 page0 gate，再取得并验证实际 descriptor 页。page0 自身直接复用 gate guard，避免重复 fix。
     */
    private PageGuard descriptorGuard(MiniTransaction mtr, ExtentDescriptorLocation location,
                                      PageLatchMode mode) {
        PageLatchMode gateMode = mode == PageLatchMode.EXCLUSIVE
                ? PageLatchMode.EXCLUSIVE : PageLatchMode.SHARED;
        PageGuard gate = mtr.getPage(pool, page0(location.extentId()), gateMode);
        if (location.descriptorPageId().pageNo().value() == 0L) {
            return gate;
        }
        PageGuard descriptor = mtr.getPage(pool, location.descriptorPageId(), mode);
        XdesPageCodec.readAndValidate(descriptor, location.descriptorPageId(),
                expectedStandaloneHeader(location.descriptorPageId()));
        return descriptor;
    }

    /**
     * 根据独立 XDES 页号构造必须与磁盘一致的 header；只接受 page5、区首或区首+5 三类公式位置。
     *
     * @param pageId 待验证的独立 XDES 物理 identity；SpaceId 原样进入返回 header 的诊断上下文
     * @return 与运行期管理区公式一致的角色、group base、首 extent 和 entry count
     * @throws FspMetadataException 页号不属于 XDES 公式位置或该位置没有实际 descriptor 时抛出
     */
    XdesPageHeader expectedStandaloneHeader(PageId pageId) {
        long pageNo = pageId.pageNo().value();
        long region;
        XdesPageRole role;
        long first;
        long count;
        if (pageNo == 5L) {
            region = 0L;
            role = XdesPageRole.OVERFLOW;
            first = layout.entriesPerDescriptorPage();
            count = layout.overflowEntryCount(region);
        } else if (pageNo > 0L && pageNo % pageSize.bytes() == 0L) {
            region = pageNo / pageSize.bytes();
            role = XdesPageRole.PRIMARY;
            first = layout.firstStandaloneExtent(region);
            count = layout.primaryEntryCount(region);
        } else if (pageNo > 5L && (pageNo - 5L) % pageSize.bytes() == 0L) {
            region = (pageNo - 5L) / pageSize.bytes();
            role = XdesPageRole.OVERFLOW;
            first = checkedAdd(layout.firstStandaloneExtent(region),
                    layout.entriesPerDescriptorPage(), "XDES overflow first extent");
            count = layout.overflowEntryCount(region);
        } else {
            throw new FspMetadataException("page is not a standalone XDES location: " + pageId);
        }
        if (count <= 0L || count > Integer.MAX_VALUE) {
            throw new FspMetadataException("standalone XDES page has no valid entries: " + pageId);
        }
        long groupBase = layout.primaryXdesPageNo(region).value();
        return new XdesPageHeader(role, groupBase, first, (int) count);
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
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.SHARED);
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
        return location(extentId).listNodeAddress();
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
        // 3、在中间分支复核阶段性结果；纯布局同时校验页号角色、entry 对齐和该页声明的槽位范围。
        ExtentId extentId = layout.extentIdOfNode(spaceId, nodeAddr);
        // 4、发布稳定结果；实际访问该 descriptor 时 repository 仍会验证独立页 header。
        return extentId;
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
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int pe = pageSize.pagesPerExtent();
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.SHARED);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        byte[] bm = g.readBytes(base + ExtentDescriptorLayout.BITMAP,
                ExtentDescriptorLayout.activeBitmapBytes(pageSize));
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
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int pe = pageSize.pagesPerExtent();
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.SHARED);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        byte[] bm = g.readBytes(base + ExtentDescriptorLayout.BITMAP,
                ExtentDescriptorLayout.activeBitmapBytes(pageSize));
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
        requireOrdinaryMutableExtent(extentId);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
        writeStateImage(mtr, g, extentId, base, ExtentState.FREE, "initialize FREE extent state");
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        writeOwnerImage(mtr, g, extentId, base, 0L, "initialize FREE extent owner");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.PREV,
                FileAddress.NULL, ExtentDescriptorLayout.PREV, "initialize FREE extent prev");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.NEXT,
                FileAddress.NULL, ExtentDescriptorLayout.NEXT, "initialize FREE extent next");
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        FspRedoDeltas.writeBytes(mtr, g, location.descriptorPageId(), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), 0, base + ExtentDescriptorLayout.BITMAP,
                new byte[ExtentDescriptorLayout.activeBitmapBytes(pageSize)], "initialize FREE extent bitmap");
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
        reserveSystemExtent(mtr, spaceId, 3);
    }

    /**
     * 初始化/修复 extent0 系统保留状态，并把 {@code page0..lastFixedPage} 标成已分配。普通 GENERAL/UNDO
     * 继续传 3；SpaceId 0 的 Change Buffer bootstrap 传 4，防止稳定 root 被普通 segment 复用。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR、空间 identity 与固定页上界，越界请求在读取 page0 前失败。</li>
     *     <li>以 page0 X latch 把 extent0 标成 owner=0 的 FSEG_FRAG 系统域并清理链指针。</li>
     *     <li>重建 bitmap 后逐页设置固定分配位；所有字节由同一 MTR 收集 FSP redo。</li>
     * </ol>
     *
     * @param mtr 承载 page0/XDES 修改的活动 MTR
     * @param spaceId 待初始化表空间 identity
     * @param lastFixedPage extent0 内最后一个固定页号，必须位于 3..pagesPerExtent-1
     * @throws DatabaseValidationException 参数缺失或固定页超出 extent0 时抛出
     */
    public void reserveSystemExtent(MiniTransaction mtr, SpaceId spaceId, int lastFixedPage) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        requireMtr(mtr);
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        if (lastFixedPage < 3 || lastFixedPage >= pageSize.pagesPerExtent()) {
            throw new DatabaseValidationException("last fixed system page is outside extent0: "
                    + lastFixedPage);
        }
        ExtentId extentId = ExtentId.of(spaceId, 0);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
        writeStateImage(mtr, g, extentId, base, ExtentState.FSEG_FRAG, "reserve system extent state");
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        writeOwnerImage(mtr, g, extentId, base, 0L, "reserve system extent owner");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.PREV,
                FileAddress.NULL, ExtentDescriptorLayout.PREV, "reserve system extent prev");
        writeAddressImage(mtr, g, extentId, base + ExtentDescriptorLayout.NEXT,
                FileAddress.NULL, ExtentDescriptorLayout.NEXT, "reserve system extent next");
        FspRedoDeltas.writeBytes(mtr, g, location.descriptorPageId(), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), 0, base + ExtentDescriptorLayout.BITMAP,
                new byte[ExtentDescriptorLayout.activeBitmapBytes(pageSize)], "reserve system extent bitmap");
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        for (int page = 0; page <= lastFixedPage; page++) {
            int byteOffset = base + ExtentDescriptorLayout.BITMAP + page / 8;
            byte b = g.readBytes(byteOffset, 1)[0];
            FspRedoDeltas.writeBytes(mtr, g, location.descriptorPageId(), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                    extentId.extentNo(), page / 8, byteOffset,
                    new byte[] {(byte) (b | (1 << (page % 8)))}, "reserve system extent allocated bit");
        }
    }

    /**
     * 把重复管理区的首个 extent 固定为 FSP 自有域，并只标记其中实际承载 XDES/IBUF_BITMAP 的页。
     * 该 extent 永不进入普通 FREE 或 segment 链；未标记的剩余页也不开放给普通分配器。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证目标确为管理区首 extent，并检查固定页下标属于该 extent。</li>
     *     <li>在 page0 gate 与实际 descriptor 页 X latch 下识别全零新槽或已完成的管理槽；任何旧 FREE/FSEG
     *     所有权和链指针冲突都 fail-closed，避免升级覆盖用户页。</li>
     *     <li>写入 owner=0、FSEG_FRAG、空链和清零 bitmap，再设置 primary、bitmap、可选 overflow 固定页位。</li>
     * </ol>
     *
     * @param mtr 承载管理页格式化与 XDES after-image redo 的活动 MTR
     * @param extentId 重复管理区首 extent identity
     * @param fixedPageIndexes extent 内必须永久保留的页下标，通常为 0、1 和可选 5
     * @throws FspMetadataException 目标不是管理 extent，或旧 descriptor 已被普通链/segment 使用时抛出
     * @throws DatabaseValidationException 参数缺失或固定页下标越界时抛出
     */
    public void reserveManagementExtent(MiniTransaction mtr, ExtentId extentId, int... fixedPageIndexes) {
        // 1. 管理 extent 和所有固定页必须先由纯布局证明合法，非法请求不能触碰 descriptor。
        requireMtr(mtr);
        requireExtent(extentId);
        if (!layout.isManagementExtent(extentId.extentNo())) {
            throw new FspMetadataException("extent is not a management-region owner: " + extentId.extentNo());
        }
        if (fixedPageIndexes == null || fixedPageIndexes.length == 0) {
            throw new DatabaseValidationException("management extent fixed pages must not be empty");
        }
        for (int pageIndex : fixedPageIndexes) {
            requireBitIndex(pageIndex);
        }

        // 2. 全零槽是尚未材料化的新格式；非零槽只接受已经脱离所有 FLST 的系统 owner 形状。
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        PageGuard guard = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
        requireReservableManagementImage(guard, extentId, base);

        // 3. 统一重写 canonical image，使重复执行和已完成 recovery 都收敛到相同固定页集合。
        writeStateImage(mtr, guard, extentId, base, ExtentState.FSEG_FRAG,
                "reserve management extent state");
        writeOwnerImage(mtr, guard, extentId, base, 0L, "reserve management extent owner");
        writeAddressImage(mtr, guard, extentId, base + ExtentDescriptorLayout.PREV,
                FileAddress.NULL, ExtentDescriptorLayout.PREV, "reserve management extent prev");
        writeAddressImage(mtr, guard, extentId, base + ExtentDescriptorLayout.NEXT,
                FileAddress.NULL, ExtentDescriptorLayout.NEXT, "reserve management extent next");
        FspRedoDeltas.writeBytes(mtr, guard, location.descriptorPageId(), FspMetadataDeltaKind.XDES_BITMAP_BYTE,
                extentId.extentNo(), 0, base + ExtentDescriptorLayout.BITMAP,
                new byte[ExtentDescriptorLayout.activeBitmapBytes(pageSize)],
                "reserve management extent bitmap");
        for (int pageIndex : fixedPageIndexes) {
            writeAllocatedBit(mtr, guard, location, base, pageIndex, true,
                    "reserve management metadata page");
        }
    }

    /**
     * 在写任何重复管理页前预检旧 descriptor 是否可安全转为系统保留。该入口服务 page0 仍承载 descriptor、
     * 而 primary/bitmap 位于远端的 4KB/8KB 兼容布局，避免最后才发现旧 owner 冲突而留下不可回滚的半格式页。
     *
     * @param mtr 已持 page0 gate 的管理区初始化 MTR
     * @param extentId 待保留的管理区首 extent
     * @throws FspMetadataException descriptor 已进入普通 FREE/segment 链或被其它 owner 占用时抛出
     */
    public void requireManagementExtentReservable(MiniTransaction mtr, ExtentId extentId) {
        requireMtr(mtr);
        requireExtent(extentId);
        if (!layout.isManagementExtent(extentId.extentNo())) {
            throw new FspMetadataException("extent is not a management-region owner: " + extentId.extentNo());
        }
        ExtentDescriptorLocation location = location(extentId);
        PageGuard guard = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
        requireReservableManagementImage(guard, extentId, location.entryOffset());
    }

    /** 接受全零新槽或已完成 canonical 管理槽，拒绝所有可能属于 legacy 业务分配的证据。 */
    private static void requireReservableManagementImage(PageGuard guard, ExtentId extentId, int base) {
        byte[] raw = guard.readBytes(base, ExtentDescriptorLayout.ENTRY_SIZE);
        boolean blank = true;
        for (byte value : raw) {
            if (value != 0) {
                blank = false;
                break;
            }
        }
        if (!blank) {
            ExtentState state = decodeState(guard.readInt(base + ExtentDescriptorLayout.STATE));
            long owner = guard.readLong(base + ExtentDescriptorLayout.OWNER_SEGMENT);
            FileAddress prev = FileAddress.readFrom(guard, base + ExtentDescriptorLayout.PREV);
            FileAddress next = FileAddress.readFrom(guard, base + ExtentDescriptorLayout.NEXT);
            if (state != ExtentState.FSEG_FRAG || owner != 0L || !prev.isNull() || !next.isNull()) {
                throw new FspMetadataException("management extent conflicts with legacy allocation; "
                        + "offline rebuild is required: " + extentId);
            }
        }
    }

    /**
     * 在已经保留的管理 extent 中追加一个固定页位。group0 的 overflow XDES page5 在旧 page0 兼容槽耗尽时
     * 才出现，因此不能在 tablespace bootstrap 时无条件覆盖 page5。
     *
     * @param mtr 承载 bitmap delta 的活动 MTR
     * @param pageId 新格式化管理页的物理 identity
     * @throws FspMetadataException 所属管理 extent 未处于 owner=0/FSEG_FRAG canonical 状态时抛出
     */
    public void markManagementPageAllocated(MiniTransaction mtr, PageId pageId) {
        requireMtr(mtr);
        if (pageId == null) {
            throw new DatabaseValidationException("management page id must not be null");
        }
        long extentNo = pageId.pageNo().value() / pageSize.pagesPerExtent();
        int pageIndex = (int) (pageId.pageNo().value() % pageSize.pagesPerExtent());
        if (!layout.isManagementExtent(extentNo)) {
            throw new FspMetadataException("page does not belong to a management extent: " + pageId);
        }
        ExtentId extentId = ExtentId.of(pageId.spaceId(), extentNo);
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        PageGuard guard = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
        ExtentState state = decodeState(guard.readInt(base + ExtentDescriptorLayout.STATE));
        long owner = guard.readLong(base + ExtentDescriptorLayout.OWNER_SEGMENT);
        if (state != ExtentState.FSEG_FRAG || owner != 0L) {
            throw new FspMetadataException("management extent is not reserved before fixed-page update: "
                    + extentId);
        }
        writeAllocatedBit(mtr, guard, location, base, pageIndex, true,
                "reserve delayed management metadata page");
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
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
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
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
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
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        requireBitIndex(pageIndexInExtent);
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.SHARED);
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
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        requireBitIndex(pageIndexInExtent);
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        writeAllocatedBit(mtr, g, location, base, pageIndexInExtent, allocated,
                "set XDES page allocation bit");
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
    }

    /** 在调用方已持实际 descriptor X latch 时执行单 bitmap byte 的原子 after-image 写入。 */
    private static void writeAllocatedBit(MiniTransaction mtr, PageGuard guard,
                                          ExtentDescriptorLocation location, int base,
                                          int pageIndexInExtent, boolean allocated, String reason) {
        int byteOffset = base + ExtentDescriptorLayout.BITMAP + pageIndexInExtent / 8;
        int mask = 1 << (pageIndexInExtent % 8);
        byte before = guard.readBytes(byteOffset, 1)[0];
        byte after = (byte) (allocated ? before | mask : before & ~mask);
        FspRedoDeltas.writeBytes(mtr, guard, location.descriptorPageId(),
                FspMetadataDeltaKind.XDES_BITMAP_BYTE, location.extentId().extentNo(),
                pageIndexInExtent / 8, byteOffset, new byte[] {after}, reason);
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
        ExtentDescriptorLocation location = location(extentId);
        int base = location.entryOffset();
        PageGuard g = descriptorGuard(mtr, location, PageLatchMode.EXCLUSIVE);
        writeAddressImage(mtr, g, extentId, base + fieldOffset, addr, fieldOffset, "write XDES list address");
    }

    private void writeStateImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int base,
                                 ExtentState state, String reason) {
        FspRedoDeltas.writeInt(mtr, guard, location(extentId).descriptorPageId(), FspMetadataDeltaKind.XDES_FIELD,
                extentId.extentNo(), ExtentDescriptorLayout.STATE,
                base + ExtentDescriptorLayout.STATE, state.ordinal(), reason);
    }

    private void writeOwnerImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int base,
                                 long owner, String reason) {
        FspRedoDeltas.writeLong(mtr, guard, location(extentId).descriptorPageId(), FspMetadataDeltaKind.XDES_FIELD,
                extentId.extentNo(), ExtentDescriptorLayout.OWNER_SEGMENT,
                base + ExtentDescriptorLayout.OWNER_SEGMENT, owner, reason);
    }

    private void writeAddressImage(MiniTransaction mtr, PageGuard guard, ExtentId extentId, int offset,
                                   FileAddress address, int fieldOffset, String reason) {
        FspRedoDeltas.writeAddress(mtr, guard, location(extentId).descriptorPageId(), FspMetadataDeltaKind.XDES_FIELD,
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

    private void requireOrdinaryMutableExtent(ExtentId extentId) {
        requireExtent(extentId);
        if (layout.isManagementExtent(extentId.extentNo())) {
            throw new FspMetadataException("management extent is reserved and cannot use ordinary XDES mutator: "
                    + extentId.extentNo());
        }
    }

    /** 以项目领域异常报告持久地址算术溢出，禁止把裸 ArithmeticException 泄漏给 FSP 调用方。 */
    private static long checkedAdd(long left, long right, String field) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException error) {
            throw new FspMetadataException(field + " overflows", error);
        }
    }
}
