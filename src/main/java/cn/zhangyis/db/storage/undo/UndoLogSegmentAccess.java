package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 跨页 undo log segment 的 MTR 生产入口。它只依赖 {@link UndoSpaceAllocator} 端口进行页分配，
 * 不反向依赖 storage.api 的 DiskSpaceManager/SegmentRef。返回的 {@link UndoLogSegment} 由 MTR memo
 * 持有页 guard，调用方不要自行 close。
 */
public final class UndoLogSegmentAccess {

    /**
     * 页大小透传给 UndoPage/UndoLogSegment，用于页内容量和生长 preflight。
     */
    private final PageSize pageSize;

    /**
     * undo 页分配端口，隐藏底层 FSP/DiskSpaceManager 细节。
     */
    private final UndoSpaceAllocator allocator;

    /**
     * record codec。Access 创建一次后复用，段句柄共享该无状态编解码器。
     */
    private final UndoRecordCodec codec;

    /**
     * undo page 访问器，负责 first/chain 页格式化和页类型守门。
     */
    private final UndoPageAccess pageAccess;
    /** external payload 页链读写设施。 */
    private final UndoPayloadStorage payloadStorage;
    /** 所有 record 槽统一解码入口。 */
    private final UndoStoredRecordResolver storedRecordResolver;
    /** 单条 external undo payload 的实例安全上限。 */
    private final int maxExternalPages;

    /**
     * 创建 {@code UndoLogSegmentAccess}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param allocator 由组合根提供的 {@code UndoSpaceAllocator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    public UndoLogSegmentAccess(BufferPool pool, PageSize pageSize, UndoSpaceAllocator allocator,
                                TypeCodecRegistry registry) {
        this(pool, pageSize, allocator, registry, null, 16);
    }

    /**
     * 创建带运行时表空间状态准入的 undo segment access。生产组合根注入共享 {@link TablespaceRegistry}，
     * 使所有 undo first/chain 页打开和新建都先经 operation lease + registry require；低层测试仍可用四参构造
     * 专注页格式和链路行为。
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param allocator 由组合根提供的 {@code UndoSpaceAllocator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param tablespaceRegistry 由组合根提供的 {@code TablespaceRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    public UndoLogSegmentAccess(BufferPool pool, PageSize pageSize, UndoSpaceAllocator allocator,
                                TypeCodecRegistry registry, TablespaceRegistry tablespaceRegistry) {
        this(pool, pageSize, allocator, registry, tablespaceRegistry, 16);
    }

    /** 创建带 external undo payload 页数上限的生产 access。
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
     * @param allocator 由组合根提供的 {@code UndoSpaceAllocator} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param tablespaceRegistry 由组合根提供的 {@code TablespaceRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param maxExternalPages 参与 {@code 构造} 的上界或规格值 {@code maxExternalPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoLogSegmentAccess(BufferPool pool, PageSize pageSize, UndoSpaceAllocator allocator,
                                TypeCodecRegistry registry, TablespaceRegistry tablespaceRegistry,
                                int maxExternalPages) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (pool == null || pageSize == null || allocator == null || registry == null) {
            throw new DatabaseValidationException("undo log segment access args must not be null");
        }
        if (maxExternalPages <= 0) {
            throw new DatabaseValidationException("max external undo pages must be positive: " + maxExternalPages);
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.pageSize = pageSize;
        this.allocator = allocator;
        this.codec = new UndoRecordCodec(registry);
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.pageAccess = new UndoPageAccess(pool, pageSize, tablespaceRegistry);
        this.payloadStorage = new UndoPayloadStorage(pool, pageSize, tablespaceRegistry);
        this.storedRecordResolver = new UndoStoredRecordResolver(codec, payloadStorage, maxExternalPages);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.maxExternalPages = maxExternalPages;
    }

    /**
     * 创建一条新的 INSERT 或 UPDATE undo log segment。数据流：分配端口建 UNDO segment 和首页（此时页类型为
     * ALLOCATED）→ 同一 MTR 内把首页重初始化为 UNDO first 页 → 返回 EXCLUSIVE 段句柄，current=first。
     *
     * <p>同页两次 newPage（ALLOCATED→UNDO）复用 T1.3a 的物理 redo 顺序，最终页类型由后一条 PAGE_INIT/bytes
     * 决定，本片不新增 redo 类型。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param undoSpace 参与 {@code create} 的稳定领域标识 {@code SpaceId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param txnId 参与 {@code create} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param kind 选择 {@code create} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code create} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoLogSegment create(MiniTransaction mtr, SpaceId undoSpace, TransactionId txnId,
                                 UndoLogKind kind) {
        if (mtr == null || undoSpace == null || txnId == null || kind == null) {
            throw new DatabaseValidationException("undo log segment create args must not be null");
        }
        if (kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("temporary undo log is not supported by ordinary undo tablespace");
        }
        UndoSegmentHandle handle = allocator.createUndoSegment(mtr, undoSpace);
        UndoPage firstPage = pageAccess.createFirstPage(mtr, handle.firstPageId(), kind, txnId, handle);
        return new UndoLogSegment(mtr, pageSize, allocator, codec, pageAccess, payloadStorage,
                storedRecordResolver, maxExternalPages, handle, firstPage, firstPage, PageLatchMode.EXCLUSIVE);
    }

    /**
     * 重开一条已有 undo log segment。数据流：按调用方请求模式打开 first 页 → 校验它确实是 log first 页 →
     * 从 first 页 page/log header 重建 {@link UndoSegmentHandle}。SHARED 会话只读遍历，仅持 first 页 S；
     * EXCLUSIVE 会话用于续 append，若尾页不是 first 页，会额外以 X 打开尾页，并校验尾页 segmentId/inodeSlot
     * 与 first header 重建的 handle 一致，防止损坏的 LAST_PAGE_NO 把其它 segment 页当作当前尾页继续写。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr         当前物理短事务。
     * @param firstPageId undo log first 页 id，不能传 chain 页。
     * @param mode        SHARED=只读，EXCLUSIVE=续写。
     * @return MTR 内有效的 undo log segment 句柄。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoLogSegment open(MiniTransaction mtr, PageId firstPageId, PageLatchMode mode) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || firstPageId == null || mode == null) {
            throw new DatabaseValidationException("undo log segment open args must not be null");
        }
        UndoPage firstPage = pageAccess.openUndoPage(mtr, firstPageId, mode);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (!firstPage.isFirstPage()) {
            throw new UndoLogFormatException("page " + firstPageId + " is not the undo log first page");
        }
        long lastPageNoVal = firstPage.lastPageNo();
        PageId lastPageId = PageId.of(firstPageId.spaceId(), PageNo.of(lastPageNoVal));
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoSegmentHandle handle = new UndoSegmentHandle(firstPageId.spaceId(), firstPage.inodeSlot(),
                firstPage.segmentId(), firstPageId, lastPageId);
        UndoPage current = firstPage;
        if (mode == PageLatchMode.EXCLUSIVE && lastPageNoVal != firstPageId.pageNo().value()) {
            UndoPage last = pageAccess.openUndoPage(mtr, lastPageId, PageLatchMode.EXCLUSIVE);
            if (!last.segmentId().equals(handle.segmentId()) || last.inodeSlot() != handle.inodeSlot()) {
                throw new UndoLogFormatException("undo last page " + lastPageId + " segment mismatch with "
                        + handle.segmentId().value() + "/slot " + handle.inodeSlot());
            }
            if (last.undoKind() != firstPage.undoKind()) {
                throw new UndoLogFormatException("undo last page " + lastPageId + " kind " + last.undoKind()
                        + " differs from first page " + firstPage.undoKind());
            }
            current = last;
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new UndoLogSegment(mtr, pageSize, allocator, codec, pageAccess, payloadStorage,
                storedRecordResolver, maxExternalPages, handle, firstPage, current, mode);
    }

    /**
     * 恢复/规划辅助：只读打开 page3 cached first page，验证它是空单页 owner，并复制可跨 MTR 使用的 handle。
     * FSP used-page/extent 账本由调用方在独立只读 MTR 中继续交叉校验，避免 first-page latch 与 page2 逆序。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param expectedKind 选择 {@code inspectCached} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code inspectCached} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public CachedUndoSegmentRef inspectCached(MiniTransaction mtr, PageId firstPageId, UndoLogKind expectedKind) {
        if (mtr == null || firstPageId == null || expectedKind == null
                || expectedKind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("cached undo inspection args are invalid");
        }
        UndoLogSegment segment = open(mtr, firstPageId, PageLatchMode.SHARED);
        segment.requireCached(expectedKind);
        return new CachedUndoSegmentRef(expectedKind, segment.handle());
    }

    /**
     * recovery/规划辅助：只读验证 page3 free FIFO 节点是空单页 owner，并读取状态复用的 prev/next 链接。
     * FSP fragment/extent 资格由调用方在独立 MTR 中交叉验证。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code inspectFree} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoFreeListNodeSnapshot inspectFree(MiniTransaction mtr, PageId firstPageId) {
        if (mtr == null || firstPageId == null) {
            throw new DatabaseValidationException("free undo inspection args must not be null");
        }
        UndoLogSegment segment = open(mtr, firstPageId, PageLatchMode.SHARED);
        segment.requireFree();
        UndoPage page = pageAccess.openUndoPage(mtr, firstPageId, PageLatchMode.SHARED);
        return new UndoFreeListNodeSnapshot(new FreeUndoSegmentRef(segment.handle()), page.undoKind(),
                freeLink(firstPageId, page.freePrevPageNo()), freeLink(firstPageId, page.freeNextPageNo()));
    }

    /**
     * 只读一张 first page 的 history/recovery header，不跟随 LAST_PAGE_NO 打开段尾。该入口用于持久 history 遍历，
     * 保证每个节点都在独立短 MTR 内读取，不把整条跨事务链的 page fix 同时压在 buffer pool 上。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code inspectHistoryNode} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoHistoryNodeSnapshot inspectHistoryNode(MiniTransaction mtr, PageId firstPageId) {
        if (mtr == null || firstPageId == null) {
            throw new DatabaseValidationException("undo history node inspection args must not be null");
        }
        UndoPage page = pageAccess.openUndoPage(mtr, firstPageId, PageLatchMode.SHARED);
        if (!page.isFirstPage() || page.firstPageNo() != firstPageId.pageNo().value()) {
            throw new UndoLogFormatException("history node is not an undo first page: " + firstPageId);
        }
        long lastPageNo = page.lastPageNo();
        UndoSegmentHandle handle = new UndoSegmentHandle(firstPageId.spaceId(), page.inodeSlot(), page.segmentId(),
                firstPageId, PageId.of(firstPageId.spaceId(), PageNo.of(lastPageNo)));
        return new UndoHistoryNodeSnapshot(firstPageId, handle, page.undoKind(),
                UndoLogState.fromPhysical(page.state()), page.transactionId(),
                TransactionNo.of(page.commitNo()), historyLink(firstPageId, page.historyPrevPageNo()),
                historyLink(firstPageId, page.historyNextPageNo()), page.logicalHead());
    }

    /**
     * 在一个 phase-one MTR 中原子标记同一事务的全部普通 undo first page。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验目标非空、PageId 唯一，并按 PageId 稳定排序后固定全部 first-page X latch。</li>
     *     <li>先全量核对 creator、kind、ACTIVE、NONE commit no和空 history links；任一冲突时尚未写页。</li>
     *     <li>全部证据闭合后逐页写 PREPARED state；page3 owner与 logical head保持不变。</li>
     * </ol>
     *
     * @param mtr phase-one 写 MTR；调用方负责在同一批次追加 transaction-state prepare redo并提交
     * @param creatorTransactionId 全部目标共享的已分配 write transaction id
     * @param targets 同一事务的一或两条 INSERT/UPDATE first-page 目标
     * @throws DatabaseValidationException 输入缺失、重复页或临时 undo target 时抛出，页面不改变
     * @throws UndoLogFormatException first-page identity/state与冻结目标不一致时抛出，页面不改变
     */
    public void markPreparedFirstPages(MiniTransaction mtr, TransactionId creatorTransactionId,
                                       List<UndoPrepareTarget> targets) {
        // 1、批次不能为空且不得重复；排序由 first-page-only helper 统一完成。
        if (mtr == null || creatorTransactionId == null || creatorTransactionId.isNone()
                || targets == null || targets.isEmpty()
                || targets.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("undo prepare batch arguments are invalid");
        }
        List<PageId> pageIds = targets.stream().map(UndoPrepareTarget::firstPageId).toList();
        if (pageIds.stream().distinct().count() != pageIds.size()) {
            throw new DatabaseValidationException("undo prepare first pages must be distinct");
        }
        Map<PageId, UndoPage> pages = openFirstPagesSorted(mtr, pageIds);

        // 2、写前完成全量校验，防止 mixed transaction 只留下一个 PREPARED first page。
        for (UndoPrepareTarget target : targets) {
            UndoPage page = pages.get(target.firstPageId());
            requireHistoryFirstPage(page, target.firstPageId());
            if (page.state() != UndoPageLayout.STATE_ACTIVE
                    || page.undoKind() != target.kind()
                    || !page.transactionId().equals(creatorTransactionId)
                    || page.commitNo() != 0L
                    || page.historyPrevPageNo() != FilePageHeader.FIL_NULL
                    || page.historyNextPageNo() != FilePageHeader.FIL_NULL) {
                throw new UndoLogFormatException(
                        "undo prepare target identity/state changed: " + target.firstPageId());
            }
        }

        // 3、同一 MTR 内只改稳定 state byte；logical head和 page3 owner继续为 phase-two 提供权威入口。
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        for (UndoPrepareTarget target : targets) {
            pages.get(target.firstPageId()).setLogState(UndoPageLayout.STATE_PREPARED);
        }
    }

    /**
     * commit 最终 MTR 的 first-page-only 批处理：按 pageNo 升序获取旧 tail 与新 UPDATE 首页，避免普通
     * {@link #open} 因跟随新节点 LAST_PAGE_NO 而越过另一 first page。全部状态校验先完成，之后才写双向链接和提交头。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param oldTailPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param newTailPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param creatorTransactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param transactionNo 参与 {@code appendHistoryNode} 的稳定领域标识 {@code TransactionNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    public void appendHistoryNode(MiniTransaction mtr, Optional<PageId> oldTailPageId,
                                  PageId newTailPageId, TransactionId creatorTransactionId,
                                  TransactionNo transactionNo) {
        appendHistoryNode(mtr, oldTailPageId, newTailPageId, creatorTransactionId, transactionNo, List.of());
    }

    /**
     * mixed commit 变体：除 history old/new first page 外，同时按同一全局 pageNo 顺序打开需要重置的 INSERT
     * cached first page。这样 page3 后的普通 undo latch 顺序不依赖各 segment 的分配先后。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param oldTailPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param newTailPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param creatorTransactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param transactionNo 参与 {@code appendHistoryNode} 的稳定领域标识 {@code TransactionNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param cacheResets 参与 {@code appendHistoryNode} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    public void appendHistoryNode(MiniTransaction mtr, Optional<PageId> oldTailPageId,
                                  PageId newTailPageId, TransactionId creatorTransactionId,
                                  TransactionNo transactionNo,
                                  List<CachedUndoSegmentRef> cacheResets) {
        appendHistoryNode(mtr, oldTailPageId, newTailPageId, creatorTransactionId, transactionNo,
                cacheResets, List.of(), Optional.empty());
    }

    /**
     * mixed commit 完整首页批处理：history old/new、cache reset、free old tail/new nodes 一次收集并全局排序。
     * 页内全部预检完成后才写 history、cache 与 free 链，避免多个局部 helper 各自取得不相容的页序。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param oldTailPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param newTailPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param creatorTransactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param transactionNo 参与 {@code appendHistoryNode} 的稳定领域标识 {@code TransactionNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param cacheResets 参与 {@code appendHistoryNode} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param freeResets 参与 {@code appendHistoryNode} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param oldFreeTail 可选的 {@code oldFreeTail}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     */
    public void appendHistoryNode(MiniTransaction mtr, Optional<PageId> oldTailPageId,
                                  PageId newTailPageId, TransactionId creatorTransactionId,
                                  TransactionNo transactionNo, List<CachedUndoSegmentRef> cacheResets,
                                  List<FreeUndoSegmentRef> freeResets,
                                  Optional<FreeUndoSegmentRef> oldFreeTail) {
        appendHistoryNodeFromState(mtr, oldTailPageId, newTailPageId, creatorTransactionId,
                transactionNo, cacheResets, freeResets, oldFreeTail, UndoPageLayout.STATE_ACTIVE);
    }

    /**
     * prepared commit 变体：新 UPDATE tail 必须仍为 PREPARED，提交批次把它直接转成 COMMITTED并挂 history。
     * prepared INSERT owner由上层先 drop，不允许借此入口进入 cache/free reuse。
     *
     * @param mtr 当前 phase-two commit MTR
     * @param oldTailPageId history append lease冻结的旧尾
     * @param newTailPageId 当前 PREPARED UPDATE undo first page
     * @param creatorTransactionId prepared creator write id
     * @param transactionNo phase two 新分配的提交号
     */
    public void appendPreparedHistoryNode(MiniTransaction mtr, Optional<PageId> oldTailPageId,
                                          PageId newTailPageId, TransactionId creatorTransactionId,
                                          TransactionNo transactionNo) {
        appendHistoryNodeFromState(mtr, oldTailPageId, newTailPageId, creatorTransactionId,
                transactionNo, List.of(), List.of(), Optional.empty(),
                UndoPageLayout.STATE_PREPARED);
    }

    /** ACTIVE/PREPARED history append 共用的 first-page-only 原子实现。 */
    private void appendHistoryNodeFromState(
            MiniTransaction mtr, Optional<PageId> oldTailPageId,
            PageId newTailPageId, TransactionId creatorTransactionId,
            TransactionNo transactionNo, List<CachedUndoSegmentRef> cacheResets,
            List<FreeUndoSegmentRef> freeResets,
            Optional<FreeUndoSegmentRef> oldFreeTail, int expectedNewTailState) {
        if (mtr == null || oldTailPageId == null || newTailPageId == null
                || creatorTransactionId == null || creatorTransactionId.isNone()
                || transactionNo == null || transactionNo.isNone() || cacheResets == null
                || freeResets == null || oldFreeTail == null
                || cacheResets.stream().anyMatch(java.util.Objects::isNull)
                || freeResets.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("undo history append batch arguments are invalid");
        }
        if (expectedNewTailState != UndoPageLayout.STATE_ACTIVE
                && expectedNewTailState != UndoPageLayout.STATE_PREPARED) {
            throw new DatabaseValidationException(
                    "history append expected state must be ACTIVE or PREPARED");
        }
        if (expectedNewTailState == UndoPageLayout.STATE_PREPARED
                && (!cacheResets.isEmpty() || !freeResets.isEmpty() || oldFreeTail.isPresent())) {
            throw new DatabaseValidationException(
                    "prepared history append cannot reuse INSERT undo pages");
        }
        oldTailPageId.ifPresent(old -> requireSameHistorySpace(old, newTailPageId));
        if (oldTailPageId.equals(Optional.of(newTailPageId))) {
            throw new DatabaseValidationException("undo history append cannot link a page to itself");
        }
        List<PageId> pageIds = new ArrayList<>();
        oldTailPageId.ifPresent(pageIds::add);
        pageIds.add(newTailPageId);
        cacheResets.stream().map(item -> item.handle().firstPageId()).forEach(pageIds::add);
        oldFreeTail.map(item -> item.handle().firstPageId()).ifPresent(pageIds::add);
        freeResets.stream().map(item -> item.handle().firstPageId()).forEach(pageIds::add);
        if (pageIds.stream().distinct().count() != pageIds.size()) {
            throw new DatabaseValidationException("history append/cache reset first pages must be distinct");
        }
        Map<PageId, UndoPage> pages = openFirstPagesSorted(mtr, pageIds);
        UndoPage newTail = pages.get(newTailPageId);
        requireHistoryFirstPage(newTail, newTailPageId);
        if (newTail.undoKind() != UndoLogKind.UPDATE || newTail.state() != expectedNewTailState
                || !newTail.transactionId().equals(creatorTransactionId) || newTail.commitNo() != 0L
                || newTail.historyPrevPageNo() != FilePageHeader.FIL_NULL
                || newTail.historyNextPageNo() != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("new history tail must be an unlinked "
                    + UndoLogState.fromPhysical(expectedNewTailState) + " UPDATE undo page: "
                    + newTailPageId);
        }
        if (oldTailPageId.isPresent()) {
            UndoPage oldTail = pages.get(oldTailPageId.orElseThrow());
            requireHistoryFirstPage(oldTail, oldTailPageId.orElseThrow());
            if (oldTail.undoKind() != UndoLogKind.UPDATE
                    || oldTail.state() != UndoPageLayout.STATE_COMMITTED
                    || oldTail.commitNo() == 0L
                    || oldTail.historyNextPageNo() != FilePageHeader.FIL_NULL) {
                throw new UndoLogFormatException("old history tail is not a terminal COMMITTED UPDATE node: "
                        + oldTailPageId.orElseThrow());
            }
        }
        for (CachedUndoSegmentRef reset : cacheResets) {
            UndoPage page = pages.get(reset.handle().firstPageId());
            requireHistoryFirstPage(page, reset.handle().firstPageId());
            if (reset.kind() == UndoLogKind.TEMPORARY || page.undoKind() != reset.kind()
                    || page.state() != UndoPageLayout.STATE_ACTIVE
                    || !page.transactionId().equals(creatorTransactionId)
                    || page.historyPrevPageNo() != FilePageHeader.FIL_NULL
                    || page.historyNextPageNo() != FilePageHeader.FIL_NULL) {
                throw new UndoLogFormatException("mixed commit cache reset target changed: "
                        + reset.handle().firstPageId());
            }
        }
        validateFreeAppendTargets(pages, oldFreeTail, freeResets, creatorTransactionId);
        oldTailPageId.ifPresent(old -> pages.get(old).setHistoryNextPageNo(newTailPageId.pageNo().value()));
        newTail.setHistoryLinks(oldTailPageId.map(page -> page.pageNo().value()).orElse(FilePageHeader.FIL_NULL),
                FilePageHeader.FIL_NULL);
        newTail.setLogState(UndoPageLayout.STATE_COMMITTED);
        newTail.setCommitNo(transactionNo.value());
        for (CachedUndoSegmentRef reset : cacheResets) {
            pages.get(reset.handle().firstPageId()).resetForCache(reset.kind(), reset.handle());
        }
        appendFreeLinks(pages, oldFreeTail, freeResets);
    }

    /**
     * rollback/recovery rollback 的普通首页批处理。cache 与 free 目标及旧 free tail 一次按全局 PageId 排序，
     * 所有目标必须仍是同一 creator 的 ACTIVE 单页 segment。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param creatorTransactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param cacheResets 参与 {@code finalizeActiveReusablePages} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param freeResets 参与 {@code finalizeActiveReusablePages} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param oldFreeTail 可选的 {@code oldFreeTail}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void finalizeActiveReusablePages(MiniTransaction mtr, TransactionId creatorTransactionId,
                                            List<CachedUndoSegmentRef> cacheResets,
                                            List<FreeUndoSegmentRef> freeResets,
                                            Optional<FreeUndoSegmentRef> oldFreeTail) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || creatorTransactionId == null || creatorTransactionId.isNone()
                || cacheResets == null || freeResets == null || oldFreeTail == null
                || cacheResets.stream().anyMatch(java.util.Objects::isNull)
                || freeResets.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("active undo reusable-page batch arguments are invalid");
        }
        List<PageId> ids = new ArrayList<>();
        cacheResets.stream().map(item -> item.handle().firstPageId()).forEach(ids::add);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        oldFreeTail.map(item -> item.handle().firstPageId()).ifPresent(ids::add);
        freeResets.stream().map(item -> item.handle().firstPageId()).forEach(ids::add);
        if (ids.stream().distinct().count() != ids.size()) {
            throw new DatabaseValidationException("active reusable-page batch targets must be distinct");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        Map<PageId, UndoPage> pages = openFirstPagesSorted(mtr, ids);
        for (CachedUndoSegmentRef reset : cacheResets) {
            UndoPage page = pages.get(reset.handle().firstPageId());
            requireActiveReusablePage(page, reset.handle(), creatorTransactionId, reset.kind());
        }
        validateFreeAppendTargets(pages, oldFreeTail, freeResets, creatorTransactionId);
        for (CachedUndoSegmentRef reset : cacheResets) {
            pages.get(reset.handle().firstPageId()).resetForCache(reset.kind(), reset.handle());
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        appendFreeLinks(pages, oldFreeTail, freeResets);
    }

    /**
     * purge 最终 MTR 的 first-page-only 批处理。非缓存路径只触碰新 head；被回收首页已经由 FSP drop，不再获取其
     * latch。缓存路径同时按页号排序打开 removed/new head，把 removed 重置为 CACHED 并清除 history links。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param removed 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param newHead 可选的 {@code newHead}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param cacheRemoved 既有物理或缓存步骤是否已经发生；该事实用于避免重复修改、重复补偿或错误发布中间状态
     */
    public void unlinkHistoryHead(MiniTransaction mtr, UndoHistoryNodeSnapshot removed,
                                  Optional<UndoHistoryNodeSnapshot> newHead, boolean cacheRemoved) {
        unlinkHistoryHead(mtr, removed, newHead, cacheRemoved, false, Optional.empty());
    }

    /** purge 完整首页批处理：history successor 与 reusable reset/free tail 一次按全局页序获取。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param removed 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param newHead 可选的 {@code newHead}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param cacheRemoved 既有物理或缓存步骤是否已经发生；该事实用于避免重复修改、重复补偿或错误发布中间状态
     * @param freeRemoved 既有物理或缓存步骤是否已经发生；该事实用于避免重复修改、重复补偿或错误发布中间状态
     * @param oldFreeTail 可选的 {@code oldFreeTail}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void unlinkHistoryHead(MiniTransaction mtr, UndoHistoryNodeSnapshot removed,
                                  Optional<UndoHistoryNodeSnapshot> newHead, boolean cacheRemoved,
                                  boolean freeRemoved, Optional<FreeUndoSegmentRef> oldFreeTail) {
        if (mtr == null || removed == null || newHead == null || !removed.isCommitted()
                || removed.kind() != UndoLogKind.UPDATE || oldFreeTail == null
                || cacheRemoved && freeRemoved || !freeRemoved && oldFreeTail.isPresent()) {
            throw new DatabaseValidationException("undo history unlink batch arguments are invalid");
        }
        if (!removed.previousHistoryPageId().isEmpty()
                || !removed.nextHistoryPageId().equals(newHead.map(UndoHistoryNodeSnapshot::firstPageId))) {
            throw new UndoLogFormatException("removed history head links do not match the frozen successor");
        }
        newHead.ifPresent(next -> {
            requireSameHistorySpace(removed.firstPageId(), next.firstPageId());
            if (!next.isCommitted() || next.kind() != UndoLogKind.UPDATE
                    || !next.previousHistoryPageId().equals(Optional.of(removed.firstPageId()))) {
                throw new UndoLogFormatException("new history head does not point back to removed node");
            }
        });
        List<PageId> ids = new ArrayList<>();
        newHead.map(UndoHistoryNodeSnapshot::firstPageId).ifPresent(ids::add);
        if (cacheRemoved) {
            ids.add(removed.firstPageId());
        }
        if (freeRemoved) {
            ids.add(removed.firstPageId());
            oldFreeTail.map(item -> item.handle().firstPageId()).ifPresent(ids::add);
        }
        if (ids.stream().distinct().count() != ids.size()) {
            throw new DatabaseValidationException("purge history/reuse page targets must be distinct");
        }
        Map<PageId, UndoPage> pages = openFirstPagesSorted(mtr, ids);
        newHead.ifPresent(snapshot -> {
            UndoPage page = pages.get(snapshot.firstPageId());
            requireHistoryFirstPage(page, snapshot.firstPageId());
            if (page.state() != UndoPageLayout.STATE_COMMITTED
                    || page.historyPrevPageNo() != removed.firstPageId().pageNo().value()) {
                throw new UndoLogFormatException("new history head changed before unlink");
            }
            page.setHistoryPrevPageNo(FilePageHeader.FIL_NULL);
        });
        if (cacheRemoved) {
            UndoPage page = pages.get(removed.firstPageId());
            requireHistoryFirstPage(page, removed.firstPageId());
            if (page.state() != UndoPageLayout.STATE_COMMITTED
                    || page.commitNo() != removed.committedTransactionNo().value()) {
                throw new UndoLogFormatException("removed history node changed before cache reset");
            }
            page.resetForCache(UndoLogKind.UPDATE, removed.handle());
        }
        if (freeRemoved) {
            UndoPage page = pages.get(removed.firstPageId());
            requireHistoryFirstPage(page, removed.firstPageId());
            if (page.state() != UndoPageLayout.STATE_COMMITTED
                    || page.commitNo() != removed.committedTransactionNo().value()) {
                throw new UndoLogFormatException("removed history node changed before free reset");
            }
            validateOldFreeTail(pages, oldFreeTail);
            long previous = oldFreeTail.map(item -> item.handle().firstPageId().pageNo().value())
                    .orElse(FilePageHeader.FIL_NULL);
            oldFreeTail.ifPresent(tail -> pages.get(tail.handle().firstPageId())
                    .setFreeNextPageNo(removed.firstPageId().pageNo().value()));
            page.resetForFree(removed.handle(), previous, FilePageHeader.FIL_NULL);
        }
    }

    private static void requireActiveReusablePage(UndoPage page, UndoSegmentHandle handle,
                                                   TransactionId creator, UndoLogKind kind) {
        requireHistoryFirstPage(page, handle.firstPageId());
        if (kind == UndoLogKind.TEMPORARY || page.state() != UndoPageLayout.STATE_ACTIVE
                || page.undoKind() != kind || !page.transactionId().equals(creator)
                || page.historyPrevPageNo() != FilePageHeader.FIL_NULL
                || page.historyNextPageNo() != FilePageHeader.FIL_NULL
                || !handle.firstPageId().equals(handle.lastPageId())) {
            throw new UndoLogFormatException("active reusable undo first page changed: " + handle.firstPageId());
        }
    }

    /**
     * 校验 {@code validateFreeAppendTargets} 涉及的Undo 日志结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param pages 参与 {@code validateFreeAppendTargets} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @param oldFreeTail 可选的 {@code oldFreeTail}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param freeResets 参与 {@code validateFreeAppendTargets} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param creator 参与 {@code validateFreeAppendTargets} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static void validateFreeAppendTargets(Map<PageId, UndoPage> pages,
                                                  Optional<FreeUndoSegmentRef> oldFreeTail,
                                                  List<FreeUndoSegmentRef> freeResets,
                                                  TransactionId creator) {
        validateOldFreeTail(pages, oldFreeTail);
        for (FreeUndoSegmentRef reset : freeResets) {
            UndoPage page = pages.get(reset.handle().firstPageId());
            requireHistoryFirstPage(page, reset.handle().firstPageId());
            if (page.state() != UndoPageLayout.STATE_ACTIVE || !page.transactionId().equals(creator)
                    || page.undoKind() == UndoLogKind.TEMPORARY
                    || page.historyPrevPageNo() != FilePageHeader.FIL_NULL
                    || page.historyNextPageNo() != FilePageHeader.FIL_NULL
                    || !reset.handle().firstPageId().equals(reset.handle().lastPageId())) {
                throw new UndoLogFormatException("free reset target changed: " + reset.handle().firstPageId());
            }
        }
    }

    /**
     * 校验 {@code validateOldFreeTail} 涉及的Undo 日志结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param pages 参与 {@code validateOldFreeTail} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @param oldFreeTail 可选的 {@code oldFreeTail}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     */
    private static void validateOldFreeTail(Map<PageId, UndoPage> pages,
                                            Optional<FreeUndoSegmentRef> oldFreeTail) {
        oldFreeTail.ifPresent(ref -> {
            UndoPage page = pages.get(ref.handle().firstPageId());
            requireHistoryFirstPage(page, ref.handle().firstPageId());
            page.requireFreeEmpty(ref.handle());
            if (page.freeNextPageNo() != FilePageHeader.FIL_NULL) {
                throw new UndoLogFormatException("old free tail is not terminal: " + page.pageId());
            }
        });
    }

    /**
     * 释放本方法拥有的Undo 日志资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param pages 参与 {@code appendFreeLinks} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @param oldFreeTail 可选的 {@code oldFreeTail}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param freeResets 参与 {@code appendFreeLinks} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    private static void appendFreeLinks(Map<PageId, UndoPage> pages,
                                        Optional<FreeUndoSegmentRef> oldFreeTail,
                                        List<FreeUndoSegmentRef> freeResets) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (freeResets.isEmpty()) {
            return;
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        PageId first = freeResets.getFirst().handle().firstPageId();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        oldFreeTail.ifPresent(ref -> pages.get(ref.handle().firstPageId())
                .setFreeNextPageNo(first.pageNo().value()));
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        for (int i = 0; i < freeResets.size(); i++) {
            FreeUndoSegmentRef reset = freeResets.get(i);
            long previous = i == 0
                    ? oldFreeTail.map(item -> item.handle().firstPageId().pageNo().value())
                    .orElse(FilePageHeader.FIL_NULL)
                    : freeResets.get(i - 1).handle().firstPageId().pageNo().value();
            long next = i + 1 == freeResets.size() ? FilePageHeader.FIL_NULL
                    : freeResets.get(i + 1).handle().firstPageId().pageNo().value();
            pages.get(reset.handle().firstPageId()).resetForFree(reset.handle(), previous, next);
        }
    }

    /**
     * 定位并读取Undo 日志领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param firstPageIds 参与 {@code openFirstPagesSorted} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    private Map<PageId, UndoPage> openFirstPagesSorted(MiniTransaction mtr, List<PageId> firstPageIds) {
        Map<PageId, UndoPage> pages = new LinkedHashMap<>();
        firstPageIds.stream().distinct()
                .sorted(Comparator.comparingInt((PageId page) -> page.spaceId().value())
                        .thenComparingLong(page -> page.pageNo().value()))
                .forEach(pageId -> pages.put(pageId,
                        pageAccess.openUndoPage(mtr, pageId, PageLatchMode.EXCLUSIVE)));
        return pages;
    }

    private static void requireHistoryFirstPage(UndoPage page, PageId expectedPageId) {
        if (page == null || !page.pageId().equals(expectedPageId) || !page.isFirstPage()) {
            throw new UndoLogFormatException("history batch target is not the expected first page: "
                    + expectedPageId);
        }
    }

    private static Optional<PageId> historyLink(PageId owner, long linkedPageNo) {
        if (linkedPageNo == FilePageHeader.FIL_NULL) {
            return Optional.empty();
        }
        if (linkedPageNo < 0 || linkedPageNo > FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("invalid undo history link on " + owner + ": " + linkedPageNo);
        }
        return Optional.of(PageId.of(owner.spaceId(), PageNo.of(linkedPageNo)));
    }

    /**
     * 根据调用参数创建或转换 {@code freeLink} 返回的 {@code Optional<PageId>}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param owner 参与 {@code freeLink} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param linkedPageNo 参与 {@code freeLink} 的原始数值身份 {@code linkedPageNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @return {@code freeLink} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static Optional<PageId> freeLink(PageId owner, long linkedPageNo) {
        if (linkedPageNo == FilePageHeader.FIL_NULL) {
            return Optional.empty();
        }
        if (linkedPageNo < 0 || linkedPageNo > FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("invalid undo free link on " + owner + ": " + linkedPageNo);
        }
        return Optional.of(PageId.of(owner.spaceId(), PageNo.of(linkedPageNo)));
    }

    private static void requireSameHistorySpace(PageId left, PageId right) {
        if (!left.spaceId().equals(right.spaceId())) {
            throw new DatabaseValidationException("undo history links cannot cross tablespaces");
        }
    }

    /**
     * 复用 cached segment：按 page3/内存冻结的 handle 重新 X-open、逐字段验证，再重置为新事务 ACTIVE 首页。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param cached 参与 {@code activateCached} 的稳定领域标识 {@code CachedUndoSegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @return {@code activateCached} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoLogSegment activateCached(MiniTransaction mtr, CachedUndoSegmentRef cached,
                                         TransactionId transactionId) {
        if (mtr == null || cached == null || transactionId == null || transactionId.isNone()) {
            throw new DatabaseValidationException("cached undo activation args are invalid");
        }
        UndoLogSegment segment = open(mtr, cached.handle().firstPageId(), PageLatchMode.EXCLUSIVE);
        if (!segment.handle().equals(cached.handle())) {
            throw new UndoLogFormatException("cached undo handle changed before activation: expected="
                    + cached.handle() + ", current=" + segment.handle());
        }
        segment.requireCached(cached.kind());
        segment.activateCached(transactionId, cached.kind());
        return segment;
    }

    /**
     * 复用 free head：一次收集 candidate 与可选 successor，按全局 PageId 升序持 X latch；先验证冻结的链关系，
     * 再清 successor.prev 并把 candidate 以新事务/kind 激活。page3 base 已由同一 MTR 的调用方先 CAS。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param candidate 参与 {@code activateFree} 的稳定领域标识 {@code FreeUndoSegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param successor 可选的 {@code successor}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param kind 选择 {@code activateFree} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code activateFree} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoLogSegment activateFree(MiniTransaction mtr, FreeUndoSegmentRef candidate,
                                       Optional<FreeUndoSegmentRef> successor,
                                       TransactionId transactionId, UndoLogKind kind) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || candidate == null || successor == null || transactionId == null
                || transactionId.isNone() || kind == null || kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("free undo activation args are invalid");
        }
        List<PageId> ids = new ArrayList<>();
        ids.add(candidate.handle().firstPageId());
        successor.map(item -> item.handle().firstPageId()).ifPresent(ids::add);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (ids.stream().distinct().count() != ids.size()) {
            throw new DatabaseValidationException("free undo head cannot be its own successor");
        }
        Map<PageId, UndoPage> pages = openFirstPagesSorted(mtr, ids);
        UndoPage head = pages.get(candidate.handle().firstPageId());
        requireHistoryFirstPage(head, candidate.handle().firstPageId());
        if (!head.segmentId().equals(candidate.handle().segmentId())
                || head.inodeSlot() != candidate.handle().inodeSlot()) {
            throw new UndoLogFormatException("free undo handle changed before activation: "
                    + candidate.handle().firstPageId());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        head.requireFreeEmpty(candidate.handle());
        Optional<PageId> expectedSuccessor = successor.map(item -> item.handle().firstPageId());
        if (head.freePrevPageNo() != FilePageHeader.FIL_NULL
                || !freeLink(head.pageId(), head.freeNextPageNo()).equals(expectedSuccessor)) {
            throw new UndoLogFormatException("free undo head links changed before activation: " + head.pageId());
        }
        successor.ifPresent(ref -> {
            UndoPage next = pages.get(ref.handle().firstPageId());
            requireHistoryFirstPage(next, ref.handle().firstPageId());
            next.requireFreeEmpty(ref.handle());
            if (next.freePrevPageNo() != head.pageId().pageNo().value()) {
                throw new UndoLogFormatException("free undo successor does not point back to head: " + next.pageId());
            }
            next.setFreePrevPageNo(FilePageHeader.FIL_NULL);
        });
        head.activateFree(kind, transactionId, candidate.handle());
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return new UndoLogSegment(mtr, pageSize, allocator, codec, pageAccess, payloadStorage,
                storedRecordResolver, maxExternalPages, candidate.handle(), head, head, PageLatchMode.EXCLUSIVE);
    }

    /** truncate 摘除若干 free heads 后校验剩余新 head 的旧前驱并清空 prev。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param remainingHead 参与 {@code relinkFreeHeadAfterDrain} 的稳定领域标识 {@code FreeUndoSegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedRemovedPredecessor 参与 {@code relinkFreeHeadAfterDrain} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void relinkFreeHeadAfterDrain(MiniTransaction mtr, FreeUndoSegmentRef remainingHead,
                                         PageId expectedRemovedPredecessor) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || remainingHead == null || expectedRemovedPredecessor == null) {
            throw new DatabaseValidationException("free drain relink args must not be null");
        }
        UndoLogSegment segment = open(mtr, remainingHead.handle().firstPageId(), PageLatchMode.EXCLUSIVE);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (!segment.handle().equals(remainingHead.handle())) {
            throw new UndoLogFormatException("remaining free head handle changed before drain relink");
        }
        segment.requireFree();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoPage page = pageAccess.openUndoPage(mtr, remainingHead.handle().firstPageId(), PageLatchMode.EXCLUSIVE);
        if (page.freePrevPageNo() != expectedRemovedPredecessor.pageNo().value()) {
            throw new UndoLogFormatException("remaining free head predecessor changed before drain relink");
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        page.setFreePrevPageNo(FilePageHeader.FIL_NULL);
    }

    /**
     * 按 roll pointer 直读单条 undo record（T1.4 MVCC 旧版本构造用）。与 {@link UndoLogSegment#readRecord} 的区别：
     * **不依赖 segment 首页/句柄**——一致性读要读的旧版本可能落在**别的事务**的 undo 段，调用方只有 roll pointer。
     * 单 undo 空间按 {@code rollPtr}(pageNo+offset) 以 S latch 直开 UNDO 页（页类型守门在 {@link UndoPageAccess#openUndoPage}）、
     * 解码单条 record（单条不跨页，§379）。
     *
     * <p><b>损坏快速失败</b>（全部抛 {@link UndoLogFormatException}，不返回拼错的历史行）：NULL 指针；offset 越出
     * record area {@code [RECORD_AREA_START, freeOffset)} 或槽溢出已用区；指针 insert 位与解码 record 类型不一致；
     * record 的 {@code indexId} 与 {@code keyDef.indexId()} 不一致。调用方读完应即提交/回滚该只读 MTR 释放 S latch。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param undoSpace 单 undo 表空间（roll pointer 不编码 spaceId，由调用方提供）。
     * @return 解码出的 undo record（INSERT_ROW 或 UPDATE_ROW）。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param rollPtr 参与 {@code readRecordByRollPointer} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoRecord readRecordByRollPointer(MiniTransaction mtr, SpaceId undoSpace, RollPointer rollPtr,
                                              IndexKeyDef keyDef, TableSchema schema) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || undoSpace == null || rollPtr == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("readRecordByRollPointer args must not be null");
        }
        if (rollPtr.isNull()) {
            throw new UndoLogFormatException("cannot read undo record from NULL roll pointer");
        }
        UndoPage page = pageAccess.openUndoPage(mtr, PageId.of(undoSpace, rollPtr.pageNo()), PageLatchMode.SHARED);
        int off = rollPtr.offset();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        int free = page.freeOffset();
        if (off < UndoPageLayout.RECORD_AREA_START || off >= free) {
            throw new UndoLogFormatException("roll pointer offset " + off + " outside undo record area ["
                    + UndoPageLayout.RECORD_AREA_START + "," + free + ")");
        }
        byte[] payload = page.recordAt(off);
        if (off + 2 + payload.length > free) {
            throw new UndoLogFormatException("undo record slot at " + off + " overruns used area (free=" + free + ")");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        UndoRecord rec = storedRecordResolver.resolve(mtr, undoSpace,
                new UndoPayloadStorage.SegmentIdentity(page.segmentId(), page.inodeSlot()),
                payload, keyDef, schema);
        requireRecordKind(page.undoKind(), rec.type(), "direct roll-pointer read");
        boolean expectInsert = rec.type() == UndoRecordType.INSERT_ROW;
        if (rollPtr.insert() != expectInsert) {
            throw new UndoLogFormatException("roll pointer insert bit " + rollPtr.insert()
                    + " inconsistent with undo record type " + rec.type());
        }
        if (rec.indexId() != keyDef.indexId()) {
            throw new UndoLogFormatException("undo record indexId " + rec.indexId()
                    + " != expected " + keyDef.indexId());
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return rec;
    }

    /**
     * 校验 {@code requireRecordKind} 涉及的Undo 日志结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param kind 选择 {@code requireRecordKind} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param type 选择 {@code requireRecordKind} 分支的 {@code UndoRecordType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param operation 传给 {@code requireRecordKind} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void requireRecordKind(UndoLogKind kind, UndoRecordType type, String operation) {
        boolean valid = switch (kind) {
            case INSERT -> type == UndoRecordType.INSERT_ROW;
            case UPDATE -> type == UndoRecordType.UPDATE_ROW || type == UndoRecordType.DELETE_MARK;
            case TEMPORARY -> false;
        };
        if (!valid) {
            throw new UndoLogFormatException(operation + " found record type " + type
                    + " in " + kind + " undo log");
        }
    }

    /** 在写 MTR admission 前冻结一条 record 的编码与 external 页数。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code planRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public UndoRecordWritePlan planRecord(UndoRecord record, IndexKeyDef keyDef, TableSchema schema) {
        return UndoRecordWritePlan.create(codec, pageSize, record, keyDef, schema, maxExternalPages);
    }

    /** 根据规划快照精确计算本次会消费的 segment 首页、普通 root grow 和 external payload 页总数。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param newLog 当前对象是否处于首次创建、首次写入或复用分支；该事实决定初始化、redo 和失败补偿路径
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param recordPlan 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code plannedNewPages} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public int plannedNewPages(boolean newLog, UndoAppendSnapshot snapshot, UndoRecordWritePlan recordPlan) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (recordPlan == null || (!newLog && snapshot == null)) {
            throw new DatabaseValidationException("undo planned page calculation args invalid");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        int rootPages;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (newLog) {
            rootPages = 1;
        } else {
            int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
            rootPages = snapshot.tailFreeOffset() + Short.BYTES + recordPlan.rootPayloadLength() > limit ? 1 : 0;
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return Math.addExact(rootPages, recordPlan.externalPageCount());
    }

    /** 为计划中全部新页一次性预留 undo 空间；0 页计划由调用方跳过。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param pages 参与 {@code reservePages} 的上界或规格值 {@code pages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return {@code reservePages} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoSpaceReservation reservePages(MiniTransaction mtr, SpaceId spaceId, long pages) {
        if (pages <= 0) {
            throw new DatabaseValidationException("planned undo reservation pages must be positive: " + pages);
        }
        return allocator.reserveGrowPages(mtr, spaceId, pages);
    }

    /**
     * 用短只读 MTR物化 append 所需的 first/tail 权威快照。尾页与首页不同时单独 S-open，并验证 segment owner；
     * 调用方提交只读 MTR 后只保留该不可变值对象。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code inspectAppendSnapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoAppendSnapshot inspectAppendSnapshot(MiniTransaction mtr, PageId firstPageId) {
        if (mtr == null || firstPageId == null) {
            throw new DatabaseValidationException("undo append snapshot mtr/first page must not be null");
        }
        UndoPage first = pageAccess.openUndoPage(mtr, firstPageId, PageLatchMode.SHARED);
        if (!first.isFirstPage()) {
            throw new UndoLogFormatException("append snapshot page is not undo first page: " + firstPageId);
        }
        PageId tailId = PageId.of(firstPageId.spaceId(), PageNo.of(first.lastPageNo()));
        UndoPage tail = first;
        if (!tailId.equals(firstPageId)) {
            try (var ignored = mtr.allowOutOfOrderPageLatch(
                    "undo append planning opens persisted tail after first-page snapshot")) {
                tail = pageAccess.openUndoPage(mtr, tailId, PageLatchMode.SHARED);
            }
            if (!tail.segmentId().equals(first.segmentId()) || tail.inodeSlot() != first.inodeSlot()) {
                throw new UndoLogFormatException("undo append snapshot tail owner mismatch: " + tailId);
            }
        }
        return new UndoAppendSnapshot(firstPageId, tailId, first.segmentId(), first.inodeSlot(),
                first.transactionId(), first.logLastUndoNo(), first.logicalHead(), first.undoKind(),
                first.logRecordCount(), tail.freeOffset());
    }
}
