package cn.zhangyis.db.storage.api.index;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageStructureValidator;

/**
 * INDEX 页的 MTR 生产入口（设计 §14，Facade）：把「建 RecordPage 的页」绑定到 MTR-owned guard，
 * 使 record 的格式化/写入经 D3 的 collector 自动产 redo（PAGE_INIT/PAGE_BYTES）、commit 盖 pageLSN。
 *
 * <p>R3–R5 算子（RecordPageInserter/Search/Updater/...）签名不变——调用方拿本类返回的 {@link RecordPage} 跑它们。
 * {@link RecordPage} 仍不依赖 mtr（保持 R3 解耦）。返回的 RecordPage 由 mtr memo 持 guard，**勿自行 close**，
 * 须在同一 MTR 内使用；MTR commit/rollback 释放 guard（commit 才盖 pageLSN）。无状态、线程安全。
 */
public final class IndexPageAccess {

    /**
     * 本对象持有的 {@code pool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final BufferPool pool;
    /**
     * 本对象持有的 {@code pageSize} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final PageSize pageSize;
    /**
     * 可选运行时表空间 registry。生产组合根注入共享 registry 后，本类会在 MTR 持有表空间 S lease 后复核状态；
     * 两参构造保留给低层页格式单测，不做 lifecycle 状态准入。
     */
    private final TablespaceRegistry tablespaceRegistry;

    /**
     * 创建 {@code IndexPageAccess}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     */
    public IndexPageAccess(BufferPool pool, PageSize pageSize) {
        this(pool, pageSize, null);
    }

    /**
     * 创建带运行时表空间状态准入的 INDEX 页访问器。数据流为：调用方传入 MTR 和 PageId →
     * 本类先通过 {@link MiniTransaction#acquireTablespaceLease(SpaceId)} 获取共享 operation lease →
     * 再调用 {@link TablespaceRegistry#require(SpaceId)} 复核 NORMAL/ACTIVE 状态 →
     * 最后进入 Buffer Pool fix/newPage。这样若 truncate/drop/discard 在调用前后切换状态，醒来的线程会看到新状态，
     * 不会绕过 {@code DiskSpaceManager} 的准入边界直接访问旧页。
     *
     * @param pool Buffer Pool。
     * @param pageSize 页大小。
     * @param tablespaceRegistry 运行时表空间 registry；可为 null，表示只做 MTR/页类型边界校验。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public IndexPageAccess(BufferPool pool, PageSize pageSize, TablespaceRegistry tablespaceRegistry) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("index page access pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.tablespaceRegistry = tablespaceRegistry;
    }

    /**
     * 暴露页大小（只读）。B+Tree merge 需要按页容量判定 underflow 阈值（MERGE_THRESHOLD≈50%）与 merge fit，
     * 故服务层经此读取页容量；本类仍不暴露 frame/裸文件。
     */
    public PageSize pageSize() {
        return pageSize;
    }

    /**
     * 建并格式化一个 INDEX 页（要求在 mtr 内）：newPage(X,INDEX) → 写信封(INDEX) → format(indexId,level)。
     * 产 PAGE_INIT(INDEX) + 信封/格式 PAGE_BYTES；commit 盖 pageLSN。
     *
     * <p><b>校验全部前置</b>：任何 newPage/写页之前先校验入参——否则 indexId/level 非法在 format 阶段才失败时，
     * 页已被 newPage 重初始化并收集 PAGE_INIT，而 MTR rollback 不做内容 undo（脏页）。
     *
     * <p><b>破坏性入口</b>：因走 newPage（D4a 对驻留页会清零重初始化），**只能用于新分配/有意重初始化的页**；
     * 对已有 INDEX 页的读写**必须走 {@link #openIndexPage}**，否则会清空在用页。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param indexId 参与 {@code createIndexPage} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @param level 参与 {@code createIndexPage} 的树层级或递归深度 {@code level}；必须非负且不得超过当前页结构、MTR memo 或解析器声明的最大深度
     * @return {@code createIndexPage} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecordPage createIndexPage(MiniTransaction mtr, PageId pageId, long indexId, int level) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || pageId == null) {
            throw new DatabaseValidationException("createIndexPage mtr/pageId must not be null");
        }
        if (indexId < 0) {
            throw new DatabaseValidationException("indexId must be non-negative: " + indexId);
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        if (level < 0) {
            throw new DatabaseValidationException("level must be non-negative: " + level);
        }
        requireOrdinaryAccess(mtr, pageId.spaceId());
        PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.INDEX);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        PageEnvelope.writeHeader(g, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INDEX));
        RecordPage rp = new RecordPage(g, pageSize);
        rp.format(indexId, level);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return rp;
    }

    /**
     * 取已存在页做 CRUD（X）或只读扫描（S，无写→无 redo）：getPage(mode) → 包成 {@link RecordPage}。
     * 入参先校验，再取页。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code openIndexPage} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public RecordPage openIndexPage(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        return openIndexPageHandle(mtr, pageId, mode).recordPage();
    }

    /**
     * 打开已存在 INDEX 页并返回包含 FilePageHeader 与 RecordPage 视图的短生命周期句柄。
     * 句柄不拥有释放权，guard 仍由 MTR memo 持有；B+Tree split 使用它窄写 leaf sibling 链。
     *
     * <p>数据流：完成 lifecycle lease/registry 复核后 fix 目标页，再在当前 S/X latch 内执行一次完整 record-page
     * 结构校验；只有 header、系统记录、用户链与 PageDirectory 账本一致才返回 handle。校验失败发生在任何本次
     * B+Tree 内容修改与 redo 收集前，调用方回滚 MTR 即可统一释放刚取得的 guard/lease。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code openIndexPageHandle} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public IndexPageHandle openIndexPageHandle(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        if (mtr == null || pageId == null || mode == null) {
            throw new DatabaseValidationException("openIndexPage mtr/pageId/mode must not be null");
        }
        requireOrdinaryAccess(mtr, pageId.spaceId());
        PageGuard g = mtr.getPage(pool, pageId, mode);
        // 已存在页在统一 fix 边界只校验一次；任何损坏都早于 B+Tree 字段解析、写页和 redo 收集失败。
        RecordPageStructureValidator.validate(new RecordPage(g, pageSize));
        return new IndexPageHandle(pageId, g, pageSize);
    }

    /**
     * 提前释放句柄的 page latch + buffer fix（B+Tree latch coupling / crab / safe-node，设计 §10.2）：把 guard 交回
     * MTR 做选择性（非 LIFO）释放。仅应对**未写过**的页使用——S 导航页恒安全；未写过的 X/SX 祖先在 safe-node 早释放
     * 或 restart 整链释放时同样合法（MTR 只对已 touched 页的 X guard 拒绝，保护 commit 盖 pageLSN 的不变量）。
     * 释放后该句柄不得再使用。btree 服务只经本入口早释放，仍不接触裸 guard/frame。
     *
     * @param mtr    持有该 guard 的 mini-transaction。
     * @param handle 待提前释放的 INDEX 页句柄（其 guard 仍在 mtr memo 中）。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void releaseHandle(MiniTransaction mtr, IndexPageHandle handle) {
        if (mtr == null || handle == null) {
            throw new DatabaseValidationException("releaseHandle mtr/handle must not be null");
        }
        mtr.releaseLatch(handle.pageId(), handle.guard());
    }

    private void requireOrdinaryAccess(MiniTransaction mtr, SpaceId spaceId) {
        if (tablespaceRegistry == null) {
            return;
        }
        mtr.acquireTablespaceLease(spaceId);
        tablespaceRegistry.require(spaceId);
    }
}
