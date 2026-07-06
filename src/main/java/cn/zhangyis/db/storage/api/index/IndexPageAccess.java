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

/**
 * INDEX 页的 MTR 生产入口（设计 §14，Facade）：把「建 RecordPage 的页」绑定到 MTR-owned guard，
 * 使 record 的格式化/写入经 D3 的 collector 自动产 redo（PAGE_INIT/PAGE_BYTES）、commit 盖 pageLSN。
 *
 * <p>R3–R5 算子（RecordPageInserter/Search/Updater/...）签名不变——调用方拿本类返回的 {@link RecordPage} 跑它们。
 * {@link RecordPage} 仍不依赖 mtr（保持 R3 解耦）。返回的 RecordPage 由 mtr memo 持 guard，**勿自行 close**，
 * 须在同一 MTR 内使用；MTR commit/rollback 释放 guard（commit 才盖 pageLSN）。无状态、线程安全。
 */
public final class IndexPageAccess {

    private final BufferPool pool;
    private final PageSize pageSize;
    /**
     * 可选运行时表空间 registry。生产组合根注入共享 registry 后，本类会在 MTR 持有表空间 S lease 后复核状态；
     * 两参构造保留给低层页格式单测，不做 lifecycle 状态准入。
     */
    private final TablespaceRegistry tablespaceRegistry;

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
     */
    public RecordPage createIndexPage(MiniTransaction mtr, PageId pageId, long indexId, int level) {
        if (mtr == null || pageId == null) {
            throw new DatabaseValidationException("createIndexPage mtr/pageId must not be null");
        }
        if (indexId < 0) {
            throw new DatabaseValidationException("indexId must be non-negative: " + indexId);
        }
        if (level < 0) {
            throw new DatabaseValidationException("level must be non-negative: " + level);
        }
        requireOrdinaryAccess(mtr, pageId.spaceId());
        PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.INDEX);
        PageEnvelope.writeHeader(g, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INDEX));
        RecordPage rp = new RecordPage(g, pageSize);
        rp.format(indexId, level);
        return rp;
    }

    /**
     * 取已存在页做 CRUD（X）或只读扫描（S，无写→无 redo）：getPage(mode) → 包成 {@link RecordPage}。
     * 入参先校验，再取页。
     */
    public RecordPage openIndexPage(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        return openIndexPageHandle(mtr, pageId, mode).recordPage();
    }

    /**
     * 打开已存在 INDEX 页并返回包含 FilePageHeader 与 RecordPage 视图的短生命周期句柄。
     * 句柄不拥有释放权，guard 仍由 MTR memo 持有；B+Tree split 使用它窄写 leaf sibling 链。
     */
    public IndexPageHandle openIndexPageHandle(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        if (mtr == null || pageId == null || mode == null) {
            throw new DatabaseValidationException("openIndexPage mtr/pageId/mode must not be null");
        }
        requireOrdinaryAccess(mtr, pageId.spaceId());
        PageGuard g = mtr.getPage(pool, pageId, mode);
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
