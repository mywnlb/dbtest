package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/**
 * undo 页的 MTR 生产入口。建/开 undo 页绑定到 MTR-owned guard，使 PAGE_INIT/PAGE_BYTES 自动进入 redo，
 * commit 时盖 pageLSN；返回的 {@link UndoPage} 不自行释放，生命周期归 MiniTransaction memo 管理。
 */
public final class UndoPageAccess {

    /**
     * 页缓存门面。仅通过 MiniTransaction 获取 guard，UndoPageAccess 不直接持有或释放 BufferFrame。
     */
    private final BufferPool pool;

    /**
     * 页大小，用于构造 UndoPage 后进行 record 区容量判断。
     */
    private final PageSize pageSize;

    /**
     * 可选运行时表空间 registry。生产 undo 路径注入后，open/create 会在 MTR 取得共享 operation lease 后复核状态；
     * 两参构造保留给低层 undo 页格式测试，不做 registry 准入。
     */
    private final TablespaceRegistry tablespaceRegistry;

    public UndoPageAccess(BufferPool pool, PageSize pageSize) {
        this(pool, pageSize, null);
    }

    /**
     * 创建带运行时表空间状态准入的 UNDO 页访问器。数据流与 INDEX 页访问一致：先持表空间 S lease，再复核
     * registry NORMAL/ACTIVE 状态，最后进入 Buffer Pool fix/newPage。这样上层若绕过 DiskSpaceManager，也不能在
     * UNDO 表空间被标记 INACTIVE/CORRUPTED/DISCARDED 后继续打开旧 undo 页或重初始化新 undo 页。
     *
     * @param pool Buffer Pool。
     * @param pageSize 页大小。
     * @param tablespaceRegistry 运行时表空间 registry；可为 null，表示仅做页类型守门。
     */
    public UndoPageAccess(BufferPool pool, PageSize pageSize, TablespaceRegistry tablespaceRegistry) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("undo page access pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.tablespaceRegistry = tablespaceRegistry;
    }

    /**
     * 建并格式化 undo log first 页。数据流：newPage(X, UNDO) 取得零化页并写 FIL 信封 →
     * {@link UndoPage#formatFirstPage(UndoLogKind, TransactionId, UndoSegmentHandle)} 写 page/log header。
     * 该入口是破坏性初始化，只能用于刚由 FSP 分配的页。
     */
    public UndoPage createFirstPage(MiniTransaction mtr, PageId pageId, UndoLogKind kind,
                                    TransactionId txnId, UndoSegmentHandle handle) {
        if (mtr == null || pageId == null || kind == null || txnId == null || handle == null) {
            throw new DatabaseValidationException("createFirstPage args must not be null");
        }
        PageGuard g = newUndoEnvelope(mtr, pageId);
        UndoPage page = new UndoPage(mtr, g, pageSize);
        page.formatFirstPage(kind, txnId, handle);
        return page;
    }

    /**
     * 建并格式化 undo chain 页。chain 页保留 page header，并在 log-header 预留区只复制 v2 kind；其 FIL prev/next
     * 由调用方在生长流程中显式链接，确保 preflight 后才产生任何页链副作用。
     */
    public UndoPage createChainPage(MiniTransaction mtr, PageId pageId, UndoLogKind kind,
                                    UndoSegmentHandle handle) {
        if (mtr == null || pageId == null || kind == null || handle == null) {
            throw new DatabaseValidationException("createChainPage args must not be null");
        }
        PageGuard g = newUndoEnvelope(mtr, pageId);
        UndoPage page = new UndoPage(mtr, g, pageSize);
        page.formatChainPage(kind, handle);
        return page;
    }

    /**
     * 打开已存在 UNDO 页。先校验 FIL 信封页类型，再校验每页 page-header 格式版本；具体 first/chain 角色由
     * UndoPage 访问器或 UndoLogSegmentAccess.open 再判断。版本守门必须位于本公共入口，因为 MVCC 可凭
     * RollPointer 直接打开 chain 页，不能依赖 first-page open 间接检查。
     */
    public UndoPage openUndoPage(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        if (mtr == null || pageId == null || mode == null) {
            throw new DatabaseValidationException("openUndoPage args must not be null");
        }
        requireOrdinaryAccess(mtr, pageId.spaceId());
        PageGuard g = mtr.getPage(pool, pageId, mode);
        FilePageHeader h = PageEnvelope.readHeader(g);
        if (h.pageType() != PageType.UNDO) {
            throw new UndoLogFormatException("page " + pageId + " is not an UNDO page: " + h.pageType());
        }
        UndoPage page = new UndoPage(mtr, g, pageSize);
        page.requireCurrentFormat();
        return page;
    }

    private PageGuard newUndoEnvelope(MiniTransaction mtr, PageId pageId) {
        requireOrdinaryAccess(mtr, pageId.spaceId());
        PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.UNDO);
        PageEnvelope.writeHeader(g, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.UNDO));
        return g;
    }

    private void requireOrdinaryAccess(MiniTransaction mtr, SpaceId spaceId) {
        if (tablespaceRegistry == null) {
            return;
        }
        mtr.acquireTablespaceLease(spaceId);
        tablespaceRegistry.require(spaceId);
    }
}
