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

    /**
     * 创建 {@code UndoPageAccess}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     */
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
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code createFirstPage} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param txnId 参与 {@code createFirstPage} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code createFirstPage} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param kind 选择 {@code createChainPage} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param handle 调用方持有的 {@code UndoSegmentHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code createChainPage} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code openUndoPage} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoPage openUndoPage(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || pageId == null || mode == null) {
            throw new DatabaseValidationException("openUndoPage args must not be null");
        }
        requireOrdinaryAccess(mtr, pageId.spaceId());
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        PageGuard g = mtr.getPage(pool, pageId, mode);
        FilePageHeader h = PageEnvelope.readHeader(g);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (h.pageType() != PageType.UNDO) {
            throw new UndoLogFormatException("page " + pageId + " is not an UNDO page: " + h.pageType());
        }
        UndoPage page = new UndoPage(mtr, g, pageSize);
        page.requireCurrentFormat();
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return page;
    }

    /**
     * 根据调用参数构造 {@code newUndoEnvelope} 对应的Undo 日志领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code newUndoEnvelope} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
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
