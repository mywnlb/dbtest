package cn.zhangyis.db.storage.mtr;
import cn.zhangyis.db.storage.fil.io.PageStore;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;

/**
 * mini-transaction：短物理临界区的一致性边界（设计 §9）。memo 收集 page latch + buffer fix，
 * commit/rollback 时 LIFO 释放；savepoint 提前释放局部资源。
 *
 * <p>单线程拥有，非线程安全。简化点：commit 暂不产 redo / LSN / pageLSN / WAL 排序；
 * rollback 不撤销已写入 buffer 的页内容（MTR 无 content undo，留 redo/recovery 切片）。
 *
 * <p>并发约束：同页禁 S→X 锁升级（ReentrantReadWriteLock 无升级，会自死锁）；将写的页直接取 EXCLUSIVE。
 *
 * <p>构造与 commit/rollback/activate 为包内可见，仅由 {@link MiniTransactionManager} 编排，保证线程绑定不变量。
 */
public final class MiniTransaction {

    /** MTR 标识，用于诊断与 savepoint 归属校验。 */
    private final long id;

    /** 短临界区资源栈。 */
    private final MtrMemo memo = new MtrMemo();

    /** 生命周期状态；仅属主线程改。 */
    private MiniTransactionState state = MiniTransactionState.NEW;

    /** 本 MTR 的 redo 收集器（随 MTR 生命周期，挂到所获取页的 PageWriteListener）。 */
    private final MtrRedoCollector collector = new MtrRedoCollector();

    /** redo 日志管理器（由 Manager 注入，commit 时 append 本 MTR 收集的记录并取得 endLsn）。 */
    private final RedoLogManager redoLogManager;

    /** 与截断服务共享的表空间 operation lease 控制器。 */
    private final TablespaceAccessController accessController;

    MiniTransaction(long id, RedoLogManager redoLogManager, TablespaceAccessController accessController) {
        this.id = id;
        this.redoLogManager = redoLogManager;
        this.accessController = accessController;
    }

    /** 包内兼容构造，仅供不参与生命周期协作的 MTR 状态单测使用。 */
    MiniTransaction(long id, RedoLogManager redoLogManager) {
        this(id, redoLogManager, new TablespaceAccessController());
    }

    /** NEW→ACTIVE。由 Manager.begin 调用。 */
    void activate() {
        transitTo(MiniTransactionState.ACTIVE);
    }

    /** MTR id。 */
    public long id() {
        return id;
    }

    /** 当前状态。 */
    public MiniTransactionState state() {
        return state;
    }

    /**
     * 取已存在页，固定并按 mode 取 page latch，收进 memo 持到 commit/savepoint 释放。
     * 返回的 guard 由本 MTR 拥有生命周期，调用方读写但不要自行 close。
     *
     * @param pool buffer pool。
     * @param pageId 目标页。
     * @param mode S 或 X（将写则直接 X，禁 S→X 升级）。
     * @return 受控页句柄。
     */
    public PageGuard getPage(BufferPool pool, PageId pageId, PageLatchMode mode) {
        return fix(pool, pageId, mode, true, null);
    }

    /**
     * 取新页（不读盘，页须已被 PageStore.extend 分配），同样收进 memo。
     *
     * @param pool buffer pool。
     * @param pageId 新页。
     * @param mode 通常 X。
     * @return 受控页句柄。
     */
    public PageGuard newPage(BufferPool pool, PageId pageId, PageLatchMode mode, PageType pageType) {
        if (pageType == null) {
            throw new DatabaseValidationException("newPage pageType must not be null");
        }
        return fix(pool, pageId, mode, false, pageType);
    }

    /**
     * 在执行表空间状态检查前显式取得共享 operation lease。DiskSpaceManager 使用该入口形成
     * “取共享 lease → 重新读取 Registry 状态 → 操作”的原子准入顺序，消除先检查后等待截断的 TOCTOU。
     * 同一 MTR/SpaceId 重复调用为 no-op，lease 由 memo 在所有页资源之后释放。
     *
     * @param spaceId 要进入的表空间。
     */
    public void acquireTablespaceLease(SpaceId spaceId) {
        ensureActive();
        if (spaceId == null) {
            throw new DatabaseValidationException("tablespace lease space id must not be null");
        }
        if (!memo.hasTablespaceLease(spaceId)) {
            memo.pushTablespaceLease(accessController.acquireShared(spaceId), spaceId);
        }
    }

    private PageGuard fix(BufferPool pool, PageId pageId, PageLatchMode mode, boolean existing, PageType pageType) {
        ensureActive();
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        // lease 先于 page fix/latch 获取并先入 memo，LIFO 下最后释放；truncate 的 X lease 因此不会与旧 frame 访问交叉。
        acquireTablespaceLease(pageId.spaceId());
        // 同页 S→X 升级防护：page latch 是 ReentrantReadWriteLock，不支持读→写升级，若仍持该页 S latch 时再求 X，
        // BufferPool.acquire 的阻塞 latch.lock() 会让本线程自死锁。这里在取 latch 前把它转成可观测的领域异常。
        // X→S 降级与"已持 X 再取 X"放行：已持 X（!holds(X) 为假）说明本线程已是写者，再取 latch 可重入，不会死锁。
        if (mode == PageLatchMode.EXCLUSIVE
                && memo.holds(pageId, PageLatchMode.SHARED)
                && !memo.holds(pageId, PageLatchMode.EXCLUSIVE)) {
            throw new MtrStateException("S to X page latch upgrade is forbidden in one MTR: " + pageId);
        }
        PageGuard guard = existing ? pool.getPage(pageId, mode) : pool.newPage(pageId, mode);
        // 挂上 redo 收集器：此后该 guard 的物理写经 collector 译成 PAGE_BYTES（commit 时 append）。
        guard.attachWriteListener(collector);
        memo.pushPageGuard(guard, pageId, mode);
        // 新页：登记一条 PAGE_INIT（带页类型），并把该页标记为 touched（即便其后无字节写，commit 也会盖 pageLSN）。
        if (!existing) {
            collector.recordInit(pageId, pageType);
        }
        return guard;
    }

    /**
     * 选择性提前释放某已固定页的 page latch + buffer fix（不等 commit）。供 B+Tree 写路径 latch coupling
     * （crab，设计 §10.2）：下降时持父页 latch 到子页 latch 到手后，立即放掉父页，缩短内部页 latch 持有窗口、
     * 放开 root 处写并发。
     *
     * <p><b>已写页防护</b>（与 {@link #rollbackToSavepoint} 同一不变量）：commit 需据 {@code collector.touchedPages()}
     * 给写过的页盖 pageLSN（{@code guardFor}），若提前放掉已写页的 guard，盖 pageLSN 时会取不到它。故对 touched 页
     * 拒绝提前释放，抛 {@link MtrStateException}。乐观 crab 只对**内部导航页（S，从不写）**早释放，天然不 touched；
     * 本防护仅拦截误用（例如试图早释放已修改的 leaf）。
     *
     * @param pageId 待释放 guard 所在页（用于 touched 判定与诊断）。
     * @param guard  本 MTR memo 仍持有的 page guard（按身份匹配）。
     */
    public void releaseLatch(PageId pageId, PageGuard guard) {
        ensureActive();
        if (pageId == null || guard == null) {
            throw new DatabaseValidationException("releaseLatch pageId/guard must not be null");
        }
        if (collector.touchedPages().contains(pageId)) {
            throw new MtrStateException("cannot early-release a written (touched) page latch: " + pageId);
        }
        memo.release(guard);
    }

    /** 记录当前 memo 深度为保存点。 */
    public MtrSavepoint savepoint() {
        ensureActive();
        return new MtrSavepoint(id, memo.depth());
    }

    /**
     * 释放保存点之后获取的 latch/fix（§9.2 提前释放局部 latch）。不撤销页内容；释放的页若写过仍标脏。
     * 建议只对未修改页使用。
     *
     * @param savepoint 本 MTR 的保存点。
     */
    public void rollbackToSavepoint(MtrSavepoint savepoint) {
        ensureActive();
        if (savepoint == null) {
            throw new DatabaseValidationException("savepoint must not be null");
        }
        if (savepoint.mtrId() != id) {
            throw new MtrStateException("savepoint does not belong to this mini transaction: "
                    + savepoint.mtrId() + " vs " + id);
        }
        // 不变量：已写过（touched）的页 guard 不允许被 savepoint 回滚释放，否则 commit 盖 pageLSN 时 guardFor 取不到它。
        // 把现有 Javadoc 的「建议只对未修改页使用」升级为强制不变量。
        for (PageId pid : memo.pageIdsAbove(savepoint.depth())) {
            if (collector.touchedPages().contains(pid)) {
                throw new MtrStateException("cannot release a written (touched) page in savepoint rollback: " + pid);
            }
        }
        memo.releaseTo(savepoint.depth());
    }

    /**
     * ACTIVE→COMMITTING→（append redo 取 endLsn → 给 touched 页盖 pageLSN → LIFO 释放 memo）→COMMITTED。
     *
     * <p>WAL/LSN 语义：先 {@code append} 本 MTR 收集的 redo records 取得 batch endLsn；再 {@code disable} collector，
     * 使随后的 pageLSN 盖戳写**不**进 redo（决策④：先分配 LSN 再盖 pageLSN，盖戳不入本批）；对 collector.touchedPages
     * 逐页 {@link PageEnvelope#stampPageLsn} 盖 endLsn（恢复幂等基线）；随后 LIFO 释放 memo（按 wrote 标脏）。
     * 只有释放完成后才关闭 redo range，保证 checkpoint 不会越过尚未发布到 Buffer Pool dirty view 的页面修改。
     */
    Lsn commit() {
        transitTo(MiniTransactionState.COMMITTING);
        LogRange range = redoLogManager.append(collector.records());
        Lsn endLsn = range.end();
        collector.disable();
        for (PageId pid : collector.touchedPages()) {
            PageEnvelope.stampPageLsn(memo.guardFor(pid), endLsn);
        }
        memo.releaseAll();
        redoLogManager.markClosed(range);
        transitTo(MiniTransactionState.COMMITTED);
        return endLsn;
    }

    /** ACTIVE→ROLLED_BACK，LIFO 释放 memo；不撤销已写入 buffer 的内容。 */
    void rollbackUncommitted() {
        transitTo(MiniTransactionState.ROLLED_BACK);
        memo.releaseAll();
    }

    private void transitTo(MiniTransactionState next) {
        state.validateTransitTo(next);
        state = next;
    }

    private void ensureActive() {
        if (state != MiniTransactionState.ACTIVE) {
            throw new MtrStateException("mini transaction not active: " + state);
        }
    }
}
