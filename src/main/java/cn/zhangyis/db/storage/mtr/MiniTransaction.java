package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.RedoAppendBudget;
import cn.zhangyis.db.storage.redo.RedoCapacityThrottle;
import cn.zhangyis.db.storage.redo.RedoRecord;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.util.List;

/**
 * mini-transaction：短物理临界区的一致性边界（设计 §9）。memo 收集 page latch + buffer fix，
 * commit/rollback 时 LIFO 释放；savepoint 提前释放局部资源。
 *
 * <p>单线程拥有，非线程安全。commit 会把 collector 中的 redo records append 到 {@link RedoLogManager}、
 * 取得 batch end LSN、给 touched 页盖 pageLSN，并在 dirty 发布后关闭 redo range；rollback 仍不撤销
 * 已写入 buffer 的页内容（MTR 无 content undo，依赖事务 undo/恢复路径兜底）。
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

    /** begin admission 已授权的不可变 redo 上界；commit 只读取，不在持页资源时追加预算。 */
    private final RedoAppendBudget redoBudget;
    /** admission 原子账本句柄；append 成功后转移给真实 current LSN，异常/rollback 由 memo 兜底释放。 */
    private final RedoCapacityThrottle.Reservation redoReservation;

    /**
     * page latch 全序例外嵌套深度。默认 MTR 获取不同页必须按 PageId 升序；B+Tree 已证明的 sibling/右邻局部路径
     * 可短暂进入例外作用域，作用域关闭后立即恢复默认守卫。
     */
    private int outOfOrderPageLatchScopeDepth;

    MiniTransaction(long id, RedoLogManager redoLogManager, TablespaceAccessController accessController,
                    RedoAppendBudget redoBudget, RedoCapacityThrottle.Reservation redoReservation) {
        this.id = id;
        this.redoLogManager = redoLogManager;
        this.accessController = accessController;
        this.redoBudget = redoBudget;
        this.redoReservation = redoReservation;
    }

    /** 包内兼容构造，仅供不参与生命周期协作的 MTR 状态单测使用。 */
    MiniTransaction(long id, RedoLogManager redoLogManager) {
        this(id, redoLogManager, new TablespaceAccessController(), RedoAppendBudget.testingUnbounded(),
                RedoCapacityThrottle.NO_OP.reserveAppendBudget(RedoAppendBudget.testingUnbounded()));
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

    /**
     * 把非 page 的短生命周期资源挂入 MTR memo，随 commit/rollback/savepoint 按 LIFO 释放。
     * 典型用途是空间预留、短期物理 lease 等与当前 MTR 同生共死的 guard；资源必须自身支持异常安全 close。
     *
     * @param resource 需要由 MTR 兜底释放的资源。
     */
    public void enlistResource(AutoCloseable resource) {
        ensureActive();
        memo.push(resource);
    }

    /**
     * 开启一个 page latch 全序例外作用域。该入口只用于已有局部死锁证明的路径，例如 B+Tree 同父 sibling merge
     * 或 FIL 右邻链维护；普通多页获取必须依赖默认 PageId 升序守卫。
     *
     * @param reason 例外理由，必须写清调用方依赖的无环不变量，便于 review 和诊断。
     * @return 必须用 try-with-resources 关闭的作用域 guard。
     */
    public MtrLatchOrderScope allowOutOfOrderPageLatch(String reason) {
        ensureActive();
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("out-of-order page latch reason must not be blank");
        }
        outOfOrderPageLatchScopeDepth++;
        return new MtrLatchOrderScope(this, reason);
    }

    /** 关闭由 {@link #allowOutOfOrderPageLatch(String)} 创建的作用域。 */
    void closeOutOfOrderPageLatchScope(String reason) {
        if (outOfOrderPageLatchScopeDepth <= 0) {
            throw new MtrStateException("out-of-order page latch scope underflow: " + reason);
        }
        outOfOrderPageLatchScopeDepth--;
    }

    /**
     * 进入 redo 分类作用域。该分类只标记本 MTR 后续 {@code PAGE_BYTES} 的语义来源，不改变持久 redo 格式；
     * 调用方必须用 try-with-resources 关闭，使嵌套分类按 LIFO 恢复。
     *
     * @param category scope 内普通页字节写的分类，不能为 {@link MtrRedoCategory#PAGE_INIT}。
     * @param reason   分类理由，需说明调用方的数据库语义，便于 review 和诊断。
     * @return 分类作用域 guard。
     */
    public MtrRedoCategoryScope enterRedoCategory(MtrRedoCategory category, String reason) {
        ensureActive();
        return collector.enterCategory(category, reason);
    }

    /**
     * 追加一条显式逻辑 redo record。该入口供 FSP/Undo 等模块在已经持有正确 MTR 资源边界时记录逻辑意图；
     * 它不会触发 PageGuard 字节写，也不会直接把任何页加入 touched 集合。若该逻辑 record 需要 pageLSN 幂等边界，
     * 调用方必须在同一 MTR 内保留对应物理页修改（例如 FSP page allocation 后紧跟 PAGE_INIT/metadata PAGE_BYTES）。
     *
     * @param record 将随本 MTR 一起持久化的 redo record。
     * @param category 本地诊断分类，不进入 redo 文件。
     * @param reason 追加原因，必须说明数据库语义。
     */
    public void appendLogicalRedo(RedoRecord record, MtrRedoCategory category, String reason) {
        ensureActive();
        collector.recordLogical(record, category, reason);
    }

    /**
     * 返回当前 MTR 已收集 redo 的诊断快照。该入口只暴露 collector 的本地分类信息，提交时仍只把
     * {@link cn.zhangyis.db.storage.redo.RedoRecord} 交给 redo manager，避免测试误认为分类已进入持久格式。
     */
    List<MtrRedoEntry> redoEntries() {
        return collector.entries();
    }

    private PageGuard fix(BufferPool pool, PageId pageId, PageLatchMode mode, boolean existing, PageType pageType) {
        ensureActive();
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        if (pageId == null || mode == null) {
            throw new DatabaseValidationException("page id/mode must not be null");
        }
        // lease 先于 page fix/latch 获取并先入 memo，LIFO 下最后释放；truncate 的 X lease 因此不会与旧 frame 访问交叉。
        acquireTablespaceLease(pageId.spaceId());
        enforcePageLatchOrder(pageId);
        // 同页 S/SX→X 升级防护：page latch 是 ReentrantReadWriteLock，不支持读→写升级；S 与 SHARED_EXCLUSIVE 都持
        // 该页 readLock，若仍持时再求 X，BufferPool.acquire 的阻塞 writeLock.lock() 会让本线程自死锁。这里在取 latch
        // 前转成可观测的领域异常。X→S 降级与"已持 X 再取 X"放行：已持 X 说明本线程已是写者，再取 latch 可重入，不会死锁。
        if (mode == PageLatchMode.EXCLUSIVE
                && (memo.holds(pageId, PageLatchMode.SHARED) || memo.holds(pageId, PageLatchMode.SHARED_EXCLUSIVE))
                && !memo.holds(pageId, PageLatchMode.EXCLUSIVE)) {
            throw new MtrStateException("S/SX to X page latch upgrade is forbidden in one MTR: " + pageId);
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
     * 独立多页 page latch 默认全序守卫。若当前 MTR 仍持有更大的 PageId，又请求一个尚未持有的更小 PageId，
     * 两个线程按相反顺序获取相同页集时会形成 hold-and-wait 环；这里在进入 page latch 等待前快速失败。
     */
    private void enforcePageLatchOrder(PageId pageId) {
        if (outOfOrderPageLatchScopeDepth > 0 || memo.holdsAnyPageLatch(pageId)) {
            return;
        }
        PageId highest = memo.highestHeldPageId();
        if (highest != null && MtrMemo.comparePageId(highest, pageId) > 0) {
            throw new MtrStateException("page latch order violation: held highest " + highest
                    + " before requesting lower " + pageId);
        }
    }

    /**
     * 选择性提前释放某已固定页的 page latch + buffer fix（不等 commit）。供 B+Tree 写路径 latch coupling
     * （crab，设计 §10.2）：下降时持父页 latch 到子页 latch 到手后，立即放掉父页，缩短内部页 latch 持有窗口、
     * 放开 root 处写并发。
     *
     * <p><b>已写页防护</b>（与 {@link #rollbackToSavepoint} 同族不变量）：commit 需据 {@code collector.touchedPages()}
     * 给写过的页盖 pageLSN（{@code guardFor} 定位该页的 X guard），若提前放掉这个 X guard，盖 pageLSN 时会取不到它。
     * 关键在于**只有 X guard 参与盖戳**：touched 页必由某 X guard 写过，只要那个 X guard 仍在 memo，盖戳就成立。
     * 因此防护精确到「拒绝提前释放 touched 页的 <b>EXCLUSIVE</b> guard」——释放同页的 SHARED guard 永远安全。
     * 这对同一 MTR 内多算子协作是必要的：后一算子的乐观 crab 会以 SHARED 重开（可重入）前一算子已写并 X 持有的祖先/root 页，
     * 再 crab 释放该 SHARED guard；此时页虽 touched，但前一算子的 X guard 仍在 memo，释放 SHARED 不破坏盖戳。
     * 乐观 crab 只释放内部/root 的 SHARED guard（leaf 的 X guard 从不早释放），故永不触发本防护；防护仅拦截「早释放已写页 X guard」的误用。
     *
     * @param pageId 待释放 guard 所在页（用于 touched 判定与诊断）。
     * @param guard  本 MTR memo 仍持有的 page guard（按身份匹配）。
     */
    public void releaseLatch(PageId pageId, PageGuard guard) {
        ensureActive();
        if (pageId == null || guard == null) {
            throw new DatabaseValidationException("releaseLatch pageId/guard must not be null");
        }
        if (collector.touchedPages().contains(pageId) && memo.isExclusiveGuard(guard)) {
            throw new MtrStateException("cannot early-release the exclusive latch of a written page: " + pageId);
        }
        memo.release(guard);
    }

    /**
     * 返回本 MTR 已持有的 X guard，未持有则返回 null。该入口供同一 MTR 写后校验读取复用 touched 页；调用方不得
     * close/release 返回 guard，生命周期仍归 memo，避免为了短读重复 fix 后被 savepoint 误判为释放写页。
     */
    public PageGuard retainedExclusivePage(PageId pageId) {
        ensureActive();
        if (pageId == null) {
            throw new DatabaseValidationException("retained page id must not be null");
        }
        return memo.holds(pageId, PageLatchMode.EXCLUSIVE) ? memo.guardFor(pageId) : null;
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
        // persisted records 只冻结一次：预算结算、LSN 分配和 repository 编码必须观察完全相同的列表。
        List<RedoRecord> persistedRecords = collector.records();
        redoBudget.requireCovers(persistedRecords);
        LogRange range = redoLogManager.append(persistedRecords);
        // append 已把 actual bytes 纳入 current LSN；立即解除 admission 账本，避免直到 page 发布后仍双计数。
        redoReservation.transferToAppend();
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
