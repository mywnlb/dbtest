package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.btree.BTreeDeleteResult;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeRootSnapshotService;
import cn.zhangyis.db.storage.btree.BTreeSecondaryRemovalResult;
import cn.zhangyis.db.storage.btree.SecondaryEntryRemovalStatus;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.api.lob.LobFreeBatchPlan;
import cn.zhangyis.db.storage.api.lob.LobFreeTarget;
import cn.zhangyis.db.storage.api.lob.LobStorage;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordType;
import cn.zhangyis.db.storage.undo.SecondaryUndoMutation;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 单线程 purge 协调器（设计 §5.7/§7.7）。{@link #runBatch} 同步处理一批已提交 undo：
 * <ol>
 *   <li>按 purge boundary（= {@link TransactionSystem#purgeLowWaterNo()}，最老 live ReadView 低水位）FIFO 处理 committed
 *       history：UPDATE 先用聚簇版本链证明旧 secondary identity 可删，DELETE 先删全部 secondary 再删聚簇；
 *       该 log 全部任务完成后回收 undo 段并释放 slot。遇队首不满足事务号/可见性边界即停。</li>
 * </ol>
 *
 * <p><b>Latch 纪律</b>（同 {@link MvccReader}）：先用短只读 MTR 取得 first-page 持久 logical head + 段 handle，
 * 再沿 {@code prevRollPointer} 每条用独立短读 MTR 物化 DELETE_MARK task；每次提交都立即释放 undo latch/fix。
 * 整条链校验完成后才逐条开启单索引 MTR，secondary 与 clustered 物理删除不跨树持 latch；最后由
 * {@link UndoSegmentFinalizer} 在单一 MTR 原子转移 cache/free/drop owner、更新 persistent history 与 page3。
 * 任一时刻只持 undo 或 index 之一，也不会把超过 Buffer Pool 容量的整条 undo 页链同时 fixed。
 *
 * <p><b>per-entry 失败边界</b>：committed 队列先 peek，只有 index tasks 与 finalization 全部成功后才按 expected
 * identity 摘队首。task 中途失败保留 page3 COMMITTED slot，重启可重建并 stale-skip 已完成任务；finalization 的
 * cache/free/drop + page3 owner 转移同批提交后，即使在内存 complete 前 crash，恢复也只依据持久 owner 重建。
 *
 * <p><b>当前范围</b>：协调器自身同步串行，生产由单 daemon driver 周期调用；history 是 page3/undo first-page
 * 持久链的运行时投影，恢复会重建 affected-table 引用并在 OPEN_TRAFFIC 前真实执行 RESUME_PURGE。
 * 仍未实现多 worker、多 rseg 与 purge→undo tablespace truncate 自动调度。
 */
public final class PurgeCoordinator implements PurgeTarget {

    /** undo 短读与 index 写 MTR 来源；每次循环返回前释放全部 latch/fix。 */
    private final MiniTransactionManager mgr;
    /** live ReadView purge low-water 的权威来源。 */
    private final TransactionSystem system;
    /** page3 COMMITTED slots 的内存 FIFO 投影。 */
    private final HistoryList history;
    /** 逐 pointer 读取 committed undo logical chain 的入口。 */
    private final UndoLogSegmentAccess undoAccess;
    /** tasks 完成后原子 cache/free/drop segment + 转移 page3 owner 的终态协作者。 */
    private final UndoSegmentFinalizer finalizer;
    /** 物理删除仍匹配 creator/roll-pointer 的 delete-marked 聚簇记录。 */
    private final SplitCapableBTreeIndexService btree;
    /** 按 undo identity 解析 exact-version 表级索引聚合；legacy 单聚簇构造映射为空 secondary 集合。 */
    private final UndoTargetMetadataResolver targetResolver;
    /** secondary 结构删除前刷新 root page header level；legacy 单聚簇模式允许为空。 */
    private final BTreeRootSnapshotService rootSnapshots;
    /** 与前台 DML/rollback 共享的行 guard；purge 只零等待尝试，busy 不移动 history。 */
    private final PurgeDmlRowGuardManager rowGuards;
    /** 从当前聚簇版本到目标 undo 的较新版本引用证明器；legacy 单聚簇模式允许为空。 */
    private final SecondaryPurgeSafetyChecker secondarySafety;
    /** 消费 LV purge-old ownership 的页链服务；legacy/无 LOB binding 模式允许为空，但遇 ownership 必须 fail-closed。 */
    private final LobStorage lobStorage;
    /** 仅包内测试可替换的 task-commit 故障接缝；生产始终为 no-op。 */
    private PurgeProgressFaultInjector faultInjector = PurgeProgressFaultInjector.NO_OP;

    /**
     * 构造只处理显式单聚簇索引、没有 secondary mutation 的兼容 purge 协调器。
     *
     * @param mgr            undo 短读与聚簇物理删除写 MTR 的工厂。
     * @param system         live ReadView/active transaction 共同决定的 purge boundary 权威来源。
     * @param history        persistent committed history 的运行时 FIFO 投影。
     * @param undoAccess     按 first page/logical pointer 读取 undo record 的访问端口。
     * @param finalizer      全部物理任务完成后原子摘链并回收 undo owner 的终结器。
     * @param btree          delete-marked 聚簇记录的物理删除服务。
     * @param clusteredIndex 低层实例唯一显式配置的聚簇索引；不得出现 secondary tail。
     * @throws DatabaseValidationException 任一依赖为空或 clusteredIndex 非聚簇时抛出。
     */
    public PurgeCoordinator(MiniTransactionManager mgr, TransactionSystem system, HistoryList history,
                            UndoLogSegmentAccess undoAccess, UndoSegmentFinalizer finalizer,
                            SplitCapableBTreeIndexService btree,
                            BTreeIndex clusteredIndex) {
        this(mgr, system, history, undoAccess, finalizer, btree,
                targetResolver(legacyResolver(clusteredIndex)), null, null, null, null);
    }

    /**
     * 构造按 undo table/index identity 动态解析聚簇索引、但仍不消费 secondary tail 的兼容协调器。
     *
     * @param mgr           undo/index MTR 工厂。
     * @param system        purge eligibility 边界来源。
     * @param history       committed history 运行时投影。
     * @param undoAccess    undo logical chain 读取端口。
     * @param finalizer     history/undo owner 原子终结器。
     * @param btree         聚簇物理删除服务。
     * @param indexResolver 按每条 undo 固定 table/index id 解析 exact clustered descriptor 的端口。
     * @throws DatabaseValidationException 任一依赖/解析器为空时抛出；若记录含 secondary tail，执行期会 fail-closed。
     */
    public PurgeCoordinator(MiniTransactionManager mgr, TransactionSystem system, HistoryList history,
                            UndoLogSegmentAccess undoAccess, UndoSegmentFinalizer finalizer,
                            SplitCapableBTreeIndexService btree,
                            IndexMetadataResolver indexResolver) {
        this(mgr, system, history, undoAccess, finalizer, btree,
                targetResolver(indexResolver), null, null, null, null);
    }

    /**
     * 构造表级生产 purge 协调器；secondary purge 使用 exact-version metadata、共享 row guard、root snapshot 与版本链证明。
     *
     * @param mgr             undo/index 独立短 MTR 的统一工厂。
     * @param system          active transaction 与 live ReadView purge boundary 的权威来源。
     * @param history         persistent history FIFO 与 affected-table barrier 的运行时 owner。
     * @param undoAccess      按 logical pointer 读取 committed undo 的端口。
     * @param finalizer       全部 task 完成后原子回收 undo owner 并发布 history head removal 的协作者。
     * @param btree           secondary/clustered 精确物理删除的 B+Tree 服务。
     * @param targetResolver  按 undo 固定 table/index identity 解析 exact-version 表索引与 LOB binding 的端口。
     * @param rootSnapshots   每次结构删除前从稳定 root 页刷新 level 的服务；与后两项必须同时配置或同时为空。
     * @param rowGuards       与前台 DML/rollback 共用的 1024 分片行物理 guard；purge 仅零等待取得。
     * @param secondarySafety 从当前聚簇版本链证明旧 secondary identity 是否仍被较新版本需要的服务。
     * @throws DatabaseValidationException 必需依赖缺失，或 secondary 三项只配置部分时抛出。
     */
    public PurgeCoordinator(MiniTransactionManager mgr, TransactionSystem system, HistoryList history,
                            UndoLogSegmentAccess undoAccess, UndoSegmentFinalizer finalizer,
                            SplitCapableBTreeIndexService btree,
                            UndoTargetMetadataResolver targetResolver,
                            BTreeRootSnapshotService rootSnapshots,
                            PurgeDmlRowGuardManager rowGuards,
                            SecondaryPurgeSafetyChecker secondarySafety) {
        this(mgr, system, history, undoAccess, finalizer, btree, targetResolver, rootSnapshots,
                rowGuards, secondarySafety, null);
    }

    /**
     * 构造完整生产 purge 协调器，使 secondary 清理、聚簇物理删除、LOB ownership 消费和 undo 进度持久化
     * 共享同一 exact-version metadata 与 row guard 边界。{@code lobStorage} 可以为空以兼容没有 LOB 功能的旧装配；
     * 但只要 undo LV tail 出现 purge-old ownership，执行期就会 fail-closed，绝不会静默泄漏页链。
     *
     * @param mgr             undo/index/记录进度短 MTR 的统一工厂和 redo admission 入口。
     * @param system          active transaction 与 live ReadView 共同形成的 purge boundary 权威来源。
     * @param history         page3 committed history 的运行时 FIFO 投影和 affected-table barrier owner。
     * @param undoAccess      按 first-page/logical pointer 读取或 EXCLUSIVE 更新 undo segment 的访问端口。
     * @param finalizer       logical head 已为空后原子摘除 history、释放 slot/segment owner 的终结器。
     * @param btree           secondary/clustered 精确物理删除服务；每棵树使用独立短 MTR。
     * @param targetResolver  按 undo 固定 table/index identity 返回 exact-version 索引与 LOB binding 的端口。
     * @param rootSnapshots   结构删除前读取 root level 的服务；必须与 rowGuards、secondarySafety 同时配置或同时为空。
     * @param rowGuards       与前台 DML/rollback 共享的行 guard；purge 只零等待，busy 时保留当前持久 head。
     * @param secondarySafety 二级旧 identity 删除前沿聚簇版本链证明不再被较新版本使用的协作者。
     * @param lobStorage      执行计划化批量 LOB free 的服务；无 LOB 功能时可为空，遇 LV ownership 时拒绝执行。
     * @throws DatabaseValidationException 必需协作者缺失，或 secondary 三项只配置部分时抛出；不访问 history/页面。
     */
    public PurgeCoordinator(MiniTransactionManager mgr, TransactionSystem system, HistoryList history,
                            UndoLogSegmentAccess undoAccess, UndoSegmentFinalizer finalizer,
                            SplitCapableBTreeIndexService btree,
                            UndoTargetMetadataResolver targetResolver,
                            BTreeRootSnapshotService rootSnapshots,
                            PurgeDmlRowGuardManager rowGuards,
                            SecondaryPurgeSafetyChecker secondarySafety,
                            LobStorage lobStorage) {
        if (mgr == null || system == null || history == null || undoAccess == null || finalizer == null
                || btree == null || targetResolver == null) {
            throw new DatabaseValidationException("purge coordinator collaborators must not be null");
        }
        if ((rootSnapshots == null) != (rowGuards == null)
                || (rootSnapshots == null) != (secondarySafety == null)) {
            throw new DatabaseValidationException(
                    "secondary purge root snapshots, row guards and safety checker must be configured together");
        }
        this.mgr = mgr;
        this.system = system;
        this.history = history;
        this.undoAccess = undoAccess;
        this.finalizer = finalizer;
        this.btree = btree;
        this.targetResolver = targetResolver;
        this.rootSnapshots = rootSnapshots;
        this.rowGuards = rowGuards;
        this.secondarySafety = secondarySafety;
        this.lobStorage = lobStorage;
    }

    /**
     * 执行一个可测试 purge 批次：按持久物理链 head 顺序处理至多 {@code maxLogs} 条 committed update/delete undo log。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验正批次上限，非法请求不读取 history 或页面。</li>
     *     <li>初始化统计并循环 peek 物理队首；空队列或 transaction/read-view boundary 不满足时立即停止，绝不越过队首。</li>
     *     <li>处理一条 eligible log；row guard busy 时记录 deferred 并保留队首，完成时累加 secondary/clustered 删除数。</li>
     *     <li>返回只描述本批已完成/延后事实的不可变统计，不以统计值反向修改 history。</li>
     * </ol>
     *
     * @param maxLogs 本批最多完成 finalization 的 committed undo log 正数上限。
     * @return 完成日志、物理删除 clustered/secondary 数和 row-guard 延后数的统计。
     * @throws DatabaseValidationException {@code maxLogs <= 0} 时抛出。
     * @throws RuntimeException undo/metadata/版本链/B+Tree/finalization 任一步失败时抛出；失败日志保持在 history 队首。
     */
    public PurgeSummary runBatch(int maxLogs) {
        // 1. 批次 admission 先于 history/IO 访问。
        if (maxLogs <= 0) {
            throw new DatabaseValidationException("purge maxLogs must be positive: " + maxLogs);
        }
        // 2. 只从物理队首推进；purge eligibility 不满足时不能跳到后续 transaction no。
        int purgedLogs = 0;
        int removedClustered = 0;
        int removedSecondary = 0;
        int deferredLogs = 0;
        while (purgedLogs < maxLogs) {
            Optional<HistoryEntry> headOpt = history.peekCommitted();
            if (headOpt.isEmpty()) {
                break;
            }
            HistoryEntry head = headOpt.get();
            if (!system.isPurgeEligible(head.transactionNo(), head.creatorTrxId())) {
                break; // 不越过物理 head；creator 非 active、提交号边界与 live ReadView 可见性必须同时成立
            }
            // 3. 单 log 内 secondary/clustered task 与 finalization 全部完成才计为 purged；busy 明确延后。
            PurgeLogOutcome outcome = purgeCommittedLog(head);
            if (!outcome.completed()) {
                deferredLogs++;
                break;
            }
            removedClustered += outcome.removedClustered();
            removedSecondary += outcome.removedSecondary();
            purgedLogs++;
        }
        // 4. 统计是诊断快照，history 已由各成功 finalization 自身原子发布。
        return new PurgeSummary(purgedLogs, removedClustered, removedSecondary, deferredLogs);
    }

    /**
     * 逐条处理一条 committed UPDATE undo log。每条记录的 secondary/clustered 清理完成后，旧 LOB ownership 与
     * logical-head 前移在同一 MTR 中提交；因此 crash 重启只会从仍持久可达的 head 继续，不会再次释放已消费链。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>循环短读 first-page 的 committed logical head；空头表示全部记录进度已持久化，可进入 finalization。</li>
     *     <li>只物化当前 head record 及其直接前驱，冻结 exact-version secondary、LOB 和下一 logical-head 计划。</li>
     *     <li>表级模式零等待取得共享 row guard，在 guard 内按 secondary、clustered 顺序执行幂等单树物理任务。</li>
     *     <li>在独立记录进度 MTR 中先预检 logical-head CAS，再批量释放 purge-old LOB，最后写入前驱 head 并提交。</li>
     *     <li>持久 head 为空后取得 history removal lease，由 finalizer 原子回收 undo owner 并发布队首摘除。</li>
     * </ol>
     *
     * @param entry 当前物理 history 队首；creator、slot、first-page identity 必须与 page3/undo owner 一致。
     * @return completed 时携带本 log 实际删除计数；row guard busy 时返回 deferred，已提交的记录级进度不会回退。
     * @throws DatabaseValidationException LV ownership 缺少 LOB wiring/binding 或表级任务缺共享 row guard 时抛出。
     * @throws UndoLogFormatException undo state、creator、head、前驱单调性、metadata 或 old image 损坏时抛出。
     * @throws PurgeProgressException LOB free 或 logical-head 写入已越过无 content-undo 边界后失败时抛出，worker 必须停止。
     */
    private PurgeLogOutcome purgeCommittedLog(HistoryEntry entry) {
        int removedClustered = 0;
        int removedSecondary = 0;
        while (true) {
            // 1. 每轮重新读取持久 head，使 crash 重试和本轮已提交 progress 使用完全相同的权威入口。
            UndoLogicalHead head = readLogicalChainStart(entry).head();
            if (head.isEmpty()) {
                break;
            }

            // 2. 单条 task 冻结当前 record、直接前驱和所有 ownership；不把整条大事务 undo 链装入内存。
            PurgeRecordTask task = buildHeadTask(entry, head);
            if (rowGuards == null) {
                requireLegacyTaskIsSelfContained(task);
                removedClustered += purgeClustered(entry, task);
                persistRecordProgress(entry, head, task);
                continue;
            }

            // 3. purge 不等待前台 DML/rollback；busy 时保留当前持久 head，下一批从同一 record 继续。
            requireTableLevelWiring(task);
            Optional<PurgeDmlRowGuard> guard = rowGuards.tryAcquireForPurge(
                    task.record().tableId(), task.clusterKey());
            if (guard.isEmpty()) {
                return PurgeLogOutcome.deferred(removedClustered, removedSecondary);
            }
            try (PurgeDmlRowGuard ignored = guard.orElseThrow()) {
                for (SecondaryTask secondaryTask : task.secondaryTasks()) {
                    SecondaryPurgeDecision decision = secondarySafety.evaluate(task.record(), task.rollPointer(),
                            task.target().tableIndexes(), secondaryTask.metadata());
                    if (decision == SecondaryPurgeDecision.REMOVE) {
                        removedSecondary += purgeSecondary(secondaryTask);
                    }
                }
                if (task.deleteClustered()) {
                    removedClustered += purgeClustered(entry, task);
                }
                // 4. row guard 覆盖索引清理到 head progress，防止同一主键在 ownership 交接窗口被前台重写。
                persistRecordProgress(entry, head, task);
            }
        }

        // 5. finalizer 只接收 EMPTY logical head；记录级进度未完全持久化时不得释放 undo segment/slot。
        try (HistoryList.HeadRemovalLease lease = history.beginHeadRemoval(entry)) {
            finalizer.finalizePurgedHistory(entry, lease);
        }
        return PurgeLogOutcome.completed(removedClustered, removedSecondary);
    }

    /**
     * 短读并校验 committed history entry 当前持久 logical head；返回对象不持有 page guard 或 latch。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建只读 MTR，以 SHARED 模式打开 history first page。</li>
     *     <li>核对 creator、UPDATE kind 与 COMMITTED 状态，拒绝 slot/history 指向错误 owner。</li>
     *     <li>复制 logical head 后提交读 MTR，释放全部 undo page 资源。</li>
     * </ol>
     *
     * @param entry 当前 history 队首，提供 first page 与 creator identity。
     * @return 当前持久 logical head 的不可变快照；可以是 {@link UndoLogicalHead#EMPTY}。
     * @throws UndoLogFormatException segment owner、kind 或 state 与 committed history 不一致时抛出。
     * @throws RuntimeException undo page 读取或只读 MTR 提交失败时抛出；释放失败保留为 suppressed。
     */
    private LogicalChainStart readLogicalChainStart(HistoryEntry entry) {
        // 1. 只读 MTR 不跨越 record/index/LOB 操作，确保 undo latch 生命周期最短。
        MiniTransaction read = mgr.beginReadOnly();
        try {
            UndoLogSegment segment = undoAccess.open(read, entry.undoFirstPageId(), PageLatchMode.SHARED);
            // 2. history/page3 的 creator 与 undo first-page state 必须描述同一个 committed UPDATE owner。
            if (!segment.creatorTransactionId().equals(entry.creatorTrxId())) {
                throw new UndoLogFormatException("purge history creator transaction "
                        + entry.creatorTrxId().value() + " != undo segment creator "
                        + segment.creatorTransactionId().value());
            }
            if (segment.undoKind() != UndoLogKind.UPDATE || !segment.isCommitted()) {
                throw new UndoLogFormatException("purge history must reference a COMMITTED UPDATE undo log");
            }
            // 3. 提交后只返回值对象，禁止后续 B+Tree/LOB 工作继承 undo page latch/fix。
            LogicalChainStart result = new LogicalChainStart(segment.logicalHead());
            mgr.commit(read);
            return result;
        } catch (RuntimeException error) {
            rollbackReadMtr(read, error);
            throw error;
        }
    }

    /**
     * 从持久 head 构造一条可执行 purge task，并只读取直接前驱来冻结下一 logical-head 的 exact-version 解码上下文。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 head pointer 读取当前 record/target，核对 undoNo、creator 和 UPDATE/DELETE 类型。</li>
     *     <li>从旧 image 与 secondary mutation 构造按 index id 排序的完整二级 physical key。</li>
     *     <li>从 LV purge-old ownership 和 authoritative table LOB binding 构造批量 free plan。</li>
     *     <li>读取直接前驱并校验 undoNo 严格下降，冻结下一 head 及其 key/schema；NULL 前驱映射为空头。</li>
     * </ol>
     *
     * @param entry 当前 history owner，提供 first page 和 creator identity。
     * @param head  first-page header 中刚读取的非空持久 logical head。
     * @return 不持有页资源的单记录 purge 任务，包含所有索引、LOB 与 head-progress 输入。
     * @throws DatabaseValidationException head 为空、LV 缺 LOB wiring/binding 或 ownership ordinal/type 无效时抛出。
     * @throws UndoLogFormatException head pointer、creator、record type 或前驱 undoNo 不满足逻辑链不变量时抛出。
     */
    private PurgeRecordTask buildHeadTask(HistoryEntry entry, UndoLogicalHead head) {
        // 1. 当前 record 必须与 first-page 公开的 undoNo/pointer 和 history creator 完全一致。
        if (head.isEmpty()) {
            throw new DatabaseValidationException("purge head task requires a non-empty logical head");
        }
        ResolvedUndo current = readLogicalRecord(entry.undoFirstPageId(), head.rollPointer());
        UndoRecord record = current.record();
        if (!record.undoNo().equals(head.undoNo())) {
            throw new UndoLogFormatException("persistent purge head undoNo " + head.undoNo().value()
                    + " resolves to " + record.undoNo().value());
        }
        if (!record.transactionId().equals(entry.creatorTrxId())) {
            throw new UndoLogFormatException("purge undo record transaction "
                    + record.transactionId().value() + " != history creator " + entry.creatorTrxId().value());
        }
        if (record.type() != UndoRecordType.UPDATE_ROW && record.type() != UndoRecordType.DELETE_MARK) {
            throw new UndoLogFormatException("committed UPDATE undo segment contains unsupported purge record: "
                    + record.type());
        }

        // 2. old image 与 exact-version secondary layout 共同产生完整 physical identity；这里只冻结值，不改树。
        List<SecondaryTask> secondaryTasks = new ArrayList<>();
        for (SecondaryUndoMutation mutation : record.secondaryMutations()) {
            SecondaryIndexMetadata metadata = current.target().tableIndexes().requireSecondary(mutation.indexId());
            LogicalRecord oldRow = new LogicalRecord(current.target().tableIndexes().schemaVersion(),
                    record.oldColumnValues(), false,
                    cn.zhangyis.db.storage.record.format.RecordType.CONVENTIONAL,
                    record.oldHiddenColumns());
            SearchKey physicalKey = metadata.layout().physicalKey(metadata.layout().toEntry(oldRow, true));
            secondaryTasks.add(new SecondaryTask(metadata, physicalKey));
        }

        // 3. LV 只授权 purge-old；reference identity 必须与 exact table LOB segment 交叉校验并冻结 redo workload。
        Optional<LobFreeBatchPlan> lobFreePlan = planPurgeLobFree(record, current.target());

        // 4. 直接前驱决定本条记录完成后的持久 head；跨表事务必须使用前驱自身 exact-version key/schema 校验。
        UndoLogicalHead targetHead = UndoLogicalHead.EMPTY;
        BTreeIndex targetHeadIndex = current.target().clusteredIndex();
        if (!record.prevRollPointer().isNull()) {
            ResolvedUndo predecessor = readLogicalRecord(entry.undoFirstPageId(), record.prevRollPointer());
            if (!predecessor.record().transactionId().equals(entry.creatorTrxId())
                    || predecessor.record().undoNo().value() >= record.undoNo().value()) {
                throw new UndoLogFormatException("purge undo predecessor is not owned by creator or strictly older: current="
                        + record.undoNo().value() + ", predecessor=" + predecessor.record().undoNo().value());
            }
            targetHead = new UndoLogicalHead(predecessor.record().undoNo(), record.prevRollPointer());
            targetHeadIndex = predecessor.target().clusteredIndex();
        }
        return new PurgeRecordTask(record, head.rollPointer(), current.target(),
                new SearchKey(record.clusterKey()), List.copyOf(secondaryTasks),
                record.type() == UndoRecordType.DELETE_MARK, lobFreePlan, targetHead, targetHeadIndex);
    }

    /**
     * 把单条 undo record 的 purge-old LV ownership 投影为 authoritative segment 下的批量 LOB free 计划。
     * rollback-new ownership 属于事务回滚路径，committed purge 必须忽略；没有 purge-old ownership 时不要求接线 LOB。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>筛选声明 purge-old 的 LV 条目；空集合返回空计划且不访问 schema/segment。</li>
     *     <li>要求生产已注入 LobStorage 且 exact-version target 提供 LOB segment，拒绝从 reference 猜授权。</li>
     *     <li>按 ordinal 从 exact schema 取得列类型，并从 old image 取得 external envelope。</li>
     *     <li>交给 LobStorage 校验类型、segment identity、重复 reference 并冻结动态 redo workload。</li>
     * </ol>
     *
     * @param record 当前 UPDATE/DELETE undo record；old image 与 LV 已通过领域对象基本形状校验。
     * @param target record 固定 table/index identity 解析出的 exact-version 索引与 LOB binding。
     * @return 无 purge-old ownership 时为空；否则返回同一 authoritative segment 的不可变批量计划。
     * @throws DatabaseValidationException 缺 LOB storage/binding、ordinal 越界或 old image 不是 external 时抛出。
     * @throws RuntimeException external envelope 类型、segment identity 或重复 ownership 校验失败时由 LobStorage 抛出。
     */
    private Optional<LobFreeBatchPlan> planPurgeLobFree(UndoRecord record, UndoTargetMetadata target) {
        // 1. committed purge 只消费旧版本 ownership；新版本 ownership 在提交后仍由当前聚簇记录持有。
        List<cn.zhangyis.db.storage.undo.LobVersionOwnership> ownerships = record.lobVersionOwnerships().stream()
                .filter(cn.zhangyis.db.storage.undo.LobVersionOwnership::purgeOldValue)
                .toList();
        if (ownerships.isEmpty()) {
            return Optional.empty();
        }
        // 2. 物理释放授权只能来自 exact DD/table binding；legacy 无接线实例必须显式失败而不是泄漏。
        if (lobStorage == null || target.lobSegment().isEmpty()) {
            throw new DatabaseValidationException(
                    "purge-old LOB ownership requires LobStorage and exact table LOB segment binding");
        }
        // 3. ordinal 同时定位 exact 列类型和 old image envelope；任何形状漂移都在写 MTR 前终止。
        List<LobFreeTarget> targets = new ArrayList<>(ownerships.size());
        for (var ownership : ownerships) {
            int ordinal = ownership.columnOrdinal();
            if (ordinal >= target.clusteredIndex().schema().columns().size()
                    || ordinal >= record.oldColumnValues().size()
                    || !(record.oldColumnValues().get(ordinal) instanceof ColumnValue.ExternalValue external)) {
                throw new DatabaseValidationException(
                        "purge-old LOB ownership does not match exact schema/old image at ordinal " + ordinal);
            }
            targets.add(new LobFreeTarget(ordinal,
                    target.clusteredIndex().schema().column(ordinal).type(), external));
        }
        // 4. LobStorage 冻结稳定顺序、segment 交叉校验和整批 redo 上界；此阶段仍不读取或修改 LOB 页。
        return Optional.of(lobStorage.planFreeBatch(target.lobSegment().orElseThrow(), targets));
    }

    /**
     * 在单个记录级 MTR 中消费 purge-old LOB ownership，并把持久 logical head 从 expected 推进到直接前驱。
     * 所有可预见 CAS/target 校验都在第一次 LOB/FSP 修改前完成；一旦越过物理边界，任何失败均升级为致命异常。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按固定 header 余量与可选 LOB 批量计划申请动态 redo admission，创建独立写 MTR。</li>
     *     <li>EXCLUSIVE 打开 undo first page，并预检 expected→target CAS 与 target record，不修改 header。</li>
     *     <li>进入显式 latch-order scope，先释放全部 purge-old LOB 页链，再写 logical-head header。</li>
     *     <li>提交 redo/pageLSN/dirty 后触发稳定故障接缝；重启将从新 head 继续，不会重复释放旧链。</li>
     * </ol>
     *
     * @param entry    当前 committed history owner，用于定位 undo first page 和致命异常诊断。
     * @param expected 当前 task 开始时读取的非空持久 logical head。
     * @param task     已完成 exact metadata、LOB plan 和直接前驱冻结的单记录任务。
     * @throws UndoLogicalHeadConflictException 预检发现 expected 已陈旧时抛出；没有发生 LOB/header 修改。
     * @throws PurgeProgressException 第一次 LOB free/header 写开始后任一步失败时抛出；同进程不得普通重试。
     * @throws RuntimeException redo admission、undo 打开或预检在物理边界前失败时原样抛出，MTR 资源会被释放。
     */
    private void persistRecordProgress(HistoryEntry entry, UndoLogicalHead expected, PurgeRecordTask task) {
        // 1. progress 固定覆盖 first-page/target-record/header 余量；LOB 计划追加其精确 FSP/page workload。
        RedoBudgetWorkload workload = RedoBudgetWorkload.pageImages(6L);
        if (task.lobFreePlan().isPresent()) {
            workload = workload.plus(task.lobFreePlan().orElseThrow().workload());
        }
        MiniTransaction progress = mgr.begin(mgr.budgetFor(RedoBudgetPurpose.PURGE_RECORD_PROGRESS, workload));
        boolean physicalMutationStarted = false;
        try {
            // 2. first-page X latch 保护预检结果直至 header 提交；target page 也在同一 MTR 中完成真实性校验。
            UndoLogSegment writable = undoAccess.open(progress, entry.undoFirstPageId(), PageLatchMode.EXCLUSIVE);
            writable.validateLogicalHeadUpdate(expected, task.targetHead(),
                    task.targetHeadIndex().keyDef(), task.targetHeadIndex().schema());
            try (var ignored = progress.allowOutOfOrderPageLatch(
                    "purge progress owns undo first-page before LOB/FSP; LOB/FSP never waits for undo pages")) {
                // 3. 从这里开始页面内容没有 Java content undo：旧链 free 必须先于 head 发布，header 永不领先 ownership。
                physicalMutationStarted = true;
                if (task.lobFreePlan().isPresent()) {
                    lobStorage.freePlannedBatch(progress, task.lobFreePlan().orElseThrow());
                }
                writable.updateLogicalHead(expected, task.targetHead(),
                        task.targetHeadIndex().keyDef(), task.targetHeadIndex().schema());
            }
            // 4. commit 返回后新 head 是 crash recovery 的权威入口；故障接缝位于提交之外，模拟真正进程中断。
            mgr.commit(progress);
        } catch (RuntimeException error) {
            rollbackReadMtr(progress, error);
            if (physicalMutationStarted) {
                throw new PurgeProgressException("purge record progress failed after physical mutation began: firstPage="
                        + entry.undoFirstPageId() + ", undoNo=" + expected.undoNo().value(), error);
            }
            throw error;
        }
        faultInjector.onBoundary(PurgeProgressPhase.AFTER_RECORD_PROGRESS_COMMIT,
                task.target().clusteredIndex().indexId());
    }

    /**
     * 精确删除一个已经通过版本链证明的 delete-marked secondary entry；ABSENT 是 crash 重试幂等成功。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从 root 页头刷新结构 level，释放读 latch 后计算 structural-delete redo 预算。</li>
     *     <li>在单棵二级树写 MTR 中按完整 physical key 删除且要求记录已 marked。</li>
     *     <li>STATE_CONFLICT 表示 entry 仍 live，按格式/恢复损坏 fail-closed；ABSENT 视为已完成重试。</li>
     *     <li>提交 redo/dirty/结构页释放后通知测试故障边界；异常只终止仍 ACTIVE 的 MTR。</li>
     * </ol>
     *
     * @param task 已完成版本链安全证明的 exact-version 二级 metadata 与完整 physical key。
     * @return 本次真实删除返回 1，crash 重试发现 ABSENT 返回 0。
     * @throws RuntimeException root 刷新、redo admission、B+Tree 删除/merge/root-shrink、状态冲突或 MTR 提交失败时抛出。
     */
    private int purgeSecondary(SecondaryTask task) {
        // 1. 结构预算只消费当前 root header level，不能依赖 DD binding 的过期提示。
        BTreeIndex index = refresh(task.metadata().index());
        MiniTransaction ix = mgr.begin(mgr.budgetFor(RedoBudgetPurpose.PURGE_INDEX,
                BTreeRedoBudgetEstimator.structuralDelete(index.rootLevel())));
        try {
            // 2. 单树 MTR 精确删除 delete-marked physical identity；不与其它 secondary/clustered latch 重叠。
            BTreeSecondaryRemovalResult result = btree.purgeDeleteMarkedSecondary(
                    ix, index, task.physicalKey());
            // 3. live entry 不能解释为幂等完成，否则会误删当前版本仍使用的索引项。
            if (result.status() == SecondaryEntryRemovalStatus.STATE_CONFLICT) {
                throw new UndoLogFormatException("purge secondary entry is still live: index="
                        + index.indexId() + " key=" + task.physicalKey());
            }
            // 4. commit 已发布 redo/pageLSN/dirty 与结构变化，之后故障由 ABSENT 状态幂等收敛。
            mgr.commit(ix);
            faultInjector.onBoundary(PurgeProgressPhase.AFTER_SECONDARY_COMMIT, index.indexId());
            return result.status() == SecondaryEntryRemovalStatus.REMOVED ? 1 : 0;
        } catch (RuntimeException error) {
            rollbackReadMtr(ix, error);
            throw error;
        }
    }

    /**
     * 物理删除 DELETE_MARK 聚簇记录，调用顺序固定在同一 undo task 的全部 secondary 删除之后。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>非 DELETE task 直接返回零，不创建 MTR。</li>
     *     <li>表级模式刷新 root level，并按 structural-delete 上界取得单树写 MTR。</li>
     *     <li>用 creator transaction id 与目标 roll pointer 精确校验并删除 marked 聚簇记录，stale identity 不改页。</li>
     *     <li>提交 redo/dirty/结构变化后通知故障边界；异常仅终止仍 ACTIVE 的 MTR。</li>
     * </ol>
     *
     * @param entry history owner，提供 delete-mark 前向事务 id。
     * @param task  当前 DELETE_MARK task，提供聚簇 key、目标 roll pointer 与 exact-version descriptor。
     * @return 精确 owner/pointer 匹配并删除时返回 1；非 DELETE 或 stale owner/pointer 幂等 no-op 返回 0。
     * @throws RuntimeException root 刷新、redo admission、聚簇结构删除或 MTR 提交失败时抛出。
     */
    private int purgeClustered(HistoryEntry entry, PurgeRecordTask task) {
        // 1. UPDATE_ROW 只产生 secondary old-key task，不执行聚簇物理删除。
        if (!task.deleteClustered()) {
            return 0;
        }
        // 2. legacy descriptor 可直接使用；表级路径必须消费当前 root header level 冻结结构预算。
        BTreeIndex index = rootSnapshots == null ? task.target().clusteredIndex()
                : refresh(task.target().clusteredIndex());
        MiniTransaction ix = mgr.begin(mgr.budgetFor(RedoBudgetPurpose.PURGE_INDEX,
                BTreeRedoBudgetEstimator.structuralDelete(index.rootLevel())));
        try {
            // 3. owner/pointer 双条件阻止误删已被后续事务更新的同主键记录；stale 结果是幂等 no-op。
            BTreeDeleteResult result = btree.purgeDeleteMarkedClustered(ix, index, task.clusterKey(),
                    entry.creatorTrxId(), task.rollPointer());
            // 4. 提交后页修改已发布，故障重试通过 stale/absent 状态继续；测试接缝只能位于此稳定边界。
            mgr.commit(ix);
            faultInjector.onBoundary(PurgeProgressPhase.AFTER_CLUSTERED_COMMIT, index.indexId());
            return result.removed() ? 1 : 0;
        } catch (RuntimeException error) {
            rollbackReadMtr(ix, error);
            throw error;
        }
    }

    /**
     * 结构删除前用独立短读 MTR 从 root 页头刷新 level，释放 S latch 后才允许创建写 MTR。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建独立只读 MTR，不继承任何 task 写 MTR 的 latch/fix memo。</li>
     *     <li>读取并校验稳定 root 页头，生成只更新 level 的 descriptor。</li>
     *     <li>提交读 MTR释放 S latch 后返回；异常时终止 ACTIVE MTR并保留原始失败。</li>
     * </ol>
     *
     * @param index 含稳定 root page id、但 rootLevel 可能过期的索引 descriptor。
     * @return 仅 rootLevel 被页头权威值替换的不可变 descriptor。
     * @throws RuntimeException root 页读取/归属校验或读 MTR 提交失败时抛出，释放失败保留为 suppressed。
     */
    private BTreeIndex refresh(BTreeIndex index) {
        // 1. root snapshot 使用独立短读 MTR，禁止与后续结构写交叠。
        MiniTransaction read = mgr.beginReadOnly();
        try {
            // 2. root page header 是结构 level 的唯一物理权威，并同时校验 index owner。
            BTreeIndex refreshed = rootSnapshots.refresh(read, index);
            // 3. 先释放 root S latch，再把 descriptor 交给结构预算与写导航。
            mgr.commit(read);
            return refreshed;
        } catch (RuntimeException error) {
            rollbackReadMtr(read, error);
            throw error;
        }
    }

    /**
     * 读取 logical chain 单条 record 并按固定 table/index identity 解析 exact-version target；返回前释放 undo 页资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 SHARED 模式打开 committed undo 段并读取 record 固定 identity，不先猜测 schema。</li>
     *     <li>用 table/index id 解析 exact-version target，并核对返回聚簇 descriptor 与固定 identity。</li>
     *     <li>使用解析出的 key definition/schema 解码完整 record，随后提交读 MTR释放 first/record 页资源。</li>
     *     <li>任一失败只终止仍 ACTIVE 的 MTR，释放失败作为 suppressed 保留。</li>
     * </ol>
     *
     * @param firstPageId 当前 committed history entry 的 undo 段首页。
     * @param pointer     logical chain 中待读取 record 的稳定 roll pointer。
     * @return 同时携带解码 undo record 与 exact-version 表级 target 的无页资源结果。
     * @throws RuntimeException 段/record 读取、identity peek、resolver 错配、格式校验或 MTR 提交失败时抛出。
     */
    private ResolvedUndo readLogicalRecord(PageId firstPageId, RollPointer pointer) {
        // 1. identity peek 只依赖 undo 固定前缀，允许在选择 exact schema 前完成。
        MiniTransaction read = mgr.beginReadOnly();
        try {
            UndoLogSegment segment = undoAccess.open(read, firstPageId, PageLatchMode.SHARED);
            var identity = segment.readRecordIdentity(pointer);
            // 2. resolver 必须返回同 table/index version 的聚簇目标，不能回退到任意“当前”索引。
            UndoTargetMetadata target = targetResolver.resolveTarget(identity.tableId(), identity.indexId());
            BTreeIndex index = target.clusteredIndex();
            if (!index.clustered()) {
                throw new UndoLogFormatException("purge undo references non-clustered index: " + index.indexId());
            }
            if (index.indexId() != identity.indexId()
                    || target.tableIndexes().tableId() != identity.tableId()) {
                throw new UndoLogFormatException("purge undo target resolver identity mismatch: table="
                        + identity.tableId() + " index=" + identity.indexId());
            }
            // 3. exact-version key/schema 决定 record key 与 old image 解码；提交后结果不再引用 undo 页。
            UndoRecord record = segment.readRecord(pointer, index.keyDef(), index.schema());
            mgr.commit(read);
            return new ResolvedUndo(record, target);
        } catch (RuntimeException e) {
            // 4. 只释放仍 ACTIVE 的 MTR，不能用重复终止覆盖 resolver/format 首因。
            rollbackReadMtr(read, e);
            throw e;
        }
    }

    /**
     * 只在 MTR 仍 ACTIVE 时终止并释放资源；提交阶段异常不能用二次状态错误覆盖原始根因。
     *
     * @param read     当前短读或单树写 MTR；已终止时不重复处理。
     * @param original 首个读取、写入或提交失败；释放异常作为 suppressed 附加。
     */
    private void rollbackReadMtr(MiniTransaction read, RuntimeException original) {
        if (read.state() != MiniTransactionState.ACTIVE) {
            return;
        }
        try {
            mgr.rollbackUncommitted(read);
        } catch (RuntimeException releaseError) {
            original.addSuppressed(releaseError);
        }
    }

    /**
     * 为低层单聚簇模式构造固定 resolver；它只接受与配置 descriptor 相同的 index id。
     *
     * @param clusteredIndex 低层实例唯一聚簇 descriptor。
     * @return 按 undo index id 返回该 descriptor、错配时 fail-closed 的 resolver。
     * @throws DatabaseValidationException descriptor 缺失或非聚簇时抛出。
     */
    private static IndexMetadataResolver legacyResolver(BTreeIndex clusteredIndex) {
        if (clusteredIndex == null || !clusteredIndex.clustered()) {
            throw new DatabaseValidationException("purge requires a clustered index");
        }
        return (tableId, indexId) -> {
            if (clusteredIndex.indexId() != indexId) {
                throw new UndoLogFormatException("undo indexId " + indexId
                        + " != configured purge index " + clusteredIndex.indexId());
            }
            return clusteredIndex;
        };
    }

    /**
     * 把旧单索引 resolver 显式包装为空 secondary 的表级 target；遇 secondary tail 时 requireSecondary 会 fail-closed。
     *
     * @param resolver 按 undo table/index identity 返回聚簇 descriptor 的旧端口。
     * @return 构造空 secondary/空 LOB 的兼容 {@link UndoTargetMetadataResolver}。
     * @throws DatabaseValidationException resolver 缺失时抛出。
     * @throws UndoLogFormatException resolver 返回 null、非聚簇或 index id 错配时由返回函数抛出。
     */
    private static UndoTargetMetadataResolver targetResolver(IndexMetadataResolver resolver) {
        if (resolver == null) {
            throw new DatabaseValidationException("purge index resolver must not be null");
        }
        return (tableId, indexId) -> {
            BTreeIndex index = resolver.resolve(tableId, indexId);
            if (index == null || !index.clustered() || index.indexId() != indexId) {
                throw new UndoLogFormatException("purge resolver returned wrong/non-clustered index: table="
                        + tableId + " index=" + indexId);
            }
            return new UndoTargetMetadata(new cn.zhangyis.db.storage.btree.TableIndexMetadata(
                    tableId, index.schema().schemaVersion(), index, List.of()), Optional.empty());
        };
    }

    /**
     * 校验表级 task 所需的 root snapshot、版本链证明和共享 row guard 已整体接线。
     *
     * @param task 当前 exact-version undo record task；secondary 列表为空时仍要求 row guard 保护 LOB/head 交接窗口。
     * @throws DatabaseValidationException secondary 协作者只配置部分，或当前协调器没有 row guard 时抛出。
     */
    private void requireTableLevelWiring(PurgeRecordTask task) {
        if (!task.secondaryTasks().isEmpty()
                && (rootSnapshots == null || rowGuards == null || secondarySafety == null)) {
            throw new DatabaseValidationException(
                    "secondary purge requires root snapshots, shared row guards and safety checker");
        }
        if (rowGuards == null) {
            throw new DatabaseValidationException("table-level purge task requires shared row guards");
        }
    }

    /**
     * 限制旧单聚簇构造器只能处理不含 secondary/LOB ownership 的兼容记录。legacy 模式没有共享 row guard、
     * exact LOB binding 或版本链证明，因此任何新尾部都必须 fail-closed，不能以“无需删除聚簇记录”为由跳过进度。
     *
     * @param task 从当前持久 head 构造的单记录任务。
     * @throws DatabaseValidationException task 含 secondary 或 purge-old LOB 计划时抛出；history head 保持不变。
     */
    private void requireLegacyTaskIsSelfContained(PurgeRecordTask task) {
        if (!task.secondaryTasks().isEmpty() || task.lobFreePlan().isPresent()) {
            throw new DatabaseValidationException(
                    "legacy purge wiring cannot consume secondary or LOB ownership");
        }
    }

    /**
     * 返回生产实际共享的 row guard manager，供包内并发测试验证 DML/purge 互斥；不允许替换运行期实例。
     *
     * @return 当前协调器持有、与 TableDmlService 相同的 row guard manager。
     * @throws DatabaseValidationException 当前为 legacy 无 guard 构造时抛出。
     */
    PurgeDmlRowGuardManager rowGuardsForTest() {
        if (rowGuards == null) {
            throw new DatabaseValidationException("purge row guards are not configured");
        }
        return rowGuards;
    }

    /**
     * 安装稳定 task-commit 故障接缝；必须在无在途 purge batch 时设置并在 finally 中恢复 no-op。
     *
     * @param faultInjector 仅在 secondary/clustered MTR 成功提交后回调的测试接缝，不能为 {@code null}。
     * @throws DatabaseValidationException 接缝为空时抛出。
     */
    void installFaultInjectorForTest(PurgeProgressFaultInjector faultInjector) {
        if (faultInjector == null) {
            throw new DatabaseValidationException("purge progress fault injector must not be null");
        }
        this.faultInjector = faultInjector;
    }

    /**
     * 单个 secondary 物理删除任务。
     *
     * @param metadata    undo exact-version resolver 返回的二级 descriptor/layout。
     * @param physicalKey 从 target old image 投影出的 logical key + 完整聚簇主键 identity。
     */
    private record SecondaryTask(SecondaryIndexMetadata metadata, SearchKey physicalKey) {
    }

    /**
     * 一条 UPDATE/DELETE undo record 对应的 purge 任务聚合。
     *
     * @param record          已完成格式校验的目标 undo record。
     * @param rollPointer     该 record 自身地址，也是版本链证明停止点。
     * @param target          exact-version 表索引与 LOB binding。
     * @param clusterKey      undo 固定前缀中的完整物化聚簇主键。
     * @param secondaryTasks  按 mutation index id 排序的二级物理任务。
     * @param deleteClustered DELETE_MARK 时为 true，UPDATE_ROW 时为 false。
     * @param lobFreePlan     LV purge-old ownership 派生的可选批量释放计划。
     * @param targetHead      当前记录完成后要发布的直接前驱或空 logical head。
     * @param targetHeadIndex 非空 target record 的 exact-version 聚簇定义；空 target 使用当前 record 定义。
     */
    private record PurgeRecordTask(UndoRecord record, RollPointer rollPointer, UndoTargetMetadata target,
                                   SearchKey clusterKey, List<SecondaryTask> secondaryTasks,
                                   boolean deleteClustered, Optional<LobFreeBatchPlan> lobFreePlan,
                                   UndoLogicalHead targetHead, BTreeIndex targetHeadIndex) {
    }

    /**
     * logical-chain 单跳读取结果。
     *
     * @param record 解码后的 undo record，不再引用 undo page。
     * @param target 与 record 固定 table/index identity 精确匹配的表级恢复目标。
     */
    private record ResolvedUndo(UndoRecord record, UndoTargetMetadata target) {
    }

    /**
     * 单条 history log 的内部处理结果。
     *
     * @param completed        全部物理任务与 finalization 已完成时为 true；row guard busy 时为 false。
     * @param removedClustered 本 log 已真实删除的聚簇记录数。
     * @param removedSecondary 本 log 已真实删除的二级 entry 数。
     */
    private record PurgeLogOutcome(boolean completed, int removedClustered, int removedSecondary) {
        private static PurgeLogOutcome completed(int removedClustered, int removedSecondary) {
            return new PurgeLogOutcome(true, removedClustered, removedSecondary);
        }

        private static PurgeLogOutcome deferred(int removedClustered, int removedSecondary) {
            return new PurgeLogOutcome(false, removedClustered, removedSecondary);
        }
    }

    /** 一次短读取得的持久逻辑链入口。 */
    private record LogicalChainStart(UndoLogicalHead head) {
    }
}
