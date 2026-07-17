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
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
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
                targetResolver(legacyResolver(clusteredIndex)), null, null, null);
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
                targetResolver(indexResolver), null, null, null);
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
     * 处理一条 committed update undo log。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>短读并校验 first-page committed logical head，返回前释放 undo page latch。</li>
     *     <li>沿 logical predecessor 链收集 exact-version record task；不扫描 detached 物理分支。</li>
     *     <li>每条 record 零等待取得共享 row guard；busy 立即延后整条 log，history head 保持不变。</li>
     *     <li>UPDATE/DELETE 的 secondary task 先做版本链安全判断并按需物理删除；DELETE clustered task 固定最后。</li>
     *     <li>全部任务完成后才取得 history removal lease，由 finalizer 原子回收 undo owner 并发布 head removal。</li>
     * </ol>
     *
     * @param entry 当前物理 history 队首；creator/slot/first-page identity 必须与 persistent owner 一致。
     * @return completed 时携带本 log 实际删除计数；row guard busy 时返回 deferred 且不摘除 history。
     * @throws RuntimeException undo 链损坏、metadata 解析失败、版本链无法证明、物理状态冲突或 finalization 失败时抛出。
     */
    private PurgeLogOutcome purgeCommittedLog(HistoryEntry entry) {
        // 1. 首个短 MTR 只取 logical head；不能用物理 slot/FIL NEXT 遍历，否则会重新消费 rolled-back 分支。
        LogicalChainStart start;
        MiniTransaction read = mgr.beginReadOnly();
        try {
            UndoLogSegment seg = undoAccess.open(read, entry.undoFirstPageId(), PageLatchMode.SHARED);
            if (!seg.creatorTransactionId().equals(entry.creatorTrxId())) {
                throw new UndoLogFormatException("purge history creator transaction "
                        + entry.creatorTrxId().value() + " != undo segment creator "
                        + seg.creatorTransactionId().value());
            }
            if (seg.undoKind() != UndoLogKind.UPDATE || !seg.isCommitted()) {
                throw new UndoLogFormatException("purge history must reference a COMMITTED UPDATE undo log");
            }
            start = new LogicalChainStart(seg.logicalHead());
            mgr.commit(read);
        } catch (RuntimeException e) {
            rollbackReadMtr(read, e);
            throw e;
        }

        // 2. 全链先物化为不带页句柄的 exact-version task，随后才开始索引写。
        List<PurgeRecordTask> tasks = collectTasks(entry, start.head());

        int removedClustered = 0;
        int removedSecondary = 0;
        // 3. 每条业务记录独立零等待取得共享 row guard；busy 保留当前 log 的 history owner。
        for (PurgeRecordTask task : tasks) {
            if (task.secondaryTasks().isEmpty() && rowGuards == null) {
                removedClustered += purgeClustered(entry, task);
                continue;
            }
            requireSecondaryWiring(task);
            Optional<PurgeDmlRowGuard> guard = rowGuards.tryAcquireForPurge(
                    task.record().tableId(), task.clusterKey());
            if (guard.isEmpty()) {
                return PurgeLogOutcome.deferred(removedClustered, removedSecondary);
            }
            try (PurgeDmlRowGuard ignored = guard.orElseThrow()) {
                // 4. 同一行 guard 内按 secondary 在前、clustered 在后的顺序执行多个独立短 MTR。
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
            }
        }

        // 5. B+Tree 任务全部完成后才占用跨 IO history transition，避免慢索引清理阻塞其它 commit append。
        try (HistoryList.HeadRemovalLease lease = history.beginHeadRemoval(entry)) {
            finalizer.finalizePurgedHistory(entry, lease);
        }
        return PurgeLogOutcome.completed(removedClustered, removedSecondary);
    }

    /**
     * 沿持久 logical head 反向收集 UPDATE 旧 secondary key 与 DELETE 全 secondary+clustered 任务。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从 header logical head 初始化 pointer 与严格下降的 undoNo 校验状态。</li>
     *     <li>每跳用独立短 MTR 读取 record 和 exact-version target，校验首条 head undoNo、后续单调性与 creator identity。</li>
     *     <li>对 UPDATE/DELETE 从 old image 按 mutation index-id 顺序重建 secondary physical key，并冻结 clustered 删除标志。</li>
     *     <li>沿 prevRollPointer 前进直到 NULL；收集阶段不访问 B+Tree，不持有跨记录 undo latch。</li>
     * </ol>
     *
     * @param entry history 队首，提供 first-page 与 creator identity。
     * @param head  first-page header 中已校验的持久 logical chain 入口。
     * @return 按 undo logical chain 新→旧顺序排列的不带页资源任务列表。
     * @throws RuntimeException undo 链读失败、head/undoNo/creator 错配、metadata 无法 exact-version 解析或 old image 损坏时抛出。
     */
    private List<PurgeRecordTask> collectTasks(HistoryEntry entry, UndoLogicalHead head) {
        // 1. logical head 是唯一入口；previousUndoNo 只用于本次链内严格下降证明。
        List<PurgeRecordTask> tasks = new ArrayList<>();
        RollPointer pointer = head.rollPointer();
        long previousUndoNo = 0L;
        boolean first = true;
        while (!pointer.isNull()) {
            // 2. 每条 record 在独立读 MTR 中完全物化并解析 exact-version target。
            ResolvedUndo recordAt = readLogicalRecord(entry.undoFirstPageId(), pointer);
            UndoRecord record = recordAt.record();
            long undoNo = record.undoNo().value();
            if (first) {
                if (undoNo != head.undoNo().value()) {
                    throw new UndoLogFormatException("persistent purge head undoNo " + head.undoNo().value()
                            + " resolves to " + undoNo);
                }
            } else if (undoNo >= previousUndoNo) {
                throw new UndoLogFormatException("purge undo logical chain is not strictly descending: "
                        + undoNo + " after " + previousUndoNo);
            }
            if (!record.transactionId().equals(entry.creatorTrxId())) {
                throw new UndoLogFormatException("purge undo record transaction "
                        + record.transactionId().value() + " != history creator " + entry.creatorTrxId().value());
            }
            // 3. 只有 UPDATE/DELETE 进入 committed history 物理清理；mutation 顺序由 codec/领域对象保证按 index id 递增。
            if (record.type() == UndoRecordType.UPDATE_ROW || record.type() == UndoRecordType.DELETE_MARK) {
                List<SecondaryTask> secondaryTasks = new ArrayList<>();
                for (SecondaryUndoMutation mutation : record.secondaryMutations()) {
                    SecondaryIndexMetadata metadata = recordAt.target().tableIndexes()
                            .requireSecondary(mutation.indexId());
                    LogicalRecord oldRow = new LogicalRecord(recordAt.target().tableIndexes().schemaVersion(),
                            record.oldColumnValues(), false,
                            cn.zhangyis.db.storage.record.format.RecordType.CONVENTIONAL,
                            record.oldHiddenColumns());
                    SearchKey physicalKey = metadata.layout().physicalKey(
                            metadata.layout().toEntry(oldRow, true));
                    secondaryTasks.add(new SecondaryTask(metadata, physicalKey));
                }
                tasks.add(new PurgeRecordTask(record, pointer, recordAt.target(),
                        new SearchKey(record.clusterKey()), List.copyOf(secondaryTasks),
                        record.type() == UndoRecordType.DELETE_MARK));
            }
            // 4. pointer 前进前保存本条 undoNo，下一跳必须严格更小；NULL 表示 logical chain 完整结束。
            first = false;
            previousUndoNo = undoNo;
            pointer = record.prevRollPointer();
        }
        return tasks;
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
     * 校验当前 task 需要的 secondary purge 协作者已经整体接线；legacy resolver 不得静默跳过 mutation。
     *
     * @param task 从 undo logical chain 收集的 record task。
     * @throws DatabaseValidationException task 含 secondary mutation 但缺少 root/guard/safety，或表级 task 缺 row guard 时抛出。
     */
    private void requireSecondaryWiring(PurgeRecordTask task) {
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
     */
    private record PurgeRecordTask(UndoRecord record, RollPointer rollPointer, UndoTargetMetadata target,
                                   SearchKey clusterKey, List<SecondaryTask> secondaryTasks,
                                   boolean deleteClustered) {
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
