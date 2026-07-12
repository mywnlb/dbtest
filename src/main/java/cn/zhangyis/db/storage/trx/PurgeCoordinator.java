package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.btree.BTreeDeleteResult;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 单线程 purge 协调器（设计 §5.7/§7.7，purge 切片）。{@link #runBatch} 同步处理一批已提交 undo：
 * <ol>
 *   <li>按 purge boundary（= {@link TransactionSystem#purgeLowWaterNo()}，最老 live ReadView 低水位）FIFO 处理 committed
 *       history：对每条 {@code DELETE_MARK} undo 严格物理移除对应 delete-marked 聚簇记录；该 log 全部记录处理完后回收
 *       undo 段并释放 slot。遇队首 {@code transactionNo >= boundary} 即停（更老读者可能仍需旧版本）。</li>
 * </ol>
 *
 * <p><b>Latch 纪律</b>（同 {@link MvccReader}）：先用短只读 MTR 取得 first-page 持久 logical head + 段 handle，
 * 再沿 {@code prevRollPointer} 每条用独立短读 MTR 物化 DELETE_MARK task；每次提交都立即释放 undo latch/fix。
 * 整条链校验完成后才逐条开启 index MTR 调用
 * {@link SplitCapableBTreeIndexService#purgeDeleteMarkedClustered}，最后由 {@link UndoSegmentFinalizer} 在单一 MTR
 * 原子执行 segment drop + page3 clear。
 * 任一时刻只持 undo 或 index 之一，也不会把超过 Buffer Pool 容量的整条 undo 页链同时 fixed。
 *
 * <p><b>per-entry 失败边界</b>：committed 队列先 peek，只有 index tasks 与 finalization 全部成功后才按 expected
 * identity 摘队首。task 中途失败保留 page3 COMMITTED slot，重启可重建并 stale-skip 已完成任务；finalization 的
 * drop+clear 同批提交后即使在内存 complete 前 crash，page3 也不会重新暴露已释放 segment。
 *
 * <p><b>当前范围</b>：协调器自身同步串行，生产由单 daemon driver 周期调用；单聚簇索引、内存 history，
 * recovery 会从仍占用 page3 的 COMMITTED header 重建队列。二级索引 purge、多 worker、多 rseg 留后续片。
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
    /** tasks 完成后原子 drop segment + clear page3 owner 的终态协作者。 */
    private final UndoSegmentFinalizer finalizer;
    /** 物理删除仍匹配 creator/roll-pointer 的 delete-marked 聚簇记录。 */
    private final SplitCapableBTreeIndexService btree;
    /** 当前无 DD 时显式配置的唯一聚簇索引。 */
    private final BTreeIndex clusteredIndex;

    /**
     * 构造单线程 purge 协调器；所有依赖必须属于同一 engine/undo tablespace 生命周期。
     *
     * @param mgr            MTR 来源。
     * @param system         purge boundary 来源。
     * @param history        committed history 内存投影。
     * @param undoAccess     undo logical chain 读取入口。
     * @param finalizer      undo 段原子终结器。
     * @param btree          聚簇物理删除服务。
     * @param clusteredIndex 当前显式配置的聚簇索引。
     */
    public PurgeCoordinator(MiniTransactionManager mgr, TransactionSystem system, HistoryList history,
                            UndoLogSegmentAccess undoAccess, UndoSegmentFinalizer finalizer,
                            SplitCapableBTreeIndexService btree,
                            BTreeIndex clusteredIndex) {
        if (mgr == null || system == null || history == null || undoAccess == null || finalizer == null
                || btree == null || clusteredIndex == null) {
            throw new DatabaseValidationException("purge coordinator collaborators must not be null");
        }
        if (!clusteredIndex.clustered()) {
            throw new DatabaseValidationException("purge requires a clustered index: " + clusteredIndex.indexId());
        }
        this.mgr = mgr;
        this.system = system;
        this.history = history;
        this.undoAccess = undoAccess;
        this.finalizer = finalizer;
        this.btree = btree;
        this.clusteredIndex = clusteredIndex;
    }

    /**
     * 执行一个可测试 purge 批次：按 boundary FIFO 处理至多 {@code maxLogs} 条 committed update/delete undo log。
     *
     * @param maxLogs 本批最多处理的 committed undo log 数（正）。
     * @return 本批统计。
     */
    public PurgeSummary runBatch(int maxLogs) {
        if (maxLogs <= 0) {
            throw new DatabaseValidationException("purge maxLogs must be positive: " + maxLogs);
        }
        TransactionNo boundary = system.purgeLowWaterNo();
        int purgedLogs = 0;
        int removedRecords = 0;
        while (purgedLogs < maxLogs) {
            Optional<HistoryEntry> headOpt = history.peekCommitted();
            if (headOpt.isEmpty()) {
                break;
            }
            HistoryEntry head = headOpt.get();
            if (head.transactionNo().value() >= boundary.value()) {
                break; // 未达 boundary：FIFO 停批，更老读者仍可能需要旧版本
            }
            removedRecords += purgeCommittedLog(head);
            try {
                history.completeCommitted(head);
            } catch (RuntimeException error) {
                throw new UndoFinalizationException(
                        "purge finalization committed but history publication failed: " + head, error);
            }
            purgedLogs++;
        }
        return new PurgeSummary(purgedLogs, removedRecords);
    }

    /**
     * 处理一条 committed undo log：移除其 DELETE_MARK 对应的 delete-marked 聚簇记录 → 回收 undo 段 → 释放 slot。
     * 返回本 log 物理移除的记录数（stale 不计）。
     */
    private int purgeCommittedLog(HistoryEntry entry) {
        // 首个短 MTR 只取逻辑入口；不能用物理 slot/FIL NEXT 遍历，否则会重新消费 rolled-back 分支。
        LogicalChainStart start;
        MiniTransaction read = mgr.beginReadOnly();
        try {
            UndoLogSegment seg = undoAccess.open(read, entry.undoFirstPageId(), PageLatchMode.SHARED);
            if (!seg.creatorTransactionId().equals(entry.creatorTrxId())) {
                throw new UndoLogFormatException("purge history creator transaction "
                        + entry.creatorTrxId().value() + " != undo segment creator "
                        + seg.creatorTransactionId().value());
            }
            start = new LogicalChainStart(seg.logicalHead());
            mgr.commit(read);
        } catch (RuntimeException e) {
            rollbackReadMtr(read, e);
            throw e;
        }

        List<DeleteTask> tasks = collectDeleteTasks(entry, start.head());

        int removed = 0;
        for (DeleteTask task : tasks) {
            MiniTransaction ix = mgr.begin(mgr.budgetFor(RedoBudgetPurpose.PURGE_INDEX,
                    BTreeRedoBudgetEstimator.structuralDelete(clusteredIndex.rootLevel())));
            BTreeDeleteResult res;
            try {
                // expected = (删除事务 id, 该 DELETE_MARK undo 记录地址)；严格：仅移除仍 delete-marked 且隐藏列匹配的行
                res = btree.purgeDeleteMarkedClustered(ix, clusteredIndex, task.key(),
                        entry.creatorTrxId(), task.rollPointer());
            } catch (RuntimeException e) {
                mgr.rollbackUncommitted(ix);
                throw e;
            }
            mgr.commit(ix);
            if (res.removed()) {
                removed++;
            }
        }

        finalizer.finalizePurgedHistory(entry);
        return removed;
    }

    /**
     * 沿持久 logical head 反向收集本 committed log 的 DELETE_MARK purge 命令。首条必须精确匹配 header undoNo，
     * 后续 undoNo 必须严格下降；该约束同时防止损坏 pointer 环和 detached physical branch 被误纳入当前链。
     */
    private List<DeleteTask> collectDeleteTasks(HistoryEntry entry, UndoLogicalHead head) {
        List<DeleteTask> tasks = new ArrayList<>();
        RollPointer pointer = head.rollPointer();
        long previousUndoNo = 0L;
        boolean first = true;
        while (!pointer.isNull()) {
            UndoRecord record = readLogicalRecord(entry.undoFirstPageId(), pointer);
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
            if (record.type() == UndoRecordType.DELETE_MARK) {
                tasks.add(new DeleteTask(new SearchKey(record.clusterKey()), pointer));
            }
            first = false;
            previousUndoNo = undoNo;
            pointer = record.prevRollPointer();
        }
        return tasks;
    }

    /** 读取逻辑链单条 record；返回前已提交只读 MTR并释放 first/record 页 latch 与 buffer fix。 */
    private UndoRecord readLogicalRecord(PageId firstPageId, RollPointer pointer) {
        MiniTransaction read = mgr.beginReadOnly();
        try {
            UndoRecord record = undoAccess.open(read, firstPageId, PageLatchMode.SHARED)
                    .readRecord(pointer, clusteredIndex.keyDef(), clusteredIndex.schema());
            mgr.commit(read);
            return record;
        } catch (RuntimeException e) {
            rollbackReadMtr(read, e);
            throw e;
        }
    }

    /** 只在 MTR 仍 ACTIVE 时回滚资源；提交阶段异常不能用二次状态错误覆盖原始根因。 */
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

    /** 一条待移除的 delete-marked 聚簇记录：搜索 key + 该 DELETE_MARK undo 记录自身地址（= 记录应有的 DB_ROLL_PTR）。 */
    private record DeleteTask(SearchKey key, RollPointer rollPointer) {
    }

    /** 一次短读取得的持久逻辑链入口。 */
    private record LogicalChainStart(UndoLogicalHead head) {
    }
}
