package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.btree.BTreeDeleteResult;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoRecordType;
import cn.zhangyis.db.storage.undo.UndoSegmentHandle;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 单线程 purge 协调器（设计 §5.7/§7.7，purge 切片）。{@link #runBatch} 同步处理一批已提交 undo：
 * <ol>
 *   <li>排空 insert-reclaim 队列：纯 insert undo 段提交即可回收（一致性读从不需要 insert undo），无 boundary，直接
 *       {@code dropUndoSegment}。</li>
 *   <li>按 purge boundary（= {@link TransactionSystem#purgeLowWaterNo()}，最老 live ReadView 低水位）FIFO 处理 committed
 *       history：对每条 {@code DELETE_MARK} undo 严格物理移除对应 delete-marked 聚簇记录；该 log 全部记录处理完后回收
 *       undo 段并释放 slot。遇队首 {@code transactionNo >= boundary} 即停（更老读者可能仍需旧版本）。</li>
 * </ol>
 *
 * <p><b>Latch 纪律</b>（同 {@link MvccReader}）：先用只读 MTR 打开 undo 段收集 (DELETE_MARK 记录, 自身地址) + 段 handle，
 * 提交释放 undo latch；再每条用独立 index MTR 经 {@link SplitCapableBTreeIndexService#purgeDeleteMarkedClustered} 移除；
 * 最后独立 MTR {@code dropUndoSegment}。任一时刻只持 undo 或 index 之一，不反转写路径锁序、不持 latch 跨 MTR。
 *
 * <p><b>per-entry 原子</b>（评审 #8）：peek 队首处理，全部成功/确认 stale 后才 {@code pollCommitted} 移除 + dropSegment
 * + release slot；任一硬失败（IO/页损坏）异常传播、队首保留、批次中止，可重试（已移除记录在 purge 时 stale-skip，幂等）。
 *
 * <p><b>本片范围</b>：单线程同步（无后台线程）、单聚簇索引、内存 history；二级索引 purge、多 worker、多 rseg、
 * 持久 history、recovery resume 留后续片。
 */
public final class PurgeCoordinator implements PurgeTarget {

    private final MiniTransactionManager mgr;
    private final TransactionSystem system;
    private final HistoryList history;
    private final UndoLogSegmentAccess undoAccess;
    private final UndoSpaceAllocator undoAllocator;
    private final RollbackSegmentSlotManager slotManager;
    private final SplitCapableBTreeIndexService btree;
    private final BTreeIndex clusteredIndex;

    public PurgeCoordinator(MiniTransactionManager mgr, TransactionSystem system, HistoryList history,
                            UndoLogSegmentAccess undoAccess, UndoSpaceAllocator undoAllocator,
                            RollbackSegmentSlotManager slotManager, SplitCapableBTreeIndexService btree,
                            BTreeIndex clusteredIndex) {
        if (mgr == null || system == null || history == null || undoAccess == null || undoAllocator == null
                || slotManager == null || btree == null || clusteredIndex == null) {
            throw new DatabaseValidationException("purge coordinator collaborators must not be null");
        }
        if (!clusteredIndex.clustered()) {
            throw new DatabaseValidationException("purge requires a clustered index: " + clusteredIndex.indexId());
        }
        this.mgr = mgr;
        this.system = system;
        this.history = history;
        this.undoAccess = undoAccess;
        this.undoAllocator = undoAllocator;
        this.slotManager = slotManager;
        this.btree = btree;
        this.clusteredIndex = clusteredIndex;
    }

    /**
     * 执行一个可测试 purge 批次：先排空 insert-reclaim，再按 boundary FIFO 处理至多 {@code maxLogs} 条 committed undo log。
     *
     * @param maxLogs 本批最多处理的 committed undo log 数（正）。
     * @return 本批统计。
     */
    public PurgeSummary runBatch(int maxLogs) {
        if (maxLogs <= 0) {
            throw new DatabaseValidationException("purge maxLogs must be positive: " + maxLogs);
        }
        int reclaimedInsertSegments = 0;
        Optional<InsertReclaimEntry> ins;
        while ((ins = history.pollInsertReclaim()).isPresent()) {
            reclaimInsertSegment(ins.get());
            reclaimedInsertSegments++;
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
            history.pollCommitted(); // 全部成功后才移除（硬失败已在上一行抛出，队首保留）
            purgedLogs++;
        }
        return new PurgeSummary(purgedLogs, removedRecords, reclaimedInsertSegments);
    }

    /**
     * 处理一条 committed undo log：移除其 DELETE_MARK 对应的 delete-marked 聚簇记录 → 回收 undo 段 → 释放 slot。
     * 返回本 log 物理移除的记录数（stale 不计）。
     */
    private int purgeCommittedLog(HistoryEntry entry) {
        // MTR-read：收集 DELETE_MARK (聚簇 key, 该 undo 记录自身地址) + 段 handle，提交释放 undo latch
        List<DeleteTask> tasks = new ArrayList<>();
        UndoSegmentHandle handle;
        MiniTransaction read = mgr.begin();
        try {
            UndoLogSegment seg = undoAccess.open(read, entry.undoFirstPageId(), PageLatchMode.SHARED);
            seg.forEachRecordWithPointer((rec, rp) -> {
                if (rec.type() == UndoRecordType.DELETE_MARK) {
                    tasks.add(new DeleteTask(new SearchKey(rec.clusterKey()), rp));
                }
            }, clusteredIndex.keyDef(), clusteredIndex.schema());
            handle = seg.handle();
        } catch (RuntimeException e) {
            mgr.rollbackUncommitted(read);
            throw e;
        }
        mgr.commit(read);

        int removed = 0;
        for (DeleteTask task : tasks) {
            MiniTransaction ix = mgr.begin();
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

        // MTR-drop：回收 undo 段（归还页给 FSP）
        MiniTransaction drop = mgr.begin();
        try {
            undoAllocator.dropUndoSegment(drop, handle);
        } catch (RuntimeException e) {
            mgr.rollbackUncommitted(drop);
            throw e;
        }
        mgr.commit(drop);

        slotManager.release(entry.slotId());
        return removed;
    }

    /** 回收一条纯 insert undo 段：只读 MTR 取 handle → 独立 MTR dropUndoSegment（slot 已在 onCommit 释放）。 */
    private void reclaimInsertSegment(InsertReclaimEntry entry) {
        UndoSegmentHandle handle;
        MiniTransaction read = mgr.begin();
        try {
            UndoLogSegment seg = undoAccess.open(read, entry.undoFirstPageId(), PageLatchMode.SHARED);
            handle = seg.handle();
        } catch (RuntimeException e) {
            mgr.rollbackUncommitted(read);
            throw e;
        }
        mgr.commit(read);

        MiniTransaction drop = mgr.begin();
        try {
            undoAllocator.dropUndoSegment(drop, handle);
        } catch (RuntimeException e) {
            mgr.rollbackUncommitted(drop);
            throw e;
        }
        mgr.commit(drop);
    }

    /** 一条待移除的 delete-marked 聚簇记录：搜索 key + 该 DELETE_MARK undo 记录自身地址（= 记录应有的 DB_ROLL_PTR）。 */
    private record DeleteTask(SearchKey key, RollPointer rollPointer) {
    }
}
