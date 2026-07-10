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
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoRecord;
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
 * <p><b>Latch 纪律</b>（同 {@link MvccReader}）：先用短只读 MTR 取得 first-page 持久 logical head + 段 handle，
 * 再沿 {@code prevRollPointer} 每条用独立短读 MTR 物化 DELETE_MARK task；每次提交都立即释放 undo latch/fix。
 * 整条链校验完成后才逐条开启 index MTR 调用
 * {@link SplitCapableBTreeIndexService#purgeDeleteMarkedClustered}，最后独立 MTR {@code dropUndoSegment}。
 * 任一时刻只持 undo 或 index 之一，也不会把超过 Buffer Pool 容量的整条 undo 页链同时 fixed。
 *
 * <p><b>per-entry 失败边界</b>：insert/committed 两队列都先 peek，只有 index 任务、segment drop 与 slot release
 * 全部成功后才 poll。可预测的 slot ownership 在任何 index/drop 副作用前预检；drop 前 IO/页损坏会保留队首并停批，
 * 已移除 index 记录重试时 stale-skip。当前 history/slot 仍是内存结构，进程在 drop 成功与 poll 之间崩溃的持久原子性
 * 依赖未来的持久 history/release 协议，本片不作超范围承诺。
 *
 * <p><b>当前范围</b>：协调器自身同步串行，生产由单 daemon driver 周期调用；单聚簇索引、内存 history，
 * recovery 会从 COMMITTED header 重建队列。二级索引 purge、多 worker、多 rseg 和持久 history/release 协议留后续片。
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
        while ((ins = history.peekInsertReclaim()).isPresent()) {
            reclaimInsertSegment(ins.get());
            history.pollInsertReclaim();
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
        PageId occupiedFirstPage = slotManager.insertUndoFirstPageId(entry.slotId());
        if (!occupiedFirstPage.equals(entry.undoFirstPageId())) {
            throw new UndoLogFormatException("purge history slot points to " + occupiedFirstPage
                    + " instead of " + entry.undoFirstPageId());
        }
        // 首个短 MTR 只取逻辑入口和段回收句柄；不能用物理 slot/FIL NEXT 遍历，否则会重新消费 rolled-back 分支。
        LogicalChainStart start;
        MiniTransaction read = mgr.begin();
        try {
            UndoLogSegment seg = undoAccess.open(read, entry.undoFirstPageId(), PageLatchMode.SHARED);
            if (!seg.creatorTransactionId().equals(entry.creatorTrxId())) {
                throw new UndoLogFormatException("purge history creator transaction "
                        + entry.creatorTrxId().value() + " != undo segment creator "
                        + seg.creatorTransactionId().value());
            }
            start = new LogicalChainStart(seg.logicalHead(), seg.handle());
            mgr.commit(read);
        } catch (RuntimeException e) {
            rollbackReadMtr(read, e);
            throw e;
        }

        List<DeleteTask> tasks = collectDeleteTasks(entry, start.head());

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
            undoAllocator.dropUndoSegment(drop, start.handle());
        } catch (RuntimeException e) {
            mgr.rollbackUncommitted(drop);
            throw e;
        }
        mgr.commit(drop);

        slotManager.release(entry.slotId());
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
        MiniTransaction read = mgr.begin();
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

    /** 回收一条纯 insert undo 段：只读 MTR 取 handle → 独立 MTR dropUndoSegment（slot 已在 onCommit 释放）。 */
    private void reclaimInsertSegment(InsertReclaimEntry entry) {
        UndoSegmentHandle handle;
        MiniTransaction read = mgr.begin();
        try {
            UndoLogSegment seg = undoAccess.open(read, entry.undoFirstPageId(), PageLatchMode.SHARED);
            handle = seg.handle();
            mgr.commit(read);
        } catch (RuntimeException e) {
            rollbackReadMtr(read, e);
            throw e;
        }

        MiniTransaction drop = mgr.begin();
        try {
            undoAllocator.dropUndoSegment(drop, handle);
            mgr.commit(drop);
        } catch (RuntimeException e) {
            rollbackReadMtr(drop, e);
            throw e;
        }
    }

    /** 一条待移除的 delete-marked 聚簇记录：搜索 key + 该 DELETE_MARK undo 记录自身地址（= 记录应有的 DB_ROLL_PTR）。 */
    private record DeleteTask(SearchKey key, RollPointer rollPointer) {
    }

    /** 一次短读取得的持久链入口与后续 drop 所需 segment 定位值对象。 */
    private record LogicalChainStart(UndoLogicalHead head, UndoSegmentHandle handle) {
    }
}
