package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * 事务 rollback 执行器（设计 §7.6/§11.2/§14.4，T1.3d 首次消费 undo）。从 {@link UndoContext#lastRollPointer}
 * 反向走当前逻辑 undo 链，对 {@code INSERT_ROW}/{@code UPDATE_ROW}/{@code DELETE_MARK} 分别执行删除、旧 image
 * 恢复和取消删除标记；完整 rollback 最后回收内存 slot，savepoint rollback 只退运行期逻辑边界。
 *
 * <p><b>依赖方向</b>：{@code storage.trx → storage.btree + storage.undo}（设计 §94）。本类直 import
 * {@link SplitCapableBTreeIndexService}（删除聚簇行）与 {@link UndoLogSegmentAccess}/{@link UndoRecord}（读 undo 链）；
 * btree/undo 均不反向 import trx，无环。
 *
 * <p><b>状态机两阶段</b>：{@code rollback} 先 {@link TransactionManager#beginRollback}（ACTIVE→ROLLING_BACK），
 * 把整段撤销夹在真正的 {@code ROLLING_BACK} 状态内（设计 §7.6），走完链 + 释放 slot 后再
 * {@link TransactionManager#finishRollback}（removeActive + →ROLLED_BACK）。
 *
 * <p><b>每条 undo 独立 MTR</b>（§7.6 step 6）：大事务可分批、可恢复；undo 页 redo 与聚簇删除 redo 同批 durable。
 * 单条失败（readRecord/applyUndoRecord 抛）只回滚当前 MTR 释放页 latch，向上传播异常；事务停在 {@code ROLLING_BACK}、
 * slot 不释放、活跃表不变，保持可重试（{@code release}+{@code finishRollback} 仅在走到 {@code prev=NULL} 后执行）。
 *
 * <p><b>幂等</b>：B+Tree 反向命令未命中/所有权不匹配即 no-op，故 MTR 无 content undo 留下的 orphan undo
 * （已写 undo 但无对应聚簇行）由本走链幂等清理；1.4 起 statement/savepoint 边界可在 storage 内部显式回退。
 *
 * <p><b>单聚簇索引假设</b>：用传入 {@code clusteredIndex} 的 keyDef/schema 解码所有 undo（T1 无 data dictionary）；
 * 多索引解析、二级索引删除、SQL/session statement 入口与 savepoint lock scope 留后续。
 */
public final class RollbackService {

    /** 聚簇删除入口；{@code applyUndoRecord} 对 INSERT_ROW undo 调 {@code deleteClustered} 物理删除。 */
    private final SplitCapableBTreeIndexService btree;
    /** undo 物理设施；走链经它 {@code open(SHARED)} + {@code readRecord} 读回每条 undo record。 */
    private final UndoLogSegmentAccess undoAccess;
    /** 内存 rseg slot 目录；走完链后释放本事务 slot。 */
    private final RollbackSegmentSlotManager slotManager;
    /** 事务状态门面；提供 begin/finishRollback 两阶段，集中状态机逻辑不重复。 */
    private final TransactionManager txnMgr;
    /** 物理短事务工厂；每条 undo 一个独立 MTR。 */
    private final MiniTransactionManager mtrMgr;

    public RollbackService(SplitCapableBTreeIndexService btree, UndoLogSegmentAccess undoAccess,
                           RollbackSegmentSlotManager slotManager, TransactionManager txnMgr,
                           MiniTransactionManager mtrMgr) {
        if (btree == null || undoAccess == null || slotManager == null || txnMgr == null || mtrMgr == null) {
            throw new DatabaseValidationException("rollback service collaborators must not be null");
        }
        this.btree = btree;
        this.undoAccess = undoAccess;
        this.slotManager = slotManager;
        this.txnMgr = txnMgr;
        this.mtrMgr = mtrMgr;
    }

    /**
     * 完整回滚事务。数据流：
     * <ol>
     *   <li>{@code beginRollback}：ACTIVE→ROLLING_BACK（requireActive 在内）。</li>
     *   <li>若有 {@link UndoContext}：从 {@code lastRollPointer} 反向走链，每条独立 MTR——
     *       {@code open(SHARED)} 重开 undo segment → {@code readRecord(rp)} 读回 → 取 {@code prevRollPointer} →
     *       {@code applyUndoRecord}（INSERT/UPDATE/DELETE_MARK 反向命令）→ {@code commit}；失败回滚当前 MTR 并传播。</li>
     *   <li>走到 {@code prev=NULL} 后释放 slot。</li>
     *   <li>{@code finishRollback}：removeActive + ROLLING_BACK→ROLLED_BACK。</li>
     * </ol>
     * 只读/未写事务（{@code undoContext()==null}）跳过走链，仅翻状态、不动 slot。
     *
     * @param txn            待回滚事务，必须 ACTIVE。
     * @param clusteredIndex 该事务写入的聚簇索引（提供 keyDef/schema 解码 undo + 删除目标）。
     * @return 本次回滚应用的 undo record 条数摘要。
     */
    public RollbackSummary rollback(Transaction txn, BTreeIndex clusteredIndex) {
        if (txn == null || clusteredIndex == null) {
            throw new DatabaseValidationException("rollback txn/clusteredIndex must not be null");
        }
        txnMgr.beginRollback(txn);
        int applied = 0;
        UndoContext ctx = txn.undoContext();
        if (ctx != null) {
            RollPointer rp = ctx.lastRollPointer();
            while (!rp.isNull()) {
                MiniTransaction m = mtrMgr.begin();
                RollPointer prev;
                try {
                    UndoLogSegment seg = undoAccess.open(m, ctx.undoFirstPageId(), PageLatchMode.SHARED);
                    UndoRecord rec = seg.readRecord(rp, clusteredIndex.keyDef(), clusteredIndex.schema());
                    prev = rec.prevRollPointer();
                    applyUndoRecord(m, rec, rp, clusteredIndex);
                } catch (RuntimeException e) {
                    // 单条失败：回滚当前 MTR 释放页 latch（不撤销已 commit 的前序 MTR），事务停在 ROLLING_BACK 可重试
                    mtrMgr.rollbackUncommitted(m);
                    throw e;
                }
                mtrMgr.commit(m);
                applied++;
                rp = prev;
            }
            slotManager.release(ctx.slotId());
        }
        writeRollbackCompleteRedo(txn);
        txnMgr.finishRollback(txn);
        return new RollbackSummary(applied);
    }

    /**
     * 创建 storage 内部保存点。v1 保存点挂在已经存在的 {@link UndoContext} 上，因此调用方应在事务首写之后
     * 使用本方法；首写前 statement guard 如何表达“空 undo 边界”留给后续 session/DML facade 接线。
     *
     * @param txn 当前 ACTIVE 且已经写过 undo 的事务。
     * @return 绑定到该事务 undo context 的保存点。
     */
    public TransactionSavepoint createSavepoint(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("savepoint transaction must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("createSavepoint requires ACTIVE transaction: " + txn.state());
        }
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            throw new TransactionStateException("createSavepoint requires an existing undo context");
        }
        return ctx.createSavepoint(txn);
    }

    /**
     * 回滚到事务保存点。数据流与完整 rollback 共用单条 undo 反向命令，但不进入事务终态：
     * <ol>
     *   <li>校验事务仍为 ACTIVE，保存点由同一事务创建。</li>
     *   <li>从 {@link UndoContext#lastRollPointer()} 指向的当前逻辑链头向前扫描。</li>
     *   <li>仅应用 {@code undoNo > savepoint.undoNo()} 的记录；每条仍用独立 MTR，失败只回滚当前 MTR 并传播。</li>
     *   <li>全部成功后把 {@link UndoContext} 逻辑链头退回保存点边界，并修剪保存点栈。</li>
     * </ol>
     * 本方法不释放 undo slot、不释放事务级 ReadView、不释放行锁、不写 {@code TRX_STATE_DELTA(ROLLED_BACK)}，
     * 因为数据库事务仍保持 ACTIVE，后续写入会继续追加到同一 undo segment。
     *
     * @param txn            当前 ACTIVE 事务。
     * @param clusteredIndex 该事务写入的聚簇索引（提供 keyDef/schema 解码 undo + 删除/恢复目标）。
     * @param savepoint      目标保存点。
     * @return 本次 partial rollback 实际应用的 undo record 条数。
     */
    public RollbackSummary rollbackToSavepoint(Transaction txn, BTreeIndex clusteredIndex,
                                               TransactionSavepoint savepoint) {
        if (txn == null || clusteredIndex == null || savepoint == null) {
            throw new DatabaseValidationException("rollbackToSavepoint txn/index/savepoint must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("rollbackToSavepoint requires ACTIVE transaction: " + txn.state());
        }
        if (savepoint.transaction() != txn) {
            throw new DatabaseValidationException("savepoint belongs to a different transaction");
        }

        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            return new RollbackSummary(0);
        }
        ctx.requireOwnedSavepoint(savepoint);

        int applied = 0;
        boolean reachedBoundary = savepoint.undoNo().isNone();
        RollPointer rp = ctx.lastRollPointer();
        while (!rp.isNull()) {
            MiniTransaction m = mtrMgr.begin();
            RollPointer prev;
            try {
                UndoLogSegment seg = undoAccess.open(m, ctx.undoFirstPageId(), PageLatchMode.SHARED);
                UndoRecord rec = seg.readRecord(rp, clusteredIndex.keyDef(), clusteredIndex.schema());
                if (rec.undoNo().value() <= savepoint.undoNo().value()) {
                    reachedBoundary = true;
                    mtrMgr.commit(m);
                    break;
                }
                prev = rec.prevRollPointer();
                applyUndoRecord(m, rec, rp, clusteredIndex);
            } catch (RuntimeException e) {
                mtrMgr.rollbackUncommitted(m);
                throw e;
            }
            mtrMgr.commit(m);
            applied++;
            rp = prev;
        }
        if (!reachedBoundary) {
            throw new DatabaseRuntimeException("savepoint boundary is not reachable from current undo chain");
        }
        ctx.completeRollbackToSavepoint(savepoint);
        return new RollbackSummary(applied);
    }

    /**
     * 写回滚完成 diagnostic redo。它在 {@code finishRollback} 之前执行，此时事务仍处于 ROLLING_BACK，
     * redo record 能记录真实的 from-state；若写入失败，事务仍停在 ROLLING_BACK，slot 对有 undo 的事务已释放，
     * 调用方会看到异常并按现有可重试/诊断路径处理。
     */
    private void writeRollbackCompleteRedo(Transaction txn) {
        MiniTransaction stateMtr = mtrMgr.begin();
        try {
            TransactionStateRedoDeltas.appendRollbackComplete(stateMtr, txn);
            mtrMgr.commit(stateMtr);
        } catch (RuntimeException e) {
            rollbackActiveStateMtr(stateMtr, e);
            throw e;
        }
    }

    private void rollbackActiveStateMtr(MiniTransaction stateMtr, RuntimeException original) {
        if (stateMtr.state() != MiniTransactionState.ACTIVE) {
            return;
        }
        try {
            mtrMgr.rollbackUncommitted(stateMtr);
        } catch (RuntimeException rollbackError) {
            original.addSuppressed(rollbackError);
        }
    }

    /**
     * 恢复期回滚一个 ACTIVE undo segment（R 1.2，§14.5）。**无 live {@link Transaction}**：直接从 undo segment 首页
     * + 显式配置的聚簇索引重建回滚——
     * <ol>
     *   <li>只读 MTR `open(SHARED)` + `forEachRecordWithPointer` **正向**收集 {@code (rec, rp)}（每条 record 自身地址）；</li>
     *   <li>**反向**（最后写的先撤）逐条独立 MTR `applyUndoRecord`（复用 INSERT/UPDATE/DELETE_MARK 反向命令）。</li>
     * </ol>
     * 由正式 UNDO_ROLLBACK 阶段从 page3 重建出的、且 undo 段 state=ACTIVE 的 slot 调用；不走 Transaction 状态机
     * （前台 {@link #rollback} 才走）、不在此释放 slot（恢复编排 release 内存 slot；page3 持久清留后续）。
     *
     * <p><b>单显式聚簇索引假设</b>（无 DD）：用 {@code clusteredIndex} 的 keyDef/schema 解码所有 undo；
     * {@code rec.indexId() != index.indexId()} 抛。<b>幂等</b>：`deleteClustered`/`replaceClustered`/`setClusteredDeleteMark`
     * 未命中即 no-op，故二次崩溃重复回滚安全。
     *
     * @param firstPageId    ACTIVE 事务的 undo segment 首页（来自恢复重建的 slot）。
     * @param clusteredIndex 显式配置的聚簇索引（提供 keyDef/schema 解码 undo + 删除/恢复目标）。
     * @return 本次回滚应用的 undo record 条数摘要。
     */
    public RollbackSummary rollbackRecovered(PageId firstPageId, BTreeIndex clusteredIndex) {
        if (firstPageId == null || clusteredIndex == null) {
            throw new DatabaseValidationException("rollbackRecovered firstPageId/clusteredIndex must not be null");
        }
        List<RecordAt> records = new ArrayList<>();
        MiniTransaction readMtr = mtrMgr.begin();
        try {
            UndoLogSegment seg = undoAccess.open(readMtr, firstPageId, PageLatchMode.SHARED);
            seg.forEachRecordWithPointer((rec, rp) -> records.add(new RecordAt(rec, rp)),
                    clusteredIndex.keyDef(), clusteredIndex.schema());
        } catch (RuntimeException e) {
            mtrMgr.rollbackUncommitted(readMtr);
            throw e;
        }
        mtrMgr.commit(readMtr);

        int applied = 0;
        for (int i = records.size() - 1; i >= 0; i--) {
            RecordAt at = records.get(i);
            MiniTransaction m = mtrMgr.begin();
            try {
                applyUndoRecord(m, at.record(), at.pointer(), clusteredIndex);
            } catch (RuntimeException e) {
                mtrMgr.rollbackUncommitted(m);
                throw e;
            }
            mtrMgr.commit(m);
            applied++;
        }
        return new RollbackSummary(applied);
    }

    /** 收集阶段的 (undo record, 其自身 roll pointer) 对；反向应用阶段按写入逆序撤销。 */
    private record RecordAt(UndoRecord record, RollPointer pointer) {
    }

    /**
     * 对单条 undo record 执行反向命令（Command/Template，§11.2 applyUndoRecord）。{@code expectedRollPtr} 传入正在
     * 应用的 roll pointer（= 写入时盖进记录的 DB_ROLL_PTR），与 {@code expectedTrxId} 一起作所有权校验，只改"本事务/
     * 本版本"那一行。
     * <ul>
     *   <li>{@code INSERT_ROW}：按 cluster key 物理删除未提交插入（{@code deleteClustered}）。</li>
     *   <li>{@code UPDATE_ROW}（T1.3e）：用 undo 内全量旧 image 重建 {@link LogicalRecord}（旧列 + 旧隐藏列，
     *       schema 稳定假设：版本=index.schema().schemaVersion()、未删、CONVENTIONAL），{@code replaceClustered}
     *       整记录恢复到更新前版本。</li>
     *   <li>{@code DELETE_MARK}（T1.3f）：{@code setClusteredDeleteMark(deleted=false, newHidden=rec.oldHiddenColumns(),
     *       expected=(rec.transactionId(), rp))} 取消删除标记并还原删除前旧隐藏列（恢复存活版本）。</li>
     * </ul>
     */
    private void applyUndoRecord(MiniTransaction mtr, UndoRecord rec, RollPointer rp, BTreeIndex index) {
        if (rec.indexId() != index.indexId()) {
            throw new DatabaseRuntimeException("undo indexId " + rec.indexId()
                    + " != rollback index " + index.indexId());
        }
        switch (rec.type()) {
            case INSERT_ROW ->
                    btree.deleteClustered(mtr, index, new SearchKey(rec.clusterKey()), rec.transactionId(), rp);
            case UPDATE_ROW -> {
                LogicalRecord oldRecord = new LogicalRecord(index.schema().schemaVersion(),
                        rec.oldColumnValues(), false, RecordType.CONVENTIONAL, rec.oldHiddenColumns());
                btree.replaceClustered(mtr, index, new SearchKey(rec.clusterKey()), oldRecord,
                        rec.transactionId(), rp);
            }
            case DELETE_MARK -> btree.setClusteredDeleteMark(mtr, index, new SearchKey(rec.clusterKey()),
                    false, rec.oldHiddenColumns(), rec.transactionId(), rp);
        }
    }
}
