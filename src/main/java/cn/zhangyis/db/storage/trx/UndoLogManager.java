package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoRecord;

import java.util.List;

/**
 * 事务 undo 门面（设计 §5.2/§7.1/§7.2）。在 {@code storage.trx} 持事务语义，调用
 * {@code storage.undo} 物理设施（{@link UndoLogSegmentAccess}）写 INSERT/UPDATE/DELETE_MARK undo record，
 * 返回真 {@link RollPointer} 供聚簇记录盖 {@code DB_ROLL_PTR}，并在 commit/rollback/recovery/purge
 * 流程中维护 undo slot 与 history list。
 *
 * <p><b>依赖方向</b>：{@code storage.trx → storage.undo}。本类 import {@link MiniTransaction}（物理短事务）、
 * {@link UndoLogSegmentAccess}/{@link UndoRecord}（undo 物理设施）与 record schema/type；不反向暴露
 * {@link Transaction} 内部给 undo。{@code storage.undo} 不 import 本类或 {@code Transaction}。
 *
 * <p><b>当前范围</b>：支持 {@code beforeInsert}/{@code beforeUpdate}/{@code beforeDelete}、
 * {@code onCommit}、持久 rseg header 以及恢复期 active slot 扫描的基础编排；savepoint、statement rollback、
 * prepared transaction 与多 rseg 调度仍留后续切片。
 *
 * <p><b>WAL</b>（§7.2）：{@code beforeX} 在调用方传入的 MTR 内追加 undo record；调用方随后在同一 MTR
 * 写聚簇记录，commit 时 undo page redo 与 index page redo 同批 durable。由于 MTR rollback 不撤销页内容，
 * 已写 undo 但聚簇修改失败的边界由事务级 {@link RollbackService} 幂等回滚兜底；statement/savepoint
 * 级别原子性仍留后续。
 *
 * <p><b>并发</b>：slot 认领由 {@link RollbackSegmentSlotManager} 的 {@code ReentrantLock} 串行，锁内不分配页、
 * 不访问 BufferPool、不等待 IO；页分配（{@link UndoLogSegmentAccess#create}）在 slot 锁外完成。本片单 writer
 * 假设：同一 undo segment 同时只有一个 EXCLUSIVE append 会话（同事务串行 beforeInsert）。
 */
public final class UndoLogManager {

    /** undo 物理设施入口；{@code beforeX} 经它 create/open undo segment 并 append 对应版本记录。 */
    private final UndoLogSegmentAccess access;
    /** 内存 rseg slot 目录；首写时认领 slot 登记 insert undo 首页。固定单一默认 rseg。 */
    private final RollbackSegmentSlotManager slotManager;
    /** undo 表空间；{@code ensureUndoContext} 首写建段时传给 {@link UndoLogSegmentAccess#create}。 */
    private final SpaceId undoSpace;
    /** 已提交 undo log 的 history list；{@code onCommit} 据事务类型挂入 committed / insert-reclaim 队列供 purge 回收。 */
    private final HistoryList history;
    /**
     * 持久 rseg header 仓储（0.3，可选）。非 null 时把 slot claim/release 持久化到 undo 表空间 page3（redo 保护），
     * 使崩溃后恢复扫描可重建内存 slot 目录。为 null 时纯内存（既有 test-only 行为，无持久/恢复）。
     */
    private final RollbackSegmentHeaderRepository headerRepo;
    /** 短 MTR 来源（0.3，可选）；onCommit 释放 slot 时持久化 page3 用，与 {@link #headerRepo} 同时提供才生效。 */
    private final MiniTransactionManager mtrManager;

    /**
     * 构造纯内存 undo 门面（既有 test-only：不持久 rseg header、不可恢复 slot 目录）。
     */
    public UndoLogManager(UndoLogSegmentAccess access, RollbackSegmentSlotManager slotManager, SpaceId undoSpace,
                          HistoryList history) {
        this(access, slotManager, undoSpace, history, null, null);
    }

    /**
     * 构造可持久 rseg header 的 undo 门面（0.3）。{@code headerRepo != null} 时 slot claim 在调用方 MTR 内持久到
     * page3、纯 insert 事务 {@code onCommit} 释放在短 MTR 内清空 page3 槽；崩溃后由启动恢复扫描 page3 重建。
     *
     * @param access      undo 物理设施入口，不能为 null。
     * @param slotManager 内存 rseg slot 目录，不能为 null。
     * @param undoSpace   undo 表空间，不能为 null。
     * @param history     已提交 undo log 的 history list，不能为 null。
     * @param headerRepo  持久 rseg header 仓储；为 null 则纯内存。
     * @param mtrManager  短 MTR 来源；持久 release 用，{@code headerRepo} 非 null 时必须一并提供。
     */
    public UndoLogManager(UndoLogSegmentAccess access, RollbackSegmentSlotManager slotManager, SpaceId undoSpace,
                          HistoryList history, RollbackSegmentHeaderRepository headerRepo,
                          MiniTransactionManager mtrManager) {
        if (access == null || slotManager == null || undoSpace == null || history == null) {
            throw new DatabaseValidationException("undo log manager args must not be null");
        }
        if (headerRepo != null && mtrManager == null) {
            throw new DatabaseValidationException("persistent rseg header requires a mini transaction manager");
        }
        this.access = access;
        this.slotManager = slotManager;
        this.undoSpace = undoSpace;
        this.history = history;
        this.headerRepo = headerRepo;
        this.mtrManager = mtrManager;
    }

    /**
     * INSERT undo 写路径（§7.2 步骤 3）。数据流：
     * <ol>
     *   <li>校验事务 ACTIVE 且已分配写 id（{@code assignWriteId} 须先于本方法）。</li>
     *   <li>{@code ensureUndoContext}：首写时 {@link UndoLogSegmentAccess#create} 建 insert undo segment +
     *       {@link RollbackSegmentSlotManager#claim} 占内存 slot + 构造 {@link UndoContext} 绑定到事务；
     *       后续写 {@link UndoLogSegmentAccess#open} 按 {@code ctx.undoFirstPageId} 重开 EXCLUSIVE 续 append。</li>
     *   <li>分配 {@code undoNo = ctx.lastUndoNo + 1}（首条为 1）。</li>
     *   <li>组 {@link UndoRecord}（INSERT_ROW，{@code prevRollPointer = ctx.lastRollPointer}，首条为 NULL）。</li>
     *   <li>{@link UndoLogSegment#append} 写入 undo 页（同 MTR redo），返回 insert {@link RollPointer}。</li>
     *   <li>推进 {@code ctx.lastUndoNo/lastRollPointer}，返回 roll pointer 供聚簇记录盖 {@code DB_ROLL_PTR}。</li>
     * </ol>
     *
     * <p>异常时不在事务上绑定 {@link UndoContext}、不推进 {@code lastUndoNo}：append 抛出（页溢出等）由调用方
     * 决定 MTR rollback；MTR rollback 不撤销页内容是已知缺口（orphan undo 风险，留 T1.3d+）。
     *
     * @param txn        当前事务，必须 ACTIVE 且已分配写 id。
     * @param mtr        当前物理短事务；undo append 与聚簇写同 MTR，commit 同批 redo。
     * @param tableId    表 id（rollback 定位用）。
     * @param indexId    聚簇索引 id（rollback 定位用）。
     * @param clusterKey 主键列值，顺序对应 {@code keyDef.parts()}；可含 {@link ColumnValue.NullValue}。
     * @param keyDef     聚簇索引 key 定义。
     * @param schema     表 schema（codec 按 columnId 解析类型）。
     * @return 指向刚追加的 INSERT undo record 的 roll pointer（非 NULL，insert flag=true）。
     */
    public RollPointer beforeInsert(Transaction txn, MiniTransaction mtr, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, IndexKeyDef keyDef, TableSchema schema) {
        if (txn == null || mtr == null || clusterKey == null || keyDef == null || schema == null) {
            throw new TransactionStateException("beforeInsert args (txn/mtr/clusterKey/keyDef/schema) must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("beforeInsert requires an ACTIVE transaction: " + txn.state());
        }
        TransactionId txnId = txn.transactionId();
        if (txnId.isNone()) {
            throw new TransactionStateException(
                    "beforeInsert requires a non-NONE transaction id; call TransactionManager.assignWriteId first");
        }

        UndoLogSegment seg = ensureUndoContext(txn, mtr, txnId);
        UndoContext ctx = txn.undoContext();

        // undoNo 单调递增；NONE.value()=0 → 首条为 1（UndoRecord 构造器要求 undoNo > 0）
        UndoNo undoNo = UndoNo.of(ctx.lastUndoNo().value() + 1);
        UndoRecord rec = UndoRecord.insert(undoNo, txnId, tableId, indexId, clusterKey, ctx.lastRollPointer());

        RollPointer rp = seg.append(rec, keyDef, schema);

        // append 成功后才推进事务 undo 子状态；失败时 ctx 保持旧值，避免回滚链入口指向未写入的 record
        ctx.setLastUndoNo(undoNo);
        ctx.setLastRollPointer(rp);
        return rp;
    }

    /**
     * UPDATE undo 写路径（§7.3，T1.3e）。在聚簇记录被本事务更新前调用，记录**全量旧 image**（更新前全列值 +
     * 更新前隐藏列），返回新 roll pointer 供调用方盖入记录的 {@code DB_ROLL_PTR}。数据流（与 {@link #beforeInsert}
     * 同构）：校验 ACTIVE + 已分配写 id → {@code ensureUndoContext}（复用本事务混合 undo 段，续 append）→ 分配
     * {@code undoNo=lastUndoNo+1} → 组 {@link UndoRecord#update}（{@code prevRollPointer=ctx.lastRollPointer}，事务回滚链）
     * → {@code append}（返回 insert=false 的 rp）→ 推进 ctx + 标 {@code hasUpdateUndo}。
     *
     * <p><b>两条链</b>：返回的 rp 是记录的新 {@code DB_ROLL_PTR}；undo 内 {@code oldHiddenColumns.dbRollPtr()} 是
     * 记录版本链的上一版本指针（T1.4 MVCC 用）；{@code prevRollPointer} 是事务回滚链（RollbackService 用）。两者不同。
     *
     * <p>异常时（如旧 image 超单页 {@link cn.zhangyis.db.storage.undo.UndoPageOverflowException}）不推进 ctx；
     * 全量旧 image 不支持 extern undo payload（超页即抛，T1.3e 非目标）。
     *
     * @param oldColumnValues  更新前全列值（按 schema 列序），不能为 null。
     * @param oldHiddenColumns 更新前隐藏列（旧 DB_TRX_ID/DB_ROLL_PTR），不能为 null。
     * @return 指向刚追加 UPDATE undo record 的 roll pointer（非 NULL，insert flag=false）。
     */
    public RollPointer beforeUpdate(Transaction txn, MiniTransaction mtr, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns, IndexKeyDef keyDef, TableSchema schema) {
        if (txn == null || mtr == null || clusterKey == null || oldColumnValues == null
                || oldHiddenColumns == null || keyDef == null || schema == null) {
            throw new TransactionStateException(
                    "beforeUpdate args (txn/mtr/clusterKey/oldColumnValues/oldHiddenColumns/keyDef/schema) must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("beforeUpdate requires an ACTIVE transaction: " + txn.state());
        }
        TransactionId txnId = txn.transactionId();
        if (txnId.isNone()) {
            throw new TransactionStateException(
                    "beforeUpdate requires a non-NONE transaction id; call TransactionManager.assignWriteId first");
        }

        UndoLogSegment seg = ensureUndoContext(txn, mtr, txnId);
        UndoContext ctx = txn.undoContext();

        UndoNo undoNo = UndoNo.of(ctx.lastUndoNo().value() + 1);
        UndoRecord rec = UndoRecord.update(undoNo, txnId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, ctx.lastRollPointer());

        RollPointer rp = seg.append(rec, keyDef, schema);

        // append 成功后才推进 ctx 并标记 hasUpdateUndo（决定 commit 不回收 slot）；失败时 ctx 保持旧值
        ctx.setLastUndoNo(undoNo);
        ctx.setLastRollPointer(rp);
        ctx.markHasUpdateUndo();
        return rp;
    }

    /**
     * DELETE-mark undo 写路径（§7.3/§16.3，T1.3f）。在聚簇记录被本事务 delete-mark 前调用，记录删除前**存活**版本的
     * 全量旧 image（列不变 + 旧隐藏列），返回新 roll pointer 供盖入记录的 {@code DB_ROLL_PTR}（连同删除位）。与
     * {@link #beforeUpdate} 同构（仅 undo 类型为 DELETE_MARK）：返回 insert=false 的 rp，标 {@code hasUpdateUndo}
     * （delete undo 服务旧 ReadView/purge，commit 不回收 slot）；版本链遍历与 UPDATE 同路（{@code oldHidden.dbRollPtr}）。
     *
     * @param oldColumnValues  删除前全列值（按 schema 列序），不能为 null。
     * @param oldHiddenColumns 删除前隐藏列（旧 DB_TRX_ID/DB_ROLL_PTR），不能为 null。
     * @return 指向刚追加 DELETE_MARK undo record 的 roll pointer（非 NULL，insert flag=false）。
     */
    public RollPointer beforeDelete(Transaction txn, MiniTransaction mtr, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns, IndexKeyDef keyDef, TableSchema schema) {
        if (txn == null || mtr == null || clusterKey == null || oldColumnValues == null
                || oldHiddenColumns == null || keyDef == null || schema == null) {
            throw new TransactionStateException(
                    "beforeDelete args (txn/mtr/clusterKey/oldColumnValues/oldHiddenColumns/keyDef/schema) must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("beforeDelete requires an ACTIVE transaction: " + txn.state());
        }
        TransactionId txnId = txn.transactionId();
        if (txnId.isNone()) {
            throw new TransactionStateException(
                    "beforeDelete requires a non-NONE transaction id; call TransactionManager.assignWriteId first");
        }

        UndoLogSegment seg = ensureUndoContext(txn, mtr, txnId);
        UndoContext ctx = txn.undoContext();

        UndoNo undoNo = UndoNo.of(ctx.lastUndoNo().value() + 1);
        UndoRecord rec = UndoRecord.deleteMark(undoNo, txnId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, ctx.lastRollPointer());

        RollPointer rp = seg.append(rec, keyDef, schema);

        ctx.setLastUndoNo(undoNo);
        ctx.setLastRollPointer(rp);
        ctx.markHasUpdateUndo();
        return rp;
    }

    /**
     * 提交回收 undo slot（T1.3d，对齐 InnoDB {@code trx_undo_insert_cleanup}）。数据流：
     * <ul>
     *   <li>未写事务（{@code undoContext()==null}）：no-op。</li>
     *   <li>纯 insert 事务（{@code !hasUpdateUndo()}）：insert undo 提交后不再服务一致性读，立即
     *       {@link RollbackSegmentSlotManager#release} 释放 slot 供后续事务重认领。</li>
     *   <li>含 update undo 事务（{@code hasUpdateUndo()}，T1.3e）：**保留** slot/段——committed update undo 仍可能被
     *       T1.4 MVCC 旧版本读 / purge 需要，不能在 commit 即回收。其回收留 purge/undo truncation 片（已知缺口）。</li>
     * </ul>
     *
     * <p><b>commit 编排</b>：{@code TransactionManager.commit()} 保持纯内存状态、**不**自动调用本方法；2.1 起
     * {@code ClusteredDmlService.commit} 先通过 {@code TransactionManager.prepareCommit(txn)} 预留提交号，
     * 再调用本方法持久化 undo commit marker，最后才 {@code commit(txn)} 移出 active table。本方法只处理
     * slot/history 状态，undo record/page/segment 的物理回收留 purge/truncation 片。
     *
     * @param txn 提交中的事务，不能为 null。
     */
    public void onCommit(Transaction txn) {
        if (txn == null) {
            throw new TransactionStateException("onCommit txn must not be null");
        }
        UndoContext ctx = txn.undoContext();
        if (ctx == null) {
            return; // 未写事务：无 undo 段
        }
        // R 1.2 + 0.3：短 MTR 内标 undo 段首页 STATE=COMMITTED（恢复期据此跳过已提交事务回滚）；纯 insert 同 MTR 持久
        // 清 page3 槽（0.3 release）。mtrManager 为 null（纯内存 fixture）时不持久——既有 test-only 行为不变。
        if (mtrManager != null) {
            MiniTransaction commitMtr = mtrManager.begin();
            try {
                // R 1.3：标 COMMITTED 同时写提交序号（txn.transactionNo()，commit 前已预留），供恢复重建 committed history。
                access.open(commitMtr, ctx.undoFirstPageId(), PageLatchMode.EXCLUSIVE)
                        .markCommitted(txn.transactionNo());
                if (!ctx.hasUpdateUndo() && headerRepo != null) {
                    writeRsegSlotAfterUndoPage(commitMtr, ctx.slotId(), null);
                }
                mtrManager.commit(commitMtr);
            } catch (RuntimeException e) {
                rollbackCommitMtr(commitMtr, e);
                throw e;
            }
        }
        if (ctx.hasUpdateUndo()) {
            // 含 update/delete undo：挂入 history list 供 purge 按 TransactionNo boundary 回收（slot/段由 purge 释放，
            // 不在此 release）。要求 commit 已分配 TransactionNo——编排序必须先 commit 再 onCommit（评审 #3）。
            TransactionNo no = txn.transactionNo();
            if (no.isNone()) {
                throw new TransactionStateException(
                        "onCommit requires an assigned TransactionNo for an update-undo transaction; "
                                + "call TransactionManager.prepareCommit before onCommit");
            }
            history.submitCommitted(new HistoryEntry(no, txn.transactionId(), undoSpace,
                    ctx.undoFirstPageId(), ctx.slotId()));
        } else {
            // 纯 insert undo：提交即不再服务一致性读（§7.2）。持久清 page3 已在上面同 MTR 完成，提交成功后才释放内存 slot。
            slotManager.release(ctx.slotId());
            history.submitInsertReclaim(new InsertReclaimEntry(undoSpace, ctx.undoFirstPageId()));
        }
    }

    private void rollbackCommitMtr(MiniTransaction commitMtr, RuntimeException original) {
        if (commitMtr.state() != MiniTransactionState.ACTIVE) {
            return;
        }
        try {
            mtrManager.rollbackUncommitted(commitMtr);
        } catch (RuntimeException rollbackError) {
            original.addSuppressed(rollbackError);
        }
    }

    /**
     * 惰性确保事务有 {@link UndoContext} 与可写 insert undo segment。数据流：
     * <ul>
     *   <li>事务首写（{@code txn.undoContext() == null}）：{@link UndoLogSegmentAccess#create} 建 insert undo
     *       segment（含首页格式化，X 持）→ {@link RollbackSegmentSlotManager#claim} 占内存 slot 登记首页 →
     *       构造 {@link UndoContext}（rseg=slotManager 默认 rseg，slot，首页，NONE/NULL 初值）→
     *       {@link Transaction#setUndoContext} 绑定。</li>
     *   <li>后续写：{@link UndoLogSegmentAccess#open} 按 {@code ctx.undoFirstPageId} 重开 EXCLUSIVE 续 append。</li>
     * </ul>
     * <p>slot 认领在页分配之后、锁内只登记 firstPageId，不分配页、不访问 BufferPool（§9.3 锁序）。
     *
     * @return 当前 MTR 内可写（EXCLUSIVE）的 undo segment 句柄。
     */
    private UndoLogSegment ensureUndoContext(Transaction txn, MiniTransaction mtr, TransactionId txnId) {
        UndoContext existing = txn.undoContext();
        if (existing != null) {
            return access.open(mtr, existing.undoFirstPageId(), PageLatchMode.EXCLUSIVE);
        }
        // 首写：建 insert undo segment（页分配在 slot 锁外完成）
        UndoLogSegment seg = access.create(mtr, undoSpace, txnId);
        PageId firstPageId = seg.firstPageId();
        UndoSlotId slot = slotManager.claim(firstPageId);
        // 0.3：在**同一 MTR** 内把 slot 登记持久化到 page3 rseg header——与 undo segment 创建同批 redo，crash-safe；
        // 任一失败由 MTR rollback 一并不发布（避免 page3 引用不存在的段，或段不被任何 slot 引用）。
        if (headerRepo != null) {
            writeRsegSlotAfterUndoPage(mtr, slot, firstPageId);
        }
        UndoContext ctx = new UndoContext(slotManager.rollbackSegmentId(), slot, firstPageId);
        txn.setUndoContext(ctx);
        return seg;
    }

    /**
     * 持久化 rseg slot 的 page-latch-order 例外。首写和提交清理都已经在同一 MTR 持有 undo first 页 X latch；
     * 该页可能被格式化、append 或 markCommitted 写过，不能提前释放，否则 MTR commit 盖 pageLSN 会失去 X guard。
     *
     * <p>局部无环前提：rseg header(page3) 只记录 slot->firstPageNo 映射，不会读取/等待 undo 页内容；undo
     * segment 的普通写路径也不会在持 page3 latch 时反向请求 undo page latch。因此该例外只放在 UndoLogManager
     * 的事务 undo 编排层，不下沉到 repository 的所有调用。
     */
    private void writeRsegSlotAfterUndoPage(MiniTransaction mtr, UndoSlotId slot, PageId firstPage) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "undo rseg slot update: page3 metadata never waits for undo page latches")) {
            headerRepo.writeSlot(mtr, undoSpace, slot, firstPage);
        }
    }
}
