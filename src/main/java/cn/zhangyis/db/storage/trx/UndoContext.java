package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;

import java.util.ArrayList;
import java.util.List;

/**
 * 事务聚合内部的 undo 子状态（设计 §5.3，T1.3c 首次接入事务语义）。挂在一颗 {@link Transaction} 上，由
 * {@code UndoLogManager.ensureUndoContext} 在事务首写时惰性创建，记录该事务的 undo segment 定位、
 * rollback segment/slot 归属，以及事务内 undo record 序号和回滚链入口。
 *
 * <p><b>本片字段范围</b>：{@code rollbackSegmentId}/{@code slotId}/{@code undoFirstPageId}/
 * {@code lastUndoNo}/{@code logicalLastUndoNo}/{@code lastRollPointer}/{@code hasUpdateUndo}/
 * {@code savepointStack}。设计 §5.3 列出的 {@code modifiedTables}/{@code hasExternColumnUndo} 留后续片。
 *
 * <p><b>可变性边界</b>：{@code rollbackSegmentId}/{@code slotId}/{@code undoFirstPageId} 在创建时确定，
 * 本片不变；{@code lastUndoNo} 是 append 高水位，partial rollback 不倒回，防止后续写入复用 undoNo；
 * {@code logicalLastUndoNo}/{@code lastRollPointer} 是当前有效回滚链头，rollback-to-savepoint 会把它们退回到
 * 保存点边界。{@code lastRollPointer} ≠记录版本链入口；记录版本链由聚簇记录 {@code DB_ROLL_PTR} →
 * update undo → {@code oldHiddenColumns.dbRollPtr()} 重建，见设计 §5.3/§7.5。
 *
 * <p><b>惰性初值</b>：刚建段尚未 append 时 {@code lastUndoNo=NONE}、{@code lastRollPointer=NULL}、
 * {@code hasUpdateUndo=false}，{@code undoFirstPageId} 为刚分配的 undo segment 首页。
 */
public final class UndoContext {

    /** 所属 rollback segment；本片固定单一默认 rseg，多 rseg 选择留后续片。 */
    private final RollbackSegmentId rollbackSegmentId;
    /** rseg 内认领的 slot；同一时间只归属本事务，回收留 T1.3d。 */
    private final UndoSlotId slotId;
    /**
     * 本事务 undo segment 链首页 id；后续 {@code beforeInsert}/{@code beforeUpdate} 经它 reopen 续 append。
     * T1.3e 起该段为 insert+update **混合** undo 段（单段简化）：段头 {@code UndoLogKind} 仅反映创建类型、非权威，
     * 每条记录的 {@code UndoRecordType} 首字节才是权威类型；独立 insert/update undo log 留 purge 片。
     */
    private final PageId undoFirstPageId;
    /** 事务内 undo record 序号；随每次 append 单调 +1，NONE 表「尚未 append」。 */
    private UndoNo lastUndoNo;
    /**
     * 当前逻辑回滚链头 undoNo。正常 append 时等于 {@link #lastUndoNo}；rollback-to-savepoint 成功后退回保存点
     * 边界，而 {@code lastUndoNo} 仍保留 append 高水位，保证后续 undoNo 不复用。
     */
    private UndoNo logicalLastUndoNo;
    /** 当前逻辑回滚链入口（最新有效 undo record 位置）；NULL 表「当前无有效 undo」。 */
    private RollPointer lastRollPointer;
    /**
     * 本事务是否写过 UPDATE undo（T1.3e）。决定 commit 时是否回收 slot：纯 insert 事务可回收（insert undo 提交即
     * 可复用）；含 update 的事务必须保留 undo 段供 T1.4 MVCC/purge 读，故 {@code onCommit} 不回收其 slot。
     */
    private boolean hasUpdateUndo;
    /**
     * 运行期保存点栈。保存点只记录 undo 边界，不持有物理资源；partial rollback 成功后会删除目标之后的保存点，
     * 目标本身保留以支持重复 rollback-to 同一边界。
     */
    private final List<TransactionSavepoint> savepointStack = new ArrayList<>();
    /** 当前 {@link #savepointStack} 的创建序号来源；仅由所属事务线程推进。 */
    private long nextSavepointSequence;

    /**
     * 构造一个「刚建段、尚未 append」的 undo context。{@code lastUndoNo} 初始化为 {@link UndoNo#NONE}，
     * {@code lastRollPointer} 初始化为 {@link RollPointer#NULL}。
     *
     * @param rollbackSegmentId 所属 rollback segment，不能为 null。
     * @param slotId            认领的 slot，不能为 null。
     * @param undoFirstPageId   本事务 undo segment 首页，不能为 null。
     */
    public UndoContext(RollbackSegmentId rollbackSegmentId, UndoSlotId slotId, PageId undoFirstPageId) {
        if (rollbackSegmentId == null || slotId == null || undoFirstPageId == null) {
            throw new DatabaseValidationException("undo context fields must not be null");
        }
        this.rollbackSegmentId = rollbackSegmentId;
        this.slotId = slotId;
        this.undoFirstPageId = undoFirstPageId;
        this.lastUndoNo = UndoNo.NONE;
        this.logicalLastUndoNo = UndoNo.NONE;
        this.lastRollPointer = RollPointer.NULL;
    }

    /** 所属 rollback segment。 */
    public RollbackSegmentId rollbackSegmentId() {
        return rollbackSegmentId;
    }

    /** 认领的 slot。 */
    public UndoSlotId slotId() {
        return slotId;
    }

    /** 本事务 undo segment 链首页 id（insert+update 混合段）。 */
    public PageId undoFirstPageId() {
        return undoFirstPageId;
    }

    /** 事务内最新 undo record 序号；NONE 表尚未 append。 */
    public UndoNo lastUndoNo() {
        return lastUndoNo;
    }

    /** 当前逻辑回滚链头 undoNo；partial rollback 后可能小于 {@link #lastUndoNo()}. */
    public UndoNo logicalLastUndoNo() {
        return logicalLastUndoNo;
    }

    /** 当前逻辑回滚链入口（最新有效 undo record 位置）；NULL 表当前逻辑链为空。 */
    public RollPointer lastRollPointer() {
        return lastRollPointer;
    }

    /**
     * 返回当前运行期有效 undo 链头的成对快照。部分回滚用该值作为持久 header 的 expected CAS 边界；
     * 任一字段若被错误地单独推进，{@link UndoLogicalHead} 会在落盘前暴露 pair 不一致。
     */
    UndoLogicalHead logicalHead() {
        return new UndoLogicalHead(logicalLastUndoNo, lastRollPointer);
    }

    /** 本事务是否写过 UPDATE undo（决定 commit 是否回收 slot）。 */
    public boolean hasUpdateUndo() {
        return hasUpdateUndo;
    }

    /** 当前运行期保存点数量，供诊断和测试确认栈修剪语义。 */
    int savepointCount() {
        return savepointStack.size();
    }

    /**
     * 推进事务内最新 undo record 序号。仅 {@code UndoLogManager} 在 append 成功后调用。
     *
     * @param undoNo 新 undoNo，不能为 null。
     */
    void setLastUndoNo(UndoNo undoNo) {
        if (undoNo == null) {
            throw new DatabaseValidationException("undo context lastUndoNo must not be null");
        }
        this.lastUndoNo = undoNo;
        this.logicalLastUndoNo = undoNo;
    }

    /**
     * 推进事务回滚链入口。仅 {@code UndoLogManager} 在 append 成功后调用；{@code RollPointer#NULL} 是合法值
     * （表「无前驱」），但 Java null 引用必须拒绝，避免隐藏 NPE 破坏回滚链不变量。
     *
     * @param rollPointer 新 roll pointer，不能为 Java null（可为 {@link RollPointer#NULL}）。
     */
    void setLastRollPointer(RollPointer rollPointer) {
        if (rollPointer == null) {
            throw new DatabaseValidationException("undo context lastRollPointer must not be null");
        }
        this.lastRollPointer = rollPointer;
    }

    /**
     * 创建运行期保存点。保存点捕获当前逻辑链头，而不是 append 高水位；这样 rollback-to 之后再写入时，
     * 新 undo record 仍会用 {@code lastUndoNo + 1}，不会复用已经写入过的 undoNo。
     *
     * @param txn 所属事务，必须仍为 ACTIVE。
     * @return 已压入本 context 保存点栈的保存点值对象。
     */
    TransactionSavepoint createSavepoint(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("savepoint transaction must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("savepoint requires an ACTIVE transaction: " + txn.state());
        }
        if (txn.undoContext() != null && txn.undoContext() != this) {
            throw new DatabaseValidationException("transaction is bound to a different undo context");
        }
        TransactionSavepoint savepoint = new TransactionSavepoint(
                txn, logicalLastUndoNo, lastRollPointer, nextSavepointSequence++);
        savepointStack.add(savepoint);
        return savepoint;
    }

    /**
     * rollback-to-savepoint 成功后的边界收尾。调用方必须已经把保存点之后的 undo record 全部反向应用成功；
     * 本方法只修改运行期逻辑链头和保存点栈，不访问 undo 页、不释放 slot、不改变事务状态。
     *
     * @param savepoint 本 {@link UndoContext} 创建的保存点。
     */
    void completeRollbackToSavepoint(TransactionSavepoint savepoint) {
        int index = requireOwnedSavepoint(savepoint);
        this.logicalLastUndoNo = savepoint.undoNo();
        this.lastRollPointer = savepoint.rollPointer();
        for (int i = savepointStack.size() - 1; i > index; i--) {
            savepointStack.remove(i);
        }
    }

    /**
     * statement rollback 成功退回“事务首写前”的空 undo 边界。调用方必须已经反向应用当前逻辑链上的全部
     * undo record；本方法只把运行期链头置空并清除所有保存点，不释放 undo slot，也不回退
     * {@link #lastUndoNo} append 高水位。保留高水位可保证同一事务后续写入不会复用已经落入 undo 页的序号。
     */
    void completeRollbackToEmptyBoundary() {
        this.logicalLastUndoNo = UndoNo.NONE;
        this.lastRollPointer = RollPointer.NULL;
        savepointStack.clear();
    }

    /**
     * 释放一个运行期保存点及其后创建的所有嵌套保存点。该操作只管理内存中的边界生命周期，不修改 undo 链，
     * 因而用于 statement guard 成功关闭或完成回滚后的收尾；目标保存点必须仍属于本 context。
     *
     * @param savepoint 要释放的保存点，同时作为嵌套范围的起点。
     */
    void releaseSavepoint(TransactionSavepoint savepoint) {
        int index = requireOwnedSavepoint(savepoint);
        for (int i = savepointStack.size() - 1; i >= index; i--) {
            savepointStack.remove(i);
        }
    }

    /**
     * 校验保存点确实由本 context 创建，并返回其栈位置。RollbackService 会在应用任何 undo 前先调用本方法，
     * 防止传入伪造或陈旧保存点时出现“数据已经被撤销，随后才发现边界非法”的半完成状态。
     *
     * @param savepoint 待校验保存点。
     * @return 保存点在 {@link #savepointStack} 中的位置。
     */
    int requireOwnedSavepoint(TransactionSavepoint savepoint) {
        if (savepoint == null) {
            throw new DatabaseValidationException("transaction savepoint must not be null");
        }
        int index = savepointStack.indexOf(savepoint);
        if (index < 0) {
            throw new DatabaseValidationException("savepoint does not belong to this undo context");
        }
        return index;
    }

    /**
     * 标记本事务已写 UPDATE undo。仅 {@code UndoLogManager.beforeUpdate} 在 append 成功后调用；单调置位，不复位
     * （一旦含 update undo，commit 即不可回收该段 slot）。
     */
    void markHasUpdateUndo() {
        this.hasUpdateUndo = true;
    }
}
