package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;

/**
 * 事务聚合内部的 undo 子状态（设计 §5.3，T1.3c 首次接入事务语义）。挂在一颗 {@link Transaction} 上，由
 * {@code UndoLogManager.ensureUndoContext} 在事务首写时惰性创建，记录该事务的 undo segment 定位、
 * rollback segment/slot 归属，以及事务内 undo record 序号和回滚链入口。
 *
 * <p><b>本片字段范围</b>：{@code rollbackSegmentId}/{@code slotId}/{@code undoFirstPageId}/
 * {@code lastUndoNo}/{@code lastRollPointer}/{@code hasUpdateUndo}（T1.3e）。设计 §5.3 列出的
 * {@code savepointStack}/{@code modifiedTables}/{@code hasExternColumnUndo} 留后续片（savepoint/DELETE/extern）。
 *
 * <p><b>可变性边界</b>：{@code rollbackSegmentId}/{@code slotId}/{@code undoFirstPageId} 在创建时确定，
 * 本片不变；{@code lastUndoNo}/{@code lastRollPointer}/{@code hasUpdateUndo} 随每次 {@code beforeInsert}/
 * {@code beforeUpdate} 推进，仅由 {@code UndoLogManager} 经包内可见 setter 修改。{@code lastRollPointer} 是
 * **事务回滚链**入口（最新 undo record 位置），≠记录版本链入口；记录版本链由聚簇记录 {@code DB_ROLL_PTR} →
 * update undo → {@code oldHiddenColumns.dbRollPtr()} 重建（T1.4 用），见设计 §5.3/§7.5。
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
    /** 事务回滚链入口（最新 undo record 位置）；NULL 表「尚未 append」。 */
    private RollPointer lastRollPointer;
    /**
     * 本事务是否写过 UPDATE undo（T1.3e）。决定 commit 时是否回收 slot：纯 insert 事务可回收（insert undo 提交即
     * 可复用）；含 update 的事务必须保留 undo 段供 T1.4 MVCC/purge 读，故 {@code onCommit} 不回收其 slot。
     */
    private boolean hasUpdateUndo;

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

    /** 事务回滚链入口（最新 undo record 位置）；NULL 表尚未 append。 */
    public RollPointer lastRollPointer() {
        return lastRollPointer;
    }

    /** 本事务是否写过 UPDATE undo（决定 commit 是否回收 slot）。 */
    public boolean hasUpdateUndo() {
        return hasUpdateUndo;
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
     * 标记本事务已写 UPDATE undo。仅 {@code UndoLogManager.beforeUpdate} 在 append 成功后调用；单调置位，不复位
     * （一旦含 update undo，commit 即不可回收该段 slot）。
     */
    void markHasUpdateUndo() {
        this.hasUpdateUndo = true;
    }
}
