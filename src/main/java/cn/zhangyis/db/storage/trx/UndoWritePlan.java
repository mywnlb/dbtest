package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.UndoAppendSnapshot;
import cn.zhangyis.db.storage.undo.UndoRecordWritePlan;

/**
 * 事务层不可变 undo 写计划。它把事务内存链入口、持久 segment 快照和物理编码计划冻结在 redo admission 之前；
 * 执行器只接受同一 transaction id 且仍匹配该快照的计划。
 */
public final class UndoWritePlan {

    private final TransactionId transactionId;
    private final boolean firstWrite;
    private final PageId expectedFirstPageId;
    private final UndoNo expectedLastUndoNo;
    private final UndoNo expectedLogicalLastUndoNo;
    private final RollPointer expectedLastRollPointer;
    private final UndoAppendSnapshot persistentSnapshot;
    private final UndoRecordWritePlan recordPlan;
    private final int pagesToReserve;
    private final RedoBudgetWorkload redoWorkload;

    UndoWritePlan(TransactionId transactionId, boolean firstWrite, PageId expectedFirstPageId,
                  UndoNo expectedLastUndoNo, RollPointer expectedLastRollPointer,
                  UndoNo expectedLogicalLastUndoNo,
                  UndoAppendSnapshot persistentSnapshot, UndoRecordWritePlan recordPlan,
                  int pagesToReserve, RedoBudgetWorkload redoWorkload) {
        if (transactionId == null || expectedLastUndoNo == null || expectedLogicalLastUndoNo == null
                || expectedLastRollPointer == null
                || recordPlan == null || redoWorkload == null) {
            throw new DatabaseValidationException("undo write plan fields must not be null");
        }
        if (transactionId.isNone() || pagesToReserve < 0
                || (firstWrite && (expectedFirstPageId != null || persistentSnapshot != null))
                || (!firstWrite && (expectedFirstPageId == null || persistentSnapshot == null))) {
            throw new DatabaseValidationException("invalid undo write plan snapshot/bounds");
        }
        this.transactionId = transactionId;
        this.firstWrite = firstWrite;
        this.expectedFirstPageId = expectedFirstPageId;
        this.expectedLastUndoNo = expectedLastUndoNo;
        this.expectedLogicalLastUndoNo = expectedLogicalLastUndoNo;
        this.expectedLastRollPointer = expectedLastRollPointer;
        this.persistentSnapshot = persistentSnapshot;
        this.recordPlan = recordPlan;
        this.pagesToReserve = pagesToReserve;
        this.redoWorkload = redoWorkload;
    }

    /** 返回计划绑定的事务 id；执行时必须与目标事务精确一致。 */
    public TransactionId transactionId() { return transactionId; }
    /** 是否为该事务第一次创建 undo segment 的写入。 */
    public boolean firstWrite() { return firstWrite; }
    /** 本次 root/create/grow 与 payload 合计需要预留的新页数。 */
    public int pagesToReserve() { return pagesToReserve; }
    /** 交给 DML/MTR admission 的 undo 子工作量。 */
    public RedoBudgetWorkload redoWorkload() { return redoWorkload; }
    /** 完整 record 编码是否采用 external payload。 */
    public boolean external() { return recordPlan.external(); }
    /** external payload 精确页数；inline 返回 0。 */
    public int externalPageCount() { return recordPlan.externalPageCount(); }

    PageId expectedFirstPageId() { return expectedFirstPageId; }
    UndoNo expectedLastUndoNo() { return expectedLastUndoNo; }
    UndoNo expectedLogicalLastUndoNo() { return expectedLogicalLastUndoNo; }
    RollPointer expectedLastRollPointer() { return expectedLastRollPointer; }
    UndoAppendSnapshot persistentSnapshot() { return persistentSnapshot; }
    UndoRecordWritePlan recordPlan() { return recordPlan; }
}
