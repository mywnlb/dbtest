package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.UndoAppendSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoRecordWritePlan;

/**
 * 事务层不可变 undo 写计划。它同时冻结事务全局 undoNo 高水位与目标 kind 的局部 binding/header 快照，
 * 从而允许 INSERT/UPDATE log 的本地序号有间隙，又能在进入物理写前拒绝同事务的陈旧计划。
 */
public final class UndoWritePlan {

    private final TransactionId transactionId;
    private final UndoLogKind kind;
    private final UndoSegmentAcquisition acquisition;
    private final PageId expectedFirstPageId;
    private final UndoNo expectedGlobalLastUndoNo;
    private final UndoLogicalHead expectedLogicalHead;
    private final UndoAppendSnapshot persistentSnapshot;
    private final UndoSegmentReuseDirectory.CacheCandidate cachedCandidate;
    private final UndoSegmentReuseDirectory.FreeCandidate freeCandidate;
    private final UndoRecordWritePlan recordPlan;
    private final int pagesToReserve;
    private final RedoBudgetWorkload redoWorkload;

    UndoWritePlan(TransactionId transactionId, UndoLogKind kind, UndoSegmentAcquisition acquisition,
                  PageId expectedFirstPageId,
                   UndoNo expectedGlobalLastUndoNo, UndoLogicalHead expectedLogicalHead,
                   UndoAppendSnapshot persistentSnapshot,
                   UndoSegmentReuseDirectory.CacheCandidate cachedCandidate,
                   UndoSegmentReuseDirectory.FreeCandidate freeCandidate,
                   UndoRecordWritePlan recordPlan,
                   int pagesToReserve, RedoBudgetWorkload redoWorkload) {
        if (transactionId == null || kind == null || acquisition == null || expectedGlobalLastUndoNo == null
                || expectedLogicalHead == null || recordPlan == null || redoWorkload == null) {
            throw new DatabaseValidationException("undo write plan fields must not be null");
        }
        boolean invalidTarget = switch (acquisition) {
            case ALLOCATE_NEW -> expectedFirstPageId != null || persistentSnapshot != null
                    || cachedCandidate != null || freeCandidate != null || !expectedLogicalHead.isEmpty();
            case REUSE_CACHED -> expectedFirstPageId == null || persistentSnapshot != null
                    || cachedCandidate == null || freeCandidate != null || !expectedLogicalHead.isEmpty()
                    || !expectedFirstPageId.equals(cachedCandidate.segment().handle().firstPageId())
                    || cachedCandidate.segment().kind() != kind;
            case REUSE_FREE -> expectedFirstPageId == null || persistentSnapshot != null
                    || cachedCandidate != null || freeCandidate == null || !expectedLogicalHead.isEmpty()
                    || !expectedFirstPageId.equals(freeCandidate.segment().handle().firstPageId());
            case APPEND_EXISTING -> expectedFirstPageId == null || persistentSnapshot == null
                    || cachedCandidate != null || freeCandidate != null;
        };
        if (transactionId.isNone() || kind == UndoLogKind.TEMPORARY || pagesToReserve < 0 || invalidTarget) {
            throw new DatabaseValidationException("invalid undo write plan snapshot/bounds");
        }
        this.transactionId = transactionId;
        this.kind = kind;
        this.acquisition = acquisition;
        this.expectedFirstPageId = expectedFirstPageId;
        this.expectedGlobalLastUndoNo = expectedGlobalLastUndoNo;
        this.expectedLogicalHead = expectedLogicalHead;
        this.persistentSnapshot = persistentSnapshot;
        this.cachedCandidate = cachedCandidate;
        this.freeCandidate = freeCandidate;
        this.recordPlan = recordPlan;
        this.pagesToReserve = pagesToReserve;
        this.redoWorkload = redoWorkload;
    }

    public TransactionId transactionId() { return transactionId; }
    public UndoLogKind kind() { return kind; }
    /** 本次 append 的 segment 获取方式。 */
    public UndoSegmentAcquisition acquisition() { return acquisition; }
    /** 是否为目标 kind 开启新的事务局部 undo log（fresh allocation 与 cached reuse 都为 true）。 */
    public boolean startsNewLogicalLog() { return acquisition.startsNewLogicalLog(); }
    /** 兼容旧调用名称；语义等同于 {@link #startsNewLogicalLog()}。 */
    public boolean newLog() { return startsNewLogicalLog(); }
    public int pagesToReserve() { return pagesToReserve; }
    public RedoBudgetWorkload redoWorkload() { return redoWorkload; }
    public boolean external() { return recordPlan.external(); }
    public int externalPageCount() { return recordPlan.externalPageCount(); }

    PageId expectedFirstPageId() { return expectedFirstPageId; }
    UndoNo expectedGlobalLastUndoNo() { return expectedGlobalLastUndoNo; }
    UndoLogicalHead expectedLogicalHead() { return expectedLogicalHead; }
    UndoAppendSnapshot persistentSnapshot() { return persistentSnapshot; }
    UndoSegmentReuseDirectory.CacheCandidate cachedCandidate() { return cachedCandidate; }
    UndoSegmentReuseDirectory.FreeCandidate freeCandidate() { return freeCandidate; }
    UndoRecordWritePlan recordPlan() { return recordPlan; }
}
