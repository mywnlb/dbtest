package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoAppendSnapshot;

/**
 * 事务拥有的一条独立 undo log 运行期绑定。slot/first page 在创建后不可变，logical head 只在 append 或
 * rollback marker 已成功提交后发布；append head 会在同一业务 MTR 的物理 undo 写完成后、聚簇写之前发布，
 * 后续业务 MTR 失败必须走 fail-stop。对象仅由所属事务线程修改，不承担跨事务同步。
 */
public final class UndoLogBinding {

    /** log 种类；只允许普通 INSERT/UPDATE，决定 record 类型与终结策略。 */
    private final UndoLogKind kind;
    /** page3 中独占的 slot；直到 commit/rollback/purge 原子终结后才可释放。 */
    private final UndoSlotId slotId;
    /** FSP segment 首页，也是该 log header 的持久权威入口。 */
    private final PageId firstPageId;
    /** 当前已持久化的局部逻辑链头；partial/full rollback 只推进所属 log 的该字段。 */
    private UndoLogicalHead logicalHead;
    /** 单 writer 最近一次 append/marker 后的完整物理快照；新 log 首条 append 发布前可为 null。 */
    private UndoAppendSnapshot appendSnapshot;

    public UndoLogBinding(UndoLogKind kind, UndoSlotId slotId, PageId firstPageId,
                          UndoLogicalHead logicalHead) {
        if (kind == null || slotId == null || firstPageId == null || logicalHead == null) {
            throw new DatabaseValidationException("undo log binding fields must not be null");
        }
        if (kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("temporary undo binding is not supported");
        }
        requireHeadKind(kind, logicalHead);
        this.kind = kind;
        this.slotId = slotId;
        this.firstPageId = firstPageId;
        this.logicalHead = logicalHead;
    }

    public UndoLogKind kind() { return kind; }
    public UndoSlotId slotId() { return slotId; }
    public PageId firstPageId() { return firstPageId; }
    public UndoLogicalHead logicalHead() { return logicalHead; }
    UndoAppendSnapshot appendSnapshot() { return appendSnapshot; }

    /** 在所属物理写已提交后发布新的局部头。 */
    void publishHead(UndoLogicalHead head) {
        if (head == null) {
            throw new DatabaseValidationException("undo log binding head must not be null");
        }
        requireHeadKind(kind, head);
        this.logicalHead = head;
        if (appendSnapshot != null) {
            appendSnapshot = new UndoAppendSnapshot(appendSnapshot.firstPageId(), appendSnapshot.lastPageId(),
                    appendSnapshot.segmentId(), appendSnapshot.inodeSlot(), appendSnapshot.transactionId(),
                    appendSnapshot.lastUndoNo(), head, appendSnapshot.kind(), appendSnapshot.recordCount(),
                    appendSnapshot.tailFreeOffset());
        }
    }

    /** append 页写与 header 推进成功后一起发布新的局部头和完整物理快照。 */
    void publishAppend(UndoLogicalHead head, UndoAppendSnapshot snapshot) {
        if (head == null || snapshot == null || !snapshot.firstPageId().equals(firstPageId)
                || snapshot.kind() != kind || !snapshot.logicalHead().equals(head)
                || !snapshot.lastUndoNo().equals(head.undoNo())) {
            throw new DatabaseValidationException("undo binding append snapshot does not match published head");
        }
        requireHeadKind(kind, head);
        this.logicalHead = head;
        this.appendSnapshot = snapshot;
    }

    /** 非空局部头的 RollPointer.insert 位必须与持久 log kind 一致。 */
    private static void requireHeadKind(UndoLogKind kind, UndoLogicalHead head) {
        if (!head.isEmpty() && head.rollPointer().insert() != (kind == UndoLogKind.INSERT)) {
            throw new DatabaseValidationException("undo logical head pointer kind does not match " + kind);
        }
    }
}
