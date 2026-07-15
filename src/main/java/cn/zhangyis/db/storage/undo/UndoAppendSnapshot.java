package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;

/**
 * 规划阶段读取的 undo segment append 快照。执行阶段在任何页修改前逐字段复核，防止过期计划覆盖新的 logical head。
 *
 * @param firstPageId   segment 稳定首页，也是 rseg slot owner。
 * @param lastPageId    当前主 UNDO FIL 链尾页，external payload 不改变该字段。
 * @param segmentId     FSP segment identity，external payload 页必须匹配。
 * @param inodeSlot     FSP inode slot，和 segmentId 共同拒绝跨段拼链。
 * @param transactionId 该 undo segment 的创建事务。
 * @param kind           v2 每页复制的 log kind；执行计划必须与目标 binding 精确一致。
 * @param lastUndoNo    物理 append 高水位，partial rollback 后不回退。
 * @param logicalHead   当前持久有效 rollback 链头。
 * @param recordCount   已物理追加的 root record 数量。
 * @param tailFreeOffset 主 UNDO 尾页下一槽偏移，用于写前决定是否 grow。
 */
public record UndoAppendSnapshot(PageId firstPageId, PageId lastPageId, SegmentId segmentId, int inodeSlot,
                                 TransactionId transactionId, UndoNo lastUndoNo, UndoLogicalHead logicalHead,
                                 UndoLogKind kind, long recordCount, int tailFreeOffset) {

    public UndoAppendSnapshot {
        if (firstPageId == null || lastPageId == null || segmentId == null || transactionId == null
                || lastUndoNo == null || logicalHead == null || kind == null) {
            throw new DatabaseValidationException("undo append snapshot fields must not be null");
        }
        if (!firstPageId.spaceId().equals(lastPageId.spaceId()) || segmentId.value() <= 0 || inodeSlot < 0
                || transactionId.isNone() || kind == UndoLogKind.TEMPORARY
                || recordCount < 0 || tailFreeOffset < UndoPageLayout.RECORD_AREA_START) {
            throw new DatabaseValidationException("invalid undo append snapshot bounds");
        }
    }
}
