package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;

/**
 * 页内 delete-mark 算子（innodb-record-design §10.3 delete-mark 子集）。逻辑删除：只把记录头 deleted 位置 1，
 * 记录仍留在 next_record 链与 PageDirectory 中（nRecs 含 delete-marked，§7）；物理摘除/空间回收归 {@link RecordPagePurger}。
 *
 * <p>简化（trx/MVCC 暂停）：不写 undo、不更新隐藏列（DB_TRX_ID/DB_ROLL_PTR 未实现）。无状态、线程安全；要求调用方持页 X latch。
 */
public final class RecordPageDeleter {

    /**
     * 对 {@code recordOffset} 处的用户记录置 delete-mark（要求 X）。
     *
     * @throws DatabaseValidationException 目标为 infimum/supremum 系统记录，或已被 delete-mark（强制 lifecycle，避免重复删除）。
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param recordOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     */
    public void deleteMark(RecordPage page, int recordOffset) {
        RecordHeader header = page.recordHeaderAt(recordOffset);
        RecordType type = header.recordType();
        if (type == RecordType.INFIMUM || type == RecordType.SUPREMUM) {
            throw new DatabaseValidationException("cannot delete-mark system record at offset " + recordOffset);
        }
        if (header.deletedFlag()) {
            throw new DatabaseValidationException("record already delete-marked at offset " + recordOffset);
        }
        page.setDeleted(recordOffset, true);
    }
}
