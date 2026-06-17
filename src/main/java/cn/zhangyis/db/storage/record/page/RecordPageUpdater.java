package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * 页内 update 算子（innodb-record-design §10.2）。三模式：原地（新长≤旧长且 key 未变）、页内搬迁（变长且页内有空间）、
 * key 变化（返回 {@link UpdateOutcome#REQUIRES_REINSERT}，交调用方重定位）。
 *
 * <p>简化（trx/MVCC 暂停）：不写 undo、不改隐藏列；只更新存活的普通记录（拒绝系统记录与已 delete-marked 记录，
 * 后者避免 {@link RecordEncoder} 按 {@code newRecord.deleted()}=false 误"复活"删除标记）。无状态、线程安全；要求页 X latch。
 *
 * <p>**plan-then-execute**：校验、key 变化判定、（搬迁时）前驱与 owner 槽下标都在写页前算好；搬迁的第一处写页是
 * {@link HeapSpaceManager#allocate}（页满抛 overflow 时页未被修改）。
 */
public final class RecordPageUpdater {

    private final RecordEncoder encoder;
    private final RecordComparator comparator;
    private final TypeCodecRegistry registry;

    public RecordPageUpdater(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
        this.encoder = new RecordEncoder(registry);
        this.comparator = new RecordComparator(registry);
    }

    /**
     * 更新 {@code recordOffset} 处记录为 {@code newRecord}（要求 X）。返回 {@link UpdateResult}。
     *
     * @throws DatabaseValidationException 目标为系统记录或已 delete-marked。
     * @throws RecordPageOverflowException 搬迁所需空间不足（页未被修改）。
     * @throws PageDirectoryCorruptedException 搬迁定位前驱/owner 槽失败。
     */
    public UpdateResult update(RecordPage page, PageId pageId, int recordOffset, LogicalRecord newRecord,
                              IndexKeyDef keyDef, TableSchema schema) {
        // ---------- plan：校验 + key 变化判定 ----------
        RecordHeader oldHeader = page.recordHeaderAt(recordOffset);
        RecordType type = oldHeader.recordType();
        if (type == RecordType.INFIMUM || type == RecordType.SUPREMUM) {
            throw new DatabaseValidationException("cannot update system record at offset " + recordOffset);
        }
        if (oldHeader.deletedFlag()) {
            throw new DatabaseValidationException("cannot update delete-marked record at offset " + recordOffset);
        }
        RecordCursor oldCursor = new RecordCursor(page, recordOffset, schema, registry);
        SearchKey newKey = keyOf(newRecord, keyDef);
        if (comparator.compare(oldCursor, newKey, keyDef, schema) != 0) {
            return new UpdateResult(UpdateOutcome.REQUIRES_REINSERT, null);
        }
        byte[] newBytes = encoder.encode(newRecord, schema);
        int newLen = newBytes.length;
        int oldLen = oldHeader.recordLength();
        int oldHeapNo = oldHeader.heapNo();
        int oldNext = oldHeader.nextRecordOffset();
        int oldNOwned = oldHeader.nOwned();

        if (newLen <= oldLen) {
            // ---------- 原地 ----------
            page.writeRecordBytes(recordOffset, newBytes);
            // writeRecordBytes 覆盖整头：恢复 heapNo/next/n_owned（deleted 恒 false，前已拒绝 delete-marked）。
            page.setHeapNo(recordOffset, oldHeapNo);
            page.setNextRecord(recordOffset, oldNext);
            page.setNOwned(recordOffset, oldNOwned);
            if (newLen < oldLen) {
                page.writeHeader(page.header().withGarbage(page.header().garbage() + (oldLen - newLen)));
            }
            return new UpdateResult(UpdateOutcome.IN_PLACE,
                    new RecordRef(pageId, oldHeapNo, recordOffset, schema.schemaVersion(), keyDef.indexId()));
        }

        // ---------- 搬迁（newLen>oldLen）----------
        // plan（只读）：前驱、owner 槽下标。
        int prev = page.findPredecessor(recordOffset);
        RecordPageDirectory dir = page.directory();
        int slotH = -1;
        if (oldNOwned > 0) {
            slotH = dir.indexOf(recordOffset);
            if (slotH < 1) {
                throw new PageDirectoryCorruptedException("owner slot not found for moved record at " + recordOffset);
            }
        }
        // execute：allocate 为第一处写页（overflow 时页未改）。
        HeapSpaceManager.Allocation alloc = new HeapSpaceManager(page).allocate(newLen);
        int newOff = alloc.offset();
        int newHeapNo = alloc.heapNo();
        page.writeRecordBytes(newOff, newBytes);
        page.setHeapNo(newOff, newHeapNo);
        page.setNextRecord(newOff, oldNext);
        page.setNextRecord(prev, newOff);
        if (oldNOwned > 0) {
            dir.setSlot(slotH, newOff);
            page.setNOwned(newOff, oldNOwned);
        }
        new HeapSpaceManager(page).free(recordOffset);
        return new UpdateResult(UpdateOutcome.MOVED,
                new RecordRef(pageId, newHeapNo, newOff, schema.schemaVersion(), keyDef.indexId()));
    }

    /** 按 keyDef 的 part 顺序取 newRecord 的列值组 SearchKey（用于 key 变化检测）。 */
    private SearchKey keyOf(LogicalRecord rec, IndexKeyDef keyDef) {
        List<ColumnValue> vals = new ArrayList<>(keyDef.parts().size());
        for (KeyPartDef part : keyDef.parts()) {
            vals.add(rec.columnValues().get(part.columnId().value()));
        }
        return new SearchKey(vals);
    }
}
