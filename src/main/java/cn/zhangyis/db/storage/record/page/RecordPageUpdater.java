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

    /**
     * 本对象持有的 {@code encoder} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final RecordEncoder encoder;
    /**
     * 本对象持有的 {@code comparator} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final RecordComparator comparator;
    /**
     * 本对象持有的 {@code registry} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TypeCodecRegistry registry;

    /**
     * 创建 {@code RecordPageUpdater}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 目标为系统记录或已 delete-marked。
     * @throws RecordPageOverflowException 搬迁所需空间不足（页未被修改）。
     * @throws PageDirectoryCorruptedException 搬迁定位前驱/owner 槽失败。
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param recordOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param newRecord 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code update} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public UpdateResult update(RecordPage page, PageId pageId, int recordOffset, LogicalRecord newRecord,
                              IndexKeyDef keyDef, TableSchema schema) {
        // ---------- plan：校验 + key 变化判定 ----------
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
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
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
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
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
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
