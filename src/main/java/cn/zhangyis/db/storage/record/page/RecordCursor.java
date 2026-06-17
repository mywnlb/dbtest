package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordFieldResolver;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * 页内单条记录的字段级读游标（innodb-record-design §14.2 读子集）。构造时一次性把该记录字节拷入本地数组并读出记录头；
 * 字段解析（NULL/切片/值）由 {@link RecordFieldResolver} **延迟**完成——首次字段访问才解析。
 *
 * <p>延迟解析的关键作用：infimum/supremum 系统记录是「8 头 + 8 标签」而非 schema 编码记录，对其建 cursor 时若立即解析会
 * 把标签误当字段。延迟后，比较器对哨兵记录只看 {@link #recordType()}（来自记录头）即提前返回，不触发字段解析。
 *
 * <p>并发：cursor 不拥有 page latch / buffer fix（§13.1）。它在构造时即把记录字节拷出，之后不再引用页；
 * 但调用方仍应保证构造发生在持有相应 latch 期间（读至少 S）。
 */
public final class RecordCursor {

    private final TableSchema schema;
    private final TypeCodecRegistry registry;
    /** 记录在页内的起始偏移（指向 RecordHeader）；用于产出 RecordRef。 */
    private final int recordOffset;
    /** 记录字节本地副本（构造时一次拷出，后续字段解析在其上进行）。 */
    private final byte[] bytes;
    /** 记录头（构造即解析，始终可用，哪怕是系统记录）。 */
    private final RecordHeader header;
    /** 延迟构造的字段解析结果；仅在首次字段访问时填充。 */
    private RecordFieldResolver.Resolved resolved;

    public RecordCursor(RecordPage page, int recordOffset, TableSchema schema, TypeCodecRegistry registry) {
        if (page == null || schema == null || registry == null) {
            throw new DatabaseValidationException("record cursor page/schema/registry must not be null");
        }
        this.schema = schema;
        this.registry = registry;
        this.recordOffset = recordOffset;
        this.bytes = page.readRecordBytes(recordOffset);
        this.header = RecordHeader.readFrom(bytes, 0);
    }

    /** 延迟解析记录字段；对系统记录不应调用（其字节非 schema 记录）。 */
    private RecordFieldResolver.Resolved resolved() {
        if (resolved == null) {
            resolved = new RecordFieldResolver(registry).resolve(bytes, schema);
        }
        return resolved;
    }

    /** 记录头（含 type/heapNo/nOwned/next/recordLength）。 */
    public RecordHeader recordHeader() {
        return header;
    }

    /** 页内 heap 物理序号。 */
    public int heapNo() {
        return header.heapNo();
    }

    /** delete-mark。 */
    public boolean isDeleted() {
        return header.deletedFlag();
    }

    /** 记录类型（CONVENTIONAL/NODE_POINTER/INFIMUM/SUPREMUM）。 */
    public RecordType recordType() {
        return header.recordType();
    }

    /** 列是否 NULL。 */
    public boolean isNull(ColumnId id) {
        return resolved().isNull(id.value());
    }

    /** 列的编码切片（NULL 列抛 RecordFormatException）。供比较器按保序字节比较，避免构造 ColumnValue。 */
    public FieldSlice columnSlice(ColumnId id) {
        return resolved().slice(id.value());
    }

    /** 列的逻辑值。 */
    public ColumnValue readColumn(ColumnId id) {
        return resolved().value(id.value());
    }

    /** 读 key part 对应列值。 */
    public ColumnValue readKeyPart(KeyPartDef part) {
        return readColumn(part.columnId());
    }

    /** 物化为完整逻辑记录。 */
    public LogicalRecord materialize() {
        return resolved().materialize();
    }

    /** 产出稳定定位值（pageOffset 为本游标记录的页内偏移）。 */
    public RecordRef recordRef(PageId pageId, long indexId) {
        return new RecordRef(pageId, header.heapNo(), recordOffset, schema.schemaVersion(), indexId);
    }

    /** 释放游标（不持 latch/fix，空操作；保留以贴合 §14.2 接口语义）。 */
    public void release() {
        // cursor 不拥有锁与 buffer fix，无需释放；记录字节已是本地副本。
    }
}
