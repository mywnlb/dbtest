package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordFieldResolver;
import cn.zhangyis.db.storage.record.format.RecordFormatException;
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

    /**
     * 本对象持有的 {@code schema} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final TableSchema schema;
    /**
     * 本对象持有的 {@code registry} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TypeCodecRegistry registry;
    /** 记录在页内的起始偏移（指向 RecordHeader）；用于产出 RecordRef。 */
    private final int recordOffset;
    /** 记录字节本地副本（构造时一次拷出，后续字段解析在其上进行）。 */
    private final byte[] bytes;
    /** 记录头（构造即解析，始终可用，哪怕是系统记录）。 */
    private final RecordHeader header;
    /** 延迟构造的字段解析结果；仅在首次字段访问时填充。 */
    private RecordFieldResolver.Resolved resolved;

    /**
     * 创建 {@code RecordCursor}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param recordOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** 列是否 NULL。
     *
     * @param id 参与 {@code isNull} 的稳定领域标识 {@code ColumnId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code isNull} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean isNull(ColumnId id) {
        return resolved().isNull(id.value());
    }

    /** 列的编码切片（NULL 列抛 RecordFormatException）。供比较器按保序字节比较，避免构造 ColumnValue。
     *
     * @param id 参与 {@code columnSlice} 的稳定领域标识 {@code ColumnId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code columnSlice} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public FieldSlice columnSlice(ColumnId id) {
        return resolved().slice(id.value());
    }

    /** 列的逻辑值。
     *
     * @param id 参与 {@code readColumn} 的稳定领域标识 {@code ColumnId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code readColumn} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public ColumnValue readColumn(ColumnId id) {
        return resolved().value(id.value());
    }

    /** 读 key part 对应列值。
     *
     * @param part 记录格式使用的 header、隐藏列或键布局；不得为 {@code null}，偏移、字段顺序和编码宽度必须与当前页格式一致
     * @return {@code readKeyPart} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public ColumnValue readKeyPart(KeyPartDef part) {
        return readColumn(part.columnId());
    }

    /** 物化为完整逻辑记录（聚簇记录含隐藏列，隐藏列不混入 columnValues）。 */
    public LogicalRecord materialize() {
        return resolved().materialize();
    }

    /** 聚簇记录的 DB_TRX_ID；非聚簇记录无隐藏列，调用前应按 schema.clustered() 预判，否则抛 {@link RecordFormatException}。 */
    public TransactionId dbTrxId() {
        return requireHidden().dbTrxId();
    }

    /** 聚簇记录的 DB_ROLL_PTR；非聚簇记录无隐藏列时抛 {@link RecordFormatException}。 */
    public RollPointer dbRollPtr() {
        return requireHidden().dbRollPtr();
    }

    private HiddenColumns requireHidden() {
        HiddenColumns h = resolved().hiddenColumns();
        if (h == null) {
            throw new RecordFormatException("record has no hidden columns (non-clustered)");
        }
        return h;
    }

    /** 产出稳定定位值（pageOffset 为本游标记录的页内偏移）。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param indexId 参与 {@code recordRef} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code recordRef} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public RecordRef recordRef(PageId pageId, long indexId) {
        return new RecordRef(pageId, header.heapNo(), recordOffset, schema.schemaVersion(), indexId);
    }

    /** 释放游标（不持 latch/fix，空操作；保留以贴合 §14.2 接口语义）。 */
    public void release() {
        // cursor 不拥有锁与 buffer fix，无需释放；记录字节已是本地副本。
    }
}
