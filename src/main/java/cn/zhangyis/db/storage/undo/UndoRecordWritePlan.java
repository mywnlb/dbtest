package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * 一条 UndoRecord 的不可变物理编码计划。它在写 MTR admission 前冻结完整编码、inline/external 选择、payload 页数和
 * CRC；执行阶段只补充分配出的首页号，避免开始写页后再发生编码、算术或配置错误。
 */
public final class UndoRecordWritePlan {

    /** 完整逻辑记录；领域对象在构造时已经复制其列表字段。 */
    private final UndoRecord record;
    /** 解码该记录所需的稳定 key/schema。 */
    private final IndexKeyDef keyDef;
    /**
     * 本对象持有的 {@code schema} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final TableSchema schema;
    /** 完整 UndoRecord 编码（包含非空 INSERT LOB ownership 尾部）；不向跨包调用者暴露可变引用。 */
    private final byte[] encodedPayload;
    /** 是否使用 external descriptor。 */
    private final boolean external;
    /** external 页数；inline 为 0。 */
    private final int externalPageCount;
    /** 完整编码 CRC32；inline 也预计算，便于计划保持单一构造路径。 */
    private final long crc32;

    /**
     * 创建 {@code UndoRecordWritePlan}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param encodedPayload 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param external 值的编码或展示形态；为 {@code true} 时按名称所示文本、JSON 或外部引用格式处理
     * @param externalPageCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param crc32 参与 {@code 构造} 的位域或校验值 {@code crc32}；只允许当前格式定义的位，数值按无符号位模式解释
     */
    private UndoRecordWritePlan(UndoRecord record, IndexKeyDef keyDef, TableSchema schema,
                                byte[] encodedPayload, boolean external, int externalPageCount, long crc32) {
        this.record = record;
        this.keyDef = keyDef;
        this.schema = schema;
        this.encodedPayload = Arrays.copyOf(encodedPayload, encodedPayload.length);
        this.external = external;
        this.externalPageCount = externalPageCount;
        this.crc32 = crc32;
    }

    /** 在任何写 MTR/页分配前形成物理计划，并执行单条 external 页数上限。
     *
     * @param codec 由组合根提供的 {@code UndoRecordCodec} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code create} 调用
     * @param pageSize 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param maxExternalPages 参与 {@code create} 的上界或规格值 {@code maxExternalPages}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return {@code create} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoPayloadTooLargeException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    static UndoRecordWritePlan create(UndoRecordCodec codec, PageSize pageSize, UndoRecord record,
                                      IndexKeyDef keyDef, TableSchema schema, int maxExternalPages) {
        if (codec == null || pageSize == null || record == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo record write plan args must not be null");
        }
        if (maxExternalPages <= 0) {
            throw new DatabaseValidationException("max external undo payload pages must be positive: "
                    + maxExternalPages);
        }
        byte[] encoded = codec.encode(record, keyDef, schema);
        int freshInlineCapacity = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES
                - UndoPageLayout.RECORD_AREA_START - Short.BYTES;
        boolean external = encoded.length > freshInlineCapacity;
        int pageCount = external ? pageCount(encoded.length, UndoPayloadPageLayout.payloadCapacity(pageSize)) : 0;
        if (pageCount > maxExternalPages) {
            throw new UndoPayloadTooLargeException("external undo payload requires " + pageCount
                    + " pages, configured maximum is " + maxExternalPages);
        }
        CRC32 checksum = new CRC32();
        checksum.update(encoded);
        return new UndoRecordWritePlan(record, keyDef, schema, encoded, external, pageCount, checksum.getValue());
    }

    /** 返回已冻结的逻辑 undo record。 */
    public UndoRecord record() {
        return record;
    }

    /** 是否需要把完整编码写入独立 payload 页链。 */
    public boolean external() {
        return external;
    }

    /** external 页链精确页数；inline 模式为 0。 */
    public int externalPageCount() {
        return externalPageCount;
    }

    /** 普通 UNDO 页 record 槽中实际保存的 payload 长度。 */
    public int rootPayloadLength() {
        return external ? UndoPayloadDescriptor.BYTES : encodedPayload.length;
    }

    /** 返回完整逻辑 UndoRecord 编码长度，用于 deferred placeholder 与实际引用做定长证明。 */
    public int encodedPayloadLength() {
        return encodedPayload.length;
    }

    /**
     * 判断另一个计划是否可复用本计划已经固定的 root/payload 页布局；内容与 CRC 可以因 LOB 首页号变化，
     * 但完整长度、inline/external 分支、external 页数与 root descriptor 长度必须完全一致。
     *
     * @param other 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code samePhysicalShape} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean samePhysicalShape(UndoRecordWritePlan other) {
        return other != null
                && encodedPayload.length == other.encodedPayload.length
                && external == other.external
                && externalPageCount == other.externalPageCount
                && rootPayloadLength() == other.rootPayloadLength();
    }

    /** 跨包只提供防御性副本，避免计划形成后被调用方改写。 */
    public byte[] encodedPayload() {
        return Arrays.copyOf(encodedPayload, encodedPayload.length);
    }

    IndexKeyDef keyDef() {
        return keyDef;
    }

    TableSchema schema() {
        return schema;
    }

    byte[] encodedPayloadUnsafe() {
        return encodedPayload;
    }

    long crc32() {
        return crc32;
    }

    private static int pageCount(int length, int capacity) {
        return (int) (((long) length + capacity - 1L) / capacity);
    }
}
