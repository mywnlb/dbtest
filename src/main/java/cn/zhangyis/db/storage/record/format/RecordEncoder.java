package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.StorageKind;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldWriter;
import cn.zhangyis.db.storage.record.type.TypeCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * 逻辑记录 → 物理记录字节（innodb-record-design §6 反向编码）。布局：
 * [RecordHeader][NullBitmap][VarLenDirectory][FixedArea][VariableArea]。NULL 列不占 fixed/var/dir 空间。
 */
public final class RecordEncoder {

    /** recordLength 为 u16，故记录上限 65535（inline 上限近似，overflow 未实现）。 */
    public static final int MAX_RECORD_LENGTH = 0xFFFF;

    /**
     * 本对象持有的 {@code registry} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TypeCodecRegistry registry;

    /**
     * 创建 {@code RecordEncoder}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecordEncoder(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 编码：校验 → 规划 NULL 位图/变长目录/各区长度 → 写头、位图、目录、定长区、变长区。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code encode} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws SchemaVersionMismatchException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws RecordFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws RecordTooLargeException 输入值或资源需求超出编码、页面或容量上限时抛出；调用方应缩小请求、回滚或等待资源释放
     */
    public byte[] encode(LogicalRecord record, TableSchema schema) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (record == null || schema == null) {
            throw new DatabaseValidationException("record/schema must not be null");
        }
        if (record.schemaVersion() != schema.schemaVersion()) {
            throw new SchemaVersionMismatchException("record schemaVersion " + record.schemaVersion()
                    + " != schema " + schema.schemaVersion());
        }
        int n = schema.columnCount();
        if (record.columnValues().size() != n) {
            throw new RecordFormatException("column count mismatch: record " + record.columnValues().size()
                    + " vs schema " + n);
        }
        // clustered ⇔ hiddenColumns 在场：聚簇 schema 必须带隐藏列，非聚簇不得带（LogicalRecord 不持 schema，
        // 故一致性在此校验）。违反则记录会写出/缺失尾部 15B 隐藏区，破坏 decoder 的尾部长度不变量。
        if (schema.clustered() != (record.hiddenColumns() != null)) {
            throw new DatabaseValidationException("clustered=" + schema.clustered()
                    + " but hiddenColumns " + (record.hiddenColumns() == null ? "absent" : "present"));
        }

        int nullableCount = countNullable(schema);
        NullBitmap nullBitmap = new NullBitmap(nullableCount);
        List<Integer> varLengths = new ArrayList<>();
        int fixedAreaLen = 0;
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        int varAreaLen = 0;
        int nullableIdx = 0;

        for (int i = 0; i < n; i++) {
            ColumnType ct = schema.column(i).type();
            ColumnValue v = record.columnValues().get(i);
            boolean isNull = v instanceof ColumnValue.NullValue;
            if (isNull && !ct.nullable()) {
                throw new RecordFormatException("null value for non-nullable column: " + schema.column(i).name());
            }
            if (ct.nullable()) {
                if (isNull) {
                    nullBitmap.set(nullableIdx);
                }
                nullableIdx++;
            }
            if (!isNull) {
                registry.validate(v, ct);
                TypeCodec codec = registry.codecFor(ct);
                if (ct.storageKind() != StorageKind.FIXED) {
                    int len = codec.encodedLength(v, ct);
                    varLengths.add(len);
                    varAreaLen += len;
                } else {
                    fixedAreaLen += codec.fixedWidth(ct);
                }
            }
        }

        int[] dirLengths = new int[varLengths.size()];
        for (int i = 0; i < dirLengths.length; i++) {
            dirLengths[i] = varLengths.get(i);
        }
        VarLenDirectory dir = new VarLenDirectory(dirLengths);

        int recordLength = RecordHeaderLayout.SIZE + nullBitmap.byteLength() + dir.byteLength()
                + fixedAreaLen + varAreaLen;
        // 聚簇记录在用户字段区之后追加 15B 隐藏区（DB_TRX_ID + DB_ROLL_PTR），计入 recordLength，
        // 使 header.recordLength 含隐藏区、resolver 的「长度==buffer」校验成立。
        if (schema.clustered()) {
            recordLength += HiddenColumnLayout.HIDDEN_BYTES;
        }
        if (recordLength > MAX_RECORD_LENGTH) {
            throw new RecordTooLargeException("record length " + recordLength + " exceeds " + MAX_RECORD_LENGTH);
        }

        byte[] buf = new byte[recordLength];
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        new RecordHeader(record.deleted(), false, record.recordType(), 0, 0, 0, recordLength).writeTo(buf, 0);
        int off = RecordHeaderLayout.SIZE;
        nullBitmap.writeTo(buf, off);
        off += nullBitmap.byteLength();
        dir.writeTo(buf, off);
        off += dir.byteLength();

        int fixedOff = off;
        int varOff = off + fixedAreaLen;
        for (int i = 0; i < n; i++) {
            ColumnType ct = schema.column(i).type();
            ColumnValue v = record.columnValues().get(i);
            if (v instanceof ColumnValue.NullValue) {
                continue;
            }
            TypeCodec codec = registry.codecFor(ct);
            if (ct.storageKind() != StorageKind.FIXED) {
                int len = codec.encodedLength(v, ct);
                codec.encode(v, ct, new FieldWriter(buf, varOff));
                varOff += len;
            } else {
                codec.encode(v, ct, new FieldWriter(buf, fixedOff));
                fixedOff += codec.fixedWidth(ct);
            }
        }
        // 隐藏区贴在记录尾部（recordLength-15 起），与 RecordFieldResolver 的尾部解析对称。
        if (schema.clustered()) {
            HiddenColumns hc = record.hiddenColumns();
            HiddenColumnLayout.encode(buf, recordLength - HiddenColumnLayout.HIDDEN_BYTES,
                    hc.dbTrxId(), hc.dbRollPtr());
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return buf;
    }

    /**
     * 计算 {@code countNullable} 所表达的记录格式与页内组织数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code countNullable} 计算出的非负长度、位置或数量；结果必须落在所属页、集合或持久格式容量内，溢出通过领域异常报告
     */
    static int countNullable(TableSchema schema) {
        int c = 0;
        for (int i = 0; i < schema.columnCount(); i++) {
            if (schema.column(i).type().nullable()) {
                c++;
            }
        }
        return c;
    }
}
