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

    private final TypeCodecRegistry registry;

    public RecordEncoder(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 编码：校验 → 规划 NULL 位图/变长目录/各区长度 → 写头、位图、目录、定长区、变长区。
     */
    public byte[] encode(LogicalRecord record, TableSchema schema) {
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
                if (ct.storageKind() == StorageKind.VARIABLE) {
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
            if (ct.storageKind() == StorageKind.VARIABLE) {
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
        return buf;
    }

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
