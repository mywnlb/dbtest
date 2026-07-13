package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.StorageKind;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * 物理记录字节按 schema 解析为「列级视图」（innodb-record-design §6 FieldOffsetResolver）。它是 {@link RecordDecoder}
 * 与 {@code RecordCursor} 共享的**单一布局真相**：算出每列的 NULL 标志与 {@link FieldSlice}，支持按需取单列、取整条。
 *
 * <p>布局同 §5/§6：{@code [header][nullbitmap][vardir][fixed][var]}。NULL 列不占 fixed/var/dir 空间。
 * 解析只在调用时进行，不缓存页引用（FieldSlice 的 backing 是传入的记录字节副本，生命周期由调用方掌握）。
 */
public final class RecordFieldResolver {

    private final TypeCodecRegistry registry;

    public RecordFieldResolver(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 解析整条记录字节。校验 recordLength 与缓冲区一致，再据 NULL 位图 + 变长目录 + 定长宽度推导各列切片。
     *
     * @param recordBytes 完整一条记录的字节（从 RecordHeader 起，长度 = header.recordLength）。
     * @param schema      权威列序与类型来源。
     */
    public Resolved resolve(byte[] recordBytes, TableSchema schema) {
        if (recordBytes == null || schema == null) {
            throw new DatabaseValidationException("recordBytes/schema must not be null");
        }
        RecordHeader header = RecordHeader.readFrom(recordBytes, 0);
        if (header.recordLength() != recordBytes.length) {
            throw new RecordFormatException("record length " + header.recordLength()
                    + " != buffer " + recordBytes.length);
        }
        int n = schema.columnCount();
        int nullableCount = RecordEncoder.countNullable(schema);

        int off = RecordHeaderLayout.SIZE;
        NullBitmap nullBitmap = NullBitmap.readFrom(recordBytes, off, nullableCount);
        off += nullBitmap.byteLength();

        boolean[] isNull = new boolean[n];
        int nullableIdx = 0;
        for (int i = 0; i < n; i++) {
            if (schema.column(i).type().nullable()) {
                isNull[i] = nullBitmap.get(nullableIdx);
                nullableIdx++;
            }
        }

        int activeVarCount = 0;
        int fixedAreaLen = 0;
        for (int i = 0; i < n; i++) {
            ColumnType ct = schema.column(i).type();
            if (isNull[i]) {
                continue;
            }
            if (ct.storageKind() != StorageKind.FIXED) {
                activeVarCount++;
            } else {
                fixedAreaLen += registry.codecFor(ct).fixedWidth(ct);
            }
        }

        VarLenDirectory dir = VarLenDirectory.readFrom(recordBytes, off, activeVarCount);
        off += dir.byteLength();
        int fixedOff = off;
        int varOff = off + fixedAreaLen;

        FieldSlice[] slices = new FieldSlice[n];
        int dirIdx = 0;
        for (int i = 0; i < n; i++) {
            ColumnType ct = schema.column(i).type();
            if (isNull[i]) {
                continue;
            }
            if (ct.storageKind() != StorageKind.FIXED) {
                int len = dir.length(dirIdx++);
                slices[i] = new FieldSlice(recordBytes, varOff, len);
                varOff += len;
            } else {
                int w = registry.codecFor(ct).fixedWidth(ct);
                slices[i] = new FieldSlice(recordBytes, fixedOff, w);
                fixedOff += w;
            }
        }
        // 循环结束后 varOff 指向变长区末尾 = 用户字段区末尾。聚簇记录的 15B 隐藏区紧贴其后；
        // 校验隐藏区正好在尾部（聚簇恰多 15B、非聚簇不得有尾随），否则判记录损坏，不静默接受错位。
        int userEnd = varOff;
        HiddenColumns hidden = null;
        if (schema.clustered()) {
            if (header.recordLength() != userEnd + HiddenColumnLayout.HIDDEN_BYTES) {
                throw new RecordFormatException("clustered record tail mismatch: recordLength="
                        + header.recordLength() + " userEnd=" + userEnd);
            }
            int hOff = header.recordLength() - HiddenColumnLayout.HIDDEN_BYTES;
            hidden = new HiddenColumns(HiddenColumnLayout.decodeTrxId(recordBytes, hOff),
                    HiddenColumnLayout.decodeRollPtr(recordBytes, hOff));
        } else if (header.recordLength() != userEnd) {
            throw new RecordFormatException("non-clustered record has trailing bytes: recordLength="
                    + header.recordLength() + " userEnd=" + userEnd);
        }
        return new Resolved(schema, header, isNull, slices, registry, hidden);
    }

    /**
     * 解析结果：可按列序取 NULL/切片/值，或整条物化。{@code slices[i]} 对 NULL 列为 null。
     */
    public static final class Resolved {

        private final TableSchema schema;
        private final RecordHeader header;
        private final boolean[] isNull;
        private final FieldSlice[] slices;
        private final TypeCodecRegistry registry;
        /** 聚簇记录隐藏列（DB_TRX_ID + DB_ROLL_PTR）；非聚簇为 null。 */
        private final HiddenColumns hidden;

        Resolved(TableSchema schema, RecordHeader header, boolean[] isNull, FieldSlice[] slices,
                 TypeCodecRegistry registry, HiddenColumns hidden) {
            this.schema = schema;
            this.header = header;
            this.isNull = isNull;
            this.slices = slices;
            this.registry = registry;
            this.hidden = hidden;
        }

        /** 记录头。 */
        public RecordHeader header() {
            return header;
        }

        /** 聚簇记录隐藏列；非聚簇记录返回 null。 */
        public HiddenColumns hiddenColumns() {
            return hidden;
        }

        /** 第 ordinal 列是否 NULL。 */
        public boolean isNull(int ordinal) {
            check(ordinal);
            return isNull[ordinal];
        }

        /** 第 ordinal 列的编码切片；NULL 列无切片，抛 {@link RecordFormatException}。 */
        public FieldSlice slice(int ordinal) {
            check(ordinal);
            if (isNull[ordinal]) {
                throw new RecordFormatException("column " + ordinal + " is NULL, no field slice");
            }
            return slices[ordinal];
        }

        /** 第 ordinal 列的逻辑值（NULL→NullValue，否则按列类型 codec 解码）。 */
        public ColumnValue value(int ordinal) {
            check(ordinal);
            if (isNull[ordinal]) {
                return ColumnValue.NullValue.INSTANCE;
            }
            ColumnType ct = schema.column(ordinal).type();
            return registry.codecFor(ct).decode(slices[ordinal], ct);
        }

        /** 物化为逻辑记录（用户列 + 头的 deleted/recordType + 隐藏列；隐藏列不混入 columnValues）。 */
        public LogicalRecord materialize() {
            List<ColumnValue> values = new ArrayList<>(schema.columnCount());
            for (int i = 0; i < schema.columnCount(); i++) {
                values.add(value(i));
            }
            return new LogicalRecord(schema.schemaVersion(), values, header.deletedFlag(),
                    header.recordType(), hidden);
        }

        private void check(int ordinal) {
            if (ordinal < 0 || ordinal >= schema.columnCount()) {
                throw new DatabaseValidationException("column ordinal out of range: " + ordinal);
            }
        }
    }
}
