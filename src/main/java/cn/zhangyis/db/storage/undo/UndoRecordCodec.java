package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.FieldWriter;
import cn.zhangyis.db.storage.record.type.TypeCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * undo record 编解码（设计 §5.5/§6.5）。布局（big-endian），公共前缀 + 类型相关尾部：
 * <pre>
 * 公共：[type u8][undoNo u64][transactionId u64][tableId u64][indexId u64][prevRollPointer 7B][keyColCount u8]
 *       后跟每个 key 列：[nullFlag u8]（非 null 再 [len u16][bytes]）
 * UPDATE_ROW 追加尾部：[oldDbTrxId u64][oldDbRollPtr 7B][rowColCount u8] 后跟每个 row 列（同 framing，按 schema 全列序）
 * </pre>
 * INSERT_ROW 无尾部。{@code type} 首字节与 v2 普通 UNDO 页复制的 {@link UndoLogKind} 共同守门：INSERT log 仅容纳
 * INSERT_ROW，UPDATE log 仅容纳 UPDATE_ROW/DELETE_MARK。
 *
 * <p><b>为什么自带 framing</b>：undo record 无 record 页的 NullBitmap/变长目录，而 {@link TypeCodec} 约定 NULL 不由
 * codec 处理、变长 codec 不自带长度。故本 codec 为每列写 nullFlag + 显式长度。<b>为什么需要 TableSchema</b>：
 * {@link IndexKeyDef} 只含 ColumnId，不含 ColumnType；类型由 {@code schema.column(columnId).type()} 解析
 * （columnId==ordinal==列序，由 TableSchema 不变量保证）。
 */
public final class UndoRecordCodec {

    private final TypeCodecRegistry registry;

    public UndoRecordCodec(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("undo record codec registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 编码一条 INSERT undo record。数据流：写定长前缀（type/undoNo/事务/表/索引/prev 指针）→ 写 keyColCount →
     * 逐列按 schema 类型自带 framing 编码。非 INSERT_ROW 或 key 列数与 keyDef 不符抛 {@link DatabaseValidationException}。
     */
    public byte[] encode(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (rec == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo encode args must not be null");
        }
        List<KeyPartDef> parts = keyDef.parts();
        if (rec.clusterKey().size() != parts.size()) {
            throw new DatabaseValidationException("clusterKey size " + rec.clusterKey().size()
                    + " != key parts " + parts.size());
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(rec.type().code());
        writeU64(out, rec.undoNo().value());
        writeU64(out, rec.transactionId().value());
        writeU64(out, rec.tableId());
        writeU64(out, rec.indexId());
        out.writeBytes(rec.prevRollPointer().encode());
        out.write(parts.size());
        for (int i = 0; i < parts.size(); i++) {
            ColumnValue v = rec.clusterKey().get(i);
            ColumnType ct = schema.column(parts.get(i).columnId().value()).type();
            writeFramedColumn(out, v, ct);
        }
        // UPDATE_ROW / DELETE_MARK 尾部：旧隐藏列（版本链上一版本指针）+ 全量旧 image（按 schema 全列序）。INSERT_ROW 无此段。
        if (rec.type() == UndoRecordType.UPDATE_ROW || rec.type() == UndoRecordType.DELETE_MARK) {
            writeU64(out, rec.oldHiddenColumns().dbTrxId().value());
            out.writeBytes(rec.oldHiddenColumns().dbRollPtr().encode());
            List<ColumnValue> row = rec.oldColumnValues();
            out.write(row.size());
            for (int i = 0; i < row.size(); i++) {
                writeFramedColumn(out, row.get(i), schema.column(i).type());
            }
        }
        return out.toByteArray();
    }

    /** 写一列自带 framing：NULL→[1]；非 NULL→[0][len u16][bytes]。供 key 列与 UPDATE 旧 image 全列共用。 */
    private void writeFramedColumn(ByteArrayOutputStream out, ColumnValue v, ColumnType ct) {
        if (v == ColumnValue.NullValue.INSTANCE) {
            out.write(1);
            return;
        }
        out.write(0);
        TypeCodec codec = registry.codecFor(ct);
        int len = codec.encodedLength(v, ct);
        byte[] colBuf = new byte[len];
        codec.encode(v, ct, new FieldWriter(colBuf, 0));
        writeU16(out, len);
        out.writeBytes(colBuf);
    }

    /**
     * 解码一条 INSERT undo record（从 {@code off} 起）。任何越界/字段不符/落盘类型非 INSERT_ROW 抛
     * {@link UndoLogFormatException}（物理损坏，不静默跳过）。
     */
    public UndoRecord decode(byte[] buf, int off, IndexKeyDef keyDef, TableSchema schema) {
        if (buf == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo decode args must not be null");
        }
        int[] c = {off};
        int typeCode = readU8(buf, c);
        if (typeCode != UndoRecordType.INSERT_ROW.code() && typeCode != UndoRecordType.UPDATE_ROW.code()
                && typeCode != UndoRecordType.DELETE_MARK.code()) {
            throw new UndoLogFormatException("unknown/unsupported undo record type on disk: " + typeCode);
        }
        long undoNo = readU64(buf, c);
        long txn = readU64(buf, c);
        long tableId = readU64(buf, c);
        long indexId = readU64(buf, c);
        if (c[0] + RollPointer.BYTES > buf.length) {
            throw new UndoLogFormatException("undo record truncated (roll pointer)");
        }
        RollPointer prev = RollPointer.decode(buf, c[0]);
        c[0] += RollPointer.BYTES;
        // 公共 key 段（INSERT/UPDATE 一致）：keyColCount + 每列 framing（按 keyDef part 的列类型）。
        int keyColCount = readU8(buf, c);
        List<KeyPartDef> parts = keyDef.parts();
        if (keyColCount != parts.size()) {
            throw new UndoLogFormatException("keyColCount " + keyColCount + " != key parts " + parts.size());
        }
        List<ColumnValue> key = new ArrayList<>(keyColCount);
        for (int i = 0; i < keyColCount; i++) {
            key.add(readFramedColumn(buf, c, schema.column(parts.get(i).columnId().value()).type(), "key col " + i));
        }
        if (typeCode == UndoRecordType.INSERT_ROW.code()) {
            requireFullyConsumed(buf, c[0]);
            return UndoRecord.insert(UndoNo.of(undoNo), TransactionId.of(txn), tableId, indexId, key, prev);
        }
        // UPDATE_ROW / DELETE_MARK 尾部：旧隐藏列 + 全量旧 image（按 schema 全列序）。
        long oldTrx = readU64(buf, c);
        if (c[0] + RollPointer.BYTES > buf.length) {
            throw new UndoLogFormatException("undo record truncated (old roll pointer)");
        }
        RollPointer oldRollPtr = RollPointer.decode(buf, c[0]);
        c[0] += RollPointer.BYTES;
        HiddenColumns oldHidden = new HiddenColumns(TransactionId.of(oldTrx), oldRollPtr);
        int rowColCount = readU8(buf, c);
        if (rowColCount != schema.columnCount()) {
            throw new UndoLogFormatException("old-image rowColCount " + rowColCount
                    + " != schema columns " + schema.columnCount());
        }
        List<ColumnValue> oldRow = new ArrayList<>(rowColCount);
        for (int i = 0; i < rowColCount; i++) {
            oldRow.add(readFramedColumn(buf, c, schema.column(i).type(), "old row col " + i));
        }
        requireFullyConsumed(buf, c[0]);
        if (typeCode == UndoRecordType.UPDATE_ROW.code()) {
            return UndoRecord.update(UndoNo.of(undoNo), TransactionId.of(txn), tableId, indexId, key,
                    oldRow, oldHidden, prev);
        }
        return UndoRecord.deleteMark(UndoNo.of(undoNo), TransactionId.of(txn), tableId, indexId, key,
                oldRow, oldHidden, prev);
    }

    /** record 槽长度是物理 framing 的权威边界；尾随字节不能被静默忽略，否则损坏 descriptor 可藏在合法前缀后。 */
    private static void requireFullyConsumed(byte[] buf, int cursor) {
        if (cursor != buf.length) {
            throw new UndoLogFormatException("undo record has trailing bytes: consumed=" + cursor
                    + " length=" + buf.length);
        }
    }

    /** 读一列自带 framing：nullFlag==1→NULL；否则 [len u16][bytes] 按类型解码。截断抛 {@link UndoLogFormatException}。 */
    private ColumnValue readFramedColumn(byte[] buf, int[] c, ColumnType ct, String what) {
        int nullFlag = readU8(buf, c);
        if (nullFlag == 1) {
            return ColumnValue.NullValue.INSTANCE;
        }
        int len = readU16(buf, c);
        if (c[0] + len > buf.length) {
            throw new UndoLogFormatException("undo record truncated (" + what + ")");
        }
        ColumnValue v = registry.codecFor(ct).decode(new FieldSlice(buf, c[0], len), ct);
        c[0] += len;
        return v;
    }

    private static void writeU16(ByteArrayOutputStream out, int v) {
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void writeU64(ByteArrayOutputStream out, long v) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) ((v >>> shift) & 0xFF));
        }
    }

    private static int readU8(byte[] b, int[] c) {
        if (c[0] + 1 > b.length) {
            throw new UndoLogFormatException("undo record truncated (u8)");
        }
        return b[c[0]++] & 0xFF;
    }

    private static int readU16(byte[] b, int[] c) {
        if (c[0] + 2 > b.length) {
            throw new UndoLogFormatException("undo record truncated (u16)");
        }
        int v = ((b[c[0]] & 0xFF) << 8) | (b[c[0] + 1] & 0xFF);
        c[0] += 2;
        return v;
    }

    private static long readU64(byte[] b, int[] c) {
        if (c[0] + 8 > b.length) {
            throw new UndoLogFormatException("undo record truncated (u64)");
        }
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[c[0] + i] & 0xFFL);
        }
        c[0] += 8;
        return v;
    }
}
