package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.schema.StorageKind;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.FieldWriter;
import cn.zhangyis.db.storage.record.type.TypeCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * undo record 编解码（设计 §5.5/§6.5）。布局（big-endian），公共前缀 + 类型相关尾部：
 * <pre>
 * 公共：[type u8][undoNo u64][transactionId u64][tableId u64][indexId u64][prevRollPointer 7B][keyColCount u8]
 *       后跟每个 key 列：[nullFlag u8]（非 null 再 [len u16][bytes]）
 * UPDATE_ROW 追加尾部：[oldDbTrxId u64][oldDbRollPtr 7B][rowColCount u8] 后跟每个 row 列（同 framing，按 schema 全列序）
 * UPDATE/DELETE 可选 LV 尾部：[magic "LV" u16][version u8][count u16]，逐项保存 ordinal/flags/可选新 external。
 * 可选 secondary 尾部：[magic "SI" u16][version u8][count u16]，每项 [indexId u64][action u8][beforeState u8]
 * </pre>
 * INSERT_ROW 可先带既有 LOB ownership 尾部，再带 secondary 尾部；两者都为空时编码与旧版本逐字节一致。
 * {@code type} 首字节与 v2 普通 UNDO 页复制的 {@link UndoLogKind} 共同守门：INSERT log 仅容纳
 * INSERT_ROW，UPDATE log 仅容纳 UPDATE_ROW/DELETE_MARK。
 *
 * <p><b>为什么自带 framing</b>：undo record 无 record 页的 NullBitmap/变长目录，而 {@link TypeCodec} 约定 NULL 不由
 * codec 处理、变长 codec 不自带长度。故本 codec 为每列写 nullFlag + 显式长度。<b>为什么需要 TableSchema</b>：
 * {@link IndexKeyDef} 只含 ColumnId，不含 ColumnType；类型由 {@code schema.column(columnId).type()} 解析
 * （columnId==ordinal==列序，由 TableSchema 不变量保证）。
 */
public final class UndoRecordCodec {

    /** INSERT LOB ownership 尾部 magic：ASCII "LO"；旧记录因 EOF 不含该值。 */
    private static final int INSERT_LOB_TAIL_MAGIC = 0x4C4F;

    /** ownership 尾部首版；未来扩展必须追加版本并保留 v1 decoder，不能静默重解释。 */
    private static final int INSERT_LOB_TAIL_VERSION = 1;

    /** UPDATE/DELETE LOB version ownership 尾部 magic：ASCII "LV"。 */
    private static final int LOB_VERSION_TAIL_MAGIC = 0x4C56;

    /** LOB version ownership 首版。 */
    private static final int LOB_VERSION_TAIL_VERSION = 1;

    /** committed purge 释放 old image external chain。 */
    private static final int LOB_VERSION_PURGE_OLD = 0x01;

    /** rollback marker 释放前向 UPDATE 新建的 external chain。 */
    private static final int LOB_VERSION_ROLLBACK_NEW = 0x02;

    /** secondary mutation 尾部 magic：ASCII "SI"；与 INSERT 的 "LO" 区分，便于旧 EOF 与双尾顺序判定。 */
    private static final int SECONDARY_TAIL_MAGIC = 0x5349;

    /** secondary mutation 尾部首版；未知版本必须 fail-closed，不能按 v1 猜测恢复动作。 */
    private static final int SECONDARY_TAIL_VERSION = 1;

    /** key/旧 image 列值编码使用的稳定类型 codec 注册表；构造后只读，可跨线程共享。 */
    private final TypeCodecRegistry registry;

    /**
     * 创建 undo record codec。
     *
     * @param registry 必须与 record/B+Tree 使用相同稳定类型定义的 codec 注册表。
     * @throws DatabaseValidationException 注册表为空时抛出，防止编码/解码类型协议无法确定。
     */
    public UndoRecordCodec(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("undo record codec registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 按稳定 big-endian 协议编码一条 INSERT/UPDATE/DELETE undo record；空可选 tail 保持旧编码逐字节兼容。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 record、key definition、schema 和聚簇 key part 数，任何错误在创建输出前失败。</li>
     *     <li>写固定 identity/链前缀，再按 key part 对应列类型写带 null/length framing 的完整聚簇键。</li>
     *     <li>INSERT 写可选 LO；UPDATE/DELETE 写旧隐藏列、全量旧 image 和可选 LV ownership。</li>
     *     <li>最后写可选 secondary mutation tail，并返回独立字节数组；尾部顺序固定，不能按调用方列表动态互换。</li>
     * </ol>
     *
     * @param rec    领域约束已由 {@link UndoRecord} 校验的逻辑 undo record。
     * @param keyDef exact-version 聚簇主键定义，用于解释 {@code clusterKey} 每个 part 的 source column。
     * @param schema exact-version 完整聚簇表 schema，用于类型编码主键和旧 image。
     * @return 从 offset 0 开始、无额外尾随空间的稳定磁盘协议字节。
     * @throws DatabaseValidationException 参数缺失、key part 数/类型、LOB column 或可选 tail count 无效时抛出。
     */
    public byte[] encode(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        // 1. 形状校验先于 ByteArrayOutputStream 创建，失败不会产生可被误写盘的部分编码。
        if (rec == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo encode args must not be null");
        }
        List<KeyPartDef> parts = keyDef.parts();
        if (rec.clusterKey().size() != parts.size()) {
            throw new DatabaseValidationException("clusterKey size " + rec.clusterKey().size()
                    + " != key parts " + parts.size());
        }
        // 2. 公共前缀和聚簇键是 identity/peek 的稳定基础，所有类型共享完全相同的 framing。
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
        // 3. 类型专用 payload：INSERT 的 LOB tail 必须先于 secondary；UPDATE/DELETE 固定保存旧版本。
        if (rec.type() == UndoRecordType.INSERT_ROW && !rec.insertedLobs().isEmpty()) {
            writeInsertedLobTail(out, rec.insertedLobs(), schema);
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
            if (!rec.lobVersionOwnerships().isEmpty()) {
                writeLobVersionTail(out, rec.lobVersionOwnerships(), rec.oldColumnValues(), schema);
            }
        }
        // 4. secondary tail 始终位于记录末尾；空列表不写 magic，旧 decoder/旧记录都以 EOF 保持兼容。
        if (!rec.secondaryMutations().isEmpty()) {
            writeSecondaryTail(out, rec.secondaryMutations());
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
     * 从指定偏移解码完整 INSERT/UPDATE/DELETE undo record。任何截断、未知类型/tail 或 schema 形状错配都按
     * 物理格式损坏 fail-closed，禁止跳过未知字节继续 recovery。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验容器参数，从固定前缀读取 type、undo/transaction/table/index identity 与局部链前驱。</li>
     *     <li>按 exact key definition/schema 解码带 framing 的完整聚簇键，并核对落盘 key part 数。</li>
     *     <li>INSERT 按固定顺序识别可选 LOB tail 与 secondary tail，要求消费到 EOF 后构造 INSERT_ROW record。</li>
     *     <li>UPDATE/DELETE 解码旧隐藏列和全量旧 image，列数必须等于 exact-version schema。</li>
     *     <li>依次解码可选 LV 与 SI tail，要求 ownership/action 与主 type 一致且消费到 EOF，再构造领域 record。</li>
     * </ol>
     *
     * @param buf    包含一条完整 undo record 的字节数组。
     * @param off    record 固定前缀起始偏移；必须位于数组有效范围。
     * @param keyDef exact-version 聚簇主键定义。
     * @param schema exact-version 完整聚簇表 schema。
     * @return 字段集合已防御性冻结、可直接供 rollback/MVCC 使用的 UndoRecord。
     * @throws DatabaseValidationException 参数容器缺失时抛出。
     * @throws UndoLogFormatException offset 越界、记录截断、type/magic/version/code 未知、字段数/类型/排序/action
     *                                不满足磁盘协议或存在尾随垃圾时抛出。
     */
    public UndoRecord decode(byte[] buf, int off, IndexKeyDef keyDef, TableSchema schema) {
        // 1. 先校验调用容器，再读取所有类型共享的固定 identity 与局部链前驱。
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
        // 2. 公共 key 段按 keyDef part 的 source column 类型解码，落盘 part 数必须精确一致。
        int keyColCount = readU8(buf, c);
        List<KeyPartDef> parts = keyDef.parts();
        if (keyColCount != parts.size()) {
            throw new UndoLogFormatException("keyColCount " + keyColCount + " != key parts " + parts.size());
        }
        List<ColumnValue> key = new ArrayList<>(keyColCount);
        for (int i = 0; i < keyColCount; i++) {
            key.add(readFramedColumn(buf, c, schema.column(parts.get(i).columnId().value()).type(), "key col " + i));
        }
        // 3. INSERT 兼容三种尾部形状：EOF、仅 LOB/secondary，以及固定 LOB -> secondary 双尾。
        if (typeCode == UndoRecordType.INSERT_ROW.code()) {
            List<InsertedLobOwnership> ownerships = List.of();
            List<SecondaryUndoMutation> mutations = List.of();
            if (c[0] < buf.length && peekU16(buf, c[0]) == INSERT_LOB_TAIL_MAGIC) {
                ownerships = readInsertedLobTail(buf, c, schema);
            }
            if (c[0] < buf.length) {
                mutations = readSecondaryTail(buf, c, UndoRecordType.INSERT_ROW);
            }
            requireFullyConsumed(buf, c[0]);
            return UndoRecord.insert(UndoNo.of(undoNo), TransactionId.of(txn), tableId, indexId, key,
                    ownerships, mutations, prev);
        }
        // 4. UPDATE/DELETE 必须携带旧隐藏列和 exact schema 全列旧 image，缺列/多列均为恢复歧义。
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
        // 5. UPDATE/DELETE 可先带 LV tail，再带 secondary tail；旧记录允许直接 EOF 或 SI。
        UndoRecordType recordType = UndoRecordType.fromCode(typeCode);
        List<LobVersionOwnership> lobVersions = List.of();
        if (c[0] < buf.length && peekU16(buf, c[0]) == LOB_VERSION_TAIL_MAGIC) {
            lobVersions = readLobVersionTail(buf, c, recordType, oldRow, schema);
        }
        List<SecondaryUndoMutation> mutations = c[0] == buf.length
                ? List.of() : readSecondaryTail(buf, c, recordType);
        requireFullyConsumed(buf, c[0]);
        if (typeCode == UndoRecordType.UPDATE_ROW.code()) {
            return UndoRecord.update(UndoNo.of(undoNo), TransactionId.of(txn), tableId, indexId, key,
                    oldRow, oldHidden, lobVersions, mutations, prev);
        }
        return UndoRecord.deleteMark(UndoNo.of(undoNo), TransactionId.of(txn), tableId, indexId, key,
                oldRow, oldHidden, lobVersions, mutations, prev);
    }

    /**
     * 只读取固定 33B 前缀中的 type/undoNo/trxId/tableId/indexId，不触碰依赖 schema 的 key/old-image。
     * 截断或未知类型按磁盘格式损坏处理，禁止调用方猜测默认索引继续解码。
     */
    public UndoRecordIdentity peekIdentity(byte[] buf, int off) {
        if (buf == null || off < 0) {
            throw new DatabaseValidationException("undo identity buffer/offset invalid");
        }
        int[] cursor = {off};
        int typeCode = readU8(buf, cursor);
        if (typeCode != UndoRecordType.INSERT_ROW.code() && typeCode != UndoRecordType.UPDATE_ROW.code()
                && typeCode != UndoRecordType.DELETE_MARK.code()) {
            throw new UndoLogFormatException("unknown undo record type in identity prefix: " + typeCode);
        }
        return new UndoRecordIdentity(UndoRecordType.fromCode(typeCode), UndoNo.of(readU64(buf, cursor)),
                TransactionId.of(readU64(buf, cursor)), readU64(buf, cursor), readU64(buf, cursor));
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

    /**
     * 编码 INSERT ownership 尾部。所有 schema/type 校验发生在 UndoRecordWritePlan 冻结、写 MTR admission 之前；
     * external bytes 复用列自己的 LobCodec envelope，避免形成第二套 LobReference 磁盘协议。
     */
    private void writeInsertedLobTail(ByteArrayOutputStream out, List<InsertedLobOwnership> ownerships,
                                      TableSchema schema) {
        if (ownerships.size() > 0xFFFF) {
            throw new DatabaseValidationException("too many inserted LOB ownership entries: " + ownerships.size());
        }
        writeU16(out, INSERT_LOB_TAIL_MAGIC);
        out.write(INSERT_LOB_TAIL_VERSION);
        writeU16(out, ownerships.size());
        for (InsertedLobOwnership ownership : ownerships) {
            int ordinal = ownership.columnOrdinal();
            if (ordinal > 0xFFFF) {
                throw new DatabaseValidationException("inserted LOB column ordinal exceeds u16: " + ordinal);
            }
            ColumnType columnType = schema.column(ordinal).type();
            requireLobColumnForEncode(ordinal, columnType);
            TypeCodec codec = registry.codecFor(columnType);
            int encodedLength = codec.encodedLength(ownership.value(), columnType);
            if (encodedLength <= 0 || encodedLength > 0xFFFF) {
                throw new DatabaseValidationException(
                        "inserted LOB external envelope length exceeds u16: " + encodedLength);
            }
            byte[] encoded = new byte[encodedLength];
            codec.encode(ownership.value(), columnType, new FieldWriter(encoded, 0));
            writeU16(out, ordinal);
            writeU16(out, encodedLength);
            out.writeBytes(encoded);
        }
    }

    /** 有任何剩余字节就必须完整命中 magic/version/count/entries；不存在可跳过的未知扩展。 */
    private List<InsertedLobOwnership> readInsertedLobTail(byte[] buf, int[] cursor, TableSchema schema) {
        int magic = readU16(buf, cursor);
        if (magic != INSERT_LOB_TAIL_MAGIC) {
            throw new UndoLogFormatException("unknown INSERT LOB ownership tail magic: " + magic);
        }
        int version = readU8(buf, cursor);
        if (version != INSERT_LOB_TAIL_VERSION) {
            throw new UndoLogFormatException("unknown INSERT LOB ownership tail version: " + version);
        }
        int count = readU16(buf, cursor);
        if (count == 0) {
            throw new UndoLogFormatException("INSERT LOB ownership tail must not encode an empty list");
        }
        List<InsertedLobOwnership> ownerships = new ArrayList<>(count);
        int previousOrdinal = -1;
        for (int i = 0; i < count; i++) {
            int ordinal = readU16(buf, cursor);
            if (ordinal <= previousOrdinal || ordinal >= schema.columnCount()) {
                throw new UndoLogFormatException(
                        "INSERT LOB ownership ordinal is duplicate/out-of-order/out-of-range: " + ordinal);
            }
            previousOrdinal = ordinal;
            ColumnType columnType = schema.column(ordinal).type();
            if (columnType.storageKind() != StorageKind.OVERFLOW_CAPABLE) {
                throw new UndoLogFormatException("INSERT LOB ownership targets non-LOB column: " + ordinal);
            }
            int encodedLength = readU16(buf, cursor);
            if (encodedLength <= 0 || cursor[0] + encodedLength > buf.length) {
                throw new UndoLogFormatException(
                        "INSERT LOB ownership external envelope is truncated/empty: " + encodedLength);
            }
            try {
                ColumnValue decoded = registry.codecFor(columnType).decode(
                        new FieldSlice(buf, cursor[0], encodedLength), columnType);
                if (!(decoded instanceof ColumnValue.ExternalValue external)) {
                    throw new UndoLogFormatException(
                            "INSERT LOB ownership contains inline value for column: " + ordinal);
                }
                ownerships.add(new InsertedLobOwnership(ordinal, external));
            } catch (UndoLogFormatException formatFailure) {
                throw formatFailure;
            } catch (DatabaseRuntimeException invalidEnvelope) {
                throw new UndoLogFormatException(
                        "invalid INSERT LOB ownership envelope for column: " + ordinal, invalidEnvelope);
            }
            cursor[0] += encodedLength;
        }
        return List.copyOf(ownerships);
    }

    private static void requireLobColumnForEncode(int ordinal, ColumnType columnType) {
        if (columnType.storageKind() != StorageKind.OVERFLOW_CAPABLE) {
            throw new DatabaseValidationException("inserted LOB ownership targets non-LOB column: " + ordinal);
        }
    }

    /**
     * 在 old image 后编码 UPDATE/DELETE 的 LOB version ownership。旧链 envelope 已存在 old image，只在
     * rollback-new flag 存在时追加新 external envelope，避免重复持久化 purge 输入。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在写 magic 前校验非空 count，防止失败留下可被误识别的半个 tail。</li>
     *     <li>逐 ordinal 解析 exact schema 列，并核对 purge-old 动作确实指向 external old image。</li>
     *     <li>写 ordinal/flags；rollback-new 存在时复用列 LobCodec 编码完整 external envelope。</li>
     * </ol>
     *
     * @param out        已写完 fixed body 与 old image 的目标输出；方法不关闭它。
     * @param ownerships 按 ordinal 严格递增的非空 LOB version ownership。
     * @param oldRow     UPDATE/DELETE 全量旧 image；purge-old 动作必须在同 ordinal 命中 external value。
     * @param schema     解释 ordinal 和 external envelope 的 exact-version schema。
     * @throws DatabaseValidationException count、ordinal、列类型、旧 image 或 external 编码无效时抛出。
     */
    private void writeLobVersionTail(ByteArrayOutputStream out, List<LobVersionOwnership> ownerships,
                                     List<ColumnValue> oldRow, TableSchema schema) {
        // 1. 先验证 framing 上限，失败不向输出写入 magic/version。
        if (ownerships.isEmpty() || ownerships.size() > 0xFFFF) {
            throw new DatabaseValidationException(
                    "LOB version tail count must be between 1 and 65535: " + ownerships.size());
        }
        writeU16(out, LOB_VERSION_TAIL_MAGIC);
        out.write(LOB_VERSION_TAIL_VERSION);
        writeU16(out, ownerships.size());
        // 2. exact schema 与 old image 是 purge-old 的权威类型/引用来源。
        for (LobVersionOwnership ownership : ownerships) {
            int ordinal = ownership.columnOrdinal();
            if (ordinal > 0xFFFF || ordinal >= schema.columnCount()) {
                throw new DatabaseValidationException("LOB version ordinal exceeds schema/u16: " + ordinal);
            }
            ColumnType columnType = schema.column(ordinal).type();
            requireLobColumnForEncode(ordinal, columnType);
            if (ownership.purgeOldValue()
                    && !(oldRow.get(ordinal) instanceof ColumnValue.ExternalValue)) {
                throw new DatabaseValidationException(
                        "purge-old LOB ownership requires external old image: " + ordinal);
            }
            // 3. 新 external envelope 只在 rollback-new 动作存在时编码；旧 envelope 不重复保存。
            int flags = (ownership.purgeOldValue() ? LOB_VERSION_PURGE_OLD : 0)
                    | (ownership.rollbackNewValue().isPresent() ? LOB_VERSION_ROLLBACK_NEW : 0);
            writeU16(out, ordinal);
            out.write(flags);
            if (ownership.rollbackNewValue().isPresent()) {
                ColumnValue.ExternalValue external = ownership.rollbackNewValue().orElseThrow();
                TypeCodec codec = registry.codecFor(columnType);
                int encodedLength = codec.encodedLength(external, columnType);
                if (encodedLength <= 0 || encodedLength > 0xFFFF) {
                    throw new DatabaseValidationException(
                            "rollback-new LOB envelope length exceeds u16: " + encodedLength);
                }
                byte[] encoded = new byte[encodedLength];
                codec.encode(external, columnType, new FieldWriter(encoded, 0));
                writeU16(out, encodedLength);
                out.writeBytes(encoded);
            }
        }
    }

    /**
     * 解码 LV/v1，并用 exact schema 与 old image 复核 purge/rollback ownership。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 magic/version/count，未知协议或显式空 tail 立即 fail-closed。</li>
     *     <li>逐项校验 ordinal 顺序、LOB 列类型、flags 和 purge-old 的旧 image 证据。</li>
     *     <li>按 rollback-new flag 解码 external envelope，并拒绝 DELETE 携带新链 ownership。</li>
     *     <li>构造领域对象并冻结列表；cursor 停在 LV 末尾，由调用方继续识别 SI 或 EOF。</li>
     * </ol>
     *
     * @param buf        包含完整 undo record 的稳定字节数组。
     * @param cursor     单元素可变偏移；输入指向 LV magic，成功后推进到 tail 末尾。
     * @param recordType 公共前缀已验证的 UPDATE_ROW 或 DELETE_MARK 类型。
     * @param oldRow     已按 exact schema 解码的全量旧 image。
     * @param schema     解析 ordinal、列类型和 external envelope 的 exact-version schema。
     * @return 按 ordinal 严格递增的不可变 LOB version ownership；tail 非空时返回值也非空。
     * @throws UndoLogFormatException 协议、排序、flags、old image、类型或 external envelope 损坏时抛出。
     */
    private List<LobVersionOwnership> readLobVersionTail(byte[] buf, int[] cursor,
                                                          UndoRecordType recordType,
                                                          List<ColumnValue> oldRow,
                                                          TableSchema schema) {
        // 1. framing 决定后续每一项边界；未知值不能按当前版本猜测。
        int magic = readU16(buf, cursor);
        if (magic != LOB_VERSION_TAIL_MAGIC) {
            throw new UndoLogFormatException("unknown LOB version tail magic: " + magic);
        }
        int version = readU8(buf, cursor);
        if (version != LOB_VERSION_TAIL_VERSION) {
            throw new UndoLogFormatException("unknown LOB version tail version: " + version);
        }
        int count = readU16(buf, cursor);
        if (count == 0) {
            throw new UndoLogFormatException("LOB version tail must not encode an empty list");
        }
        // 2. ordinal/type/old image 校验先于领域对象发布。
        List<LobVersionOwnership> ownerships = new ArrayList<>(count);
        int previousOrdinal = -1;
        for (int i = 0; i < count; i++) {
            int ordinal = readU16(buf, cursor);
            if (ordinal <= previousOrdinal || ordinal >= schema.columnCount()) {
                throw new UndoLogFormatException(
                        "LOB version ordinal is duplicate/out-of-order/out-of-range: " + ordinal);
            }
            previousOrdinal = ordinal;
            ColumnType columnType = schema.column(ordinal).type();
            if (columnType.storageKind() != StorageKind.OVERFLOW_CAPABLE) {
                throw new UndoLogFormatException("LOB version ownership targets non-LOB column: " + ordinal);
            }
            int flags = readU8(buf, cursor);
            if (flags == 0 || (flags & ~(LOB_VERSION_PURGE_OLD | LOB_VERSION_ROLLBACK_NEW)) != 0) {
                throw new UndoLogFormatException("unknown/empty LOB version flags: " + flags);
            }
            boolean purgeOld = (flags & LOB_VERSION_PURGE_OLD) != 0;
            boolean rollbackNew = (flags & LOB_VERSION_ROLLBACK_NEW) != 0;
            if (purgeOld && !(oldRow.get(ordinal) instanceof ColumnValue.ExternalValue)) {
                throw new UndoLogFormatException(
                        "purge-old LOB ownership has no external old image: " + ordinal);
            }
            if (recordType == UndoRecordType.DELETE_MARK && rollbackNew) {
                throw new UndoLogFormatException("DELETE_MARK LV tail cannot carry rollback-new ownership");
            }
            // 3. DELETE 没有新链；UPDATE 的新 envelope 继续复用 exact column LobCodec。
            Optional<ColumnValue.ExternalValue> newValue = Optional.empty();
            if (rollbackNew) {
                int encodedLength = readU16(buf, cursor);
                if (encodedLength <= 0 || cursor[0] + encodedLength > buf.length) {
                    throw new UndoLogFormatException(
                            "rollback-new LOB envelope is truncated/empty: " + encodedLength);
                }
                try {
                    ColumnValue decoded = registry.codecFor(columnType).decode(
                            new FieldSlice(buf, cursor[0], encodedLength), columnType);
                    if (!(decoded instanceof ColumnValue.ExternalValue external)) {
                        throw new UndoLogFormatException(
                                "rollback-new LOB ownership contains inline value: " + ordinal);
                    }
                    newValue = Optional.of(external);
                } catch (UndoLogFormatException formatFailure) {
                    throw formatFailure;
                } catch (DatabaseRuntimeException invalidEnvelope) {
                    throw new UndoLogFormatException(
                            "invalid rollback-new LOB envelope at ordinal " + ordinal, invalidEnvelope);
                }
                cursor[0] += encodedLength;
            }
            // 4. 领域构造器再次守住动作非空不变量，异常统一包装成磁盘格式损坏。
            try {
                ownerships.add(new LobVersionOwnership(ordinal, purgeOld, newValue));
            } catch (DatabaseRuntimeException invalidOwnership) {
                throw new UndoLogFormatException(
                        "invalid LOB version ownership at position " + i, invalidOwnership);
            }
        }
        return List.copyOf(ownerships);
    }

    /**
     * 写 secondary mutation 可选尾部。领域对象已保证 index id 严格递增、action/type 与 state 组合合法；
     * codec 仍冻结 count 与稳定 enum code，避免 write plan 之后出现集合迭代顺序漂移。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 tail 非空且 count 可由 u16 表示，失败不写 magic，避免留下可误判的部分尾部。</li>
     *     <li>写稳定 magic、version 与 count framing。</li>
     *     <li>按领域对象既定 index id 顺序写 identity/action/before-state，不重排或猜测默认值。</li>
     * </ol>
     *
     * @param out       已写完公共和类型专用 payload 的目标输出；方法不关闭它。
     * @param mutations 按 index id 严格递增、action/type 已由 UndoRecord 校验的非空列表。
     * @throws DatabaseValidationException 列表为空或数量超过 u16 上限时抛出。
     */
    private static void writeSecondaryTail(ByteArrayOutputStream out,
                                           List<SecondaryUndoMutation> mutations) {
        // 1. 在写 magic 前验证 count，失败不会产生无法回退的半个 tail。
        if (mutations.isEmpty() || mutations.size() > 0xFFFF) {
            throw new DatabaseValidationException(
                    "secondary undo tail count must be between 1 and 65535: " + mutations.size());
        }
        // 2. magic/version/count 是未来协议演进与 fail-closed 解码的边界。
        writeU16(out, SECONDARY_TAIL_MAGIC);
        out.write(SECONDARY_TAIL_VERSION);
        writeU16(out, mutations.size());
        // 3. 保持领域列表的稳定顺序，恢复阶段据此获得确定的跨树 inverse 顺序。
        for (SecondaryUndoMutation mutation : mutations) {
            writeU64(out, mutation.indexId());
            out.write(mutation.action().code());
            out.write(mutation.newEntryBeforeState().code());
        }
    }

    /**
     * 解码并验证 secondary mutation 尾部。任何未知 magic/version/code、空列表、倒序/重复 index 或与 undo type
     * 不兼容的 action/state 都是物理恢复歧义，统一转为 {@link UndoLogFormatException}，禁止泄漏普通参数异常。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验 magic/version/count，未知协议和显式空 tail 立即按格式损坏失败。</li>
     *     <li>由主 undo type 计算唯一合法 action，防止 tail 自行重解释聚簇操作。</li>
     *     <li>逐项读取 index id/action/before-state，校验 identity 递增和动作组合；普通领域异常包装为格式损坏并保留 cause。</li>
     *     <li>返回不可变列表；cursor 精确指向 tail 末尾，由调用方统一检查 EOF/尾随垃圾。</li>
     * </ol>
     *
     * @param buf        包含 secondary tail 的完整 undo record 字节。
     * @param cursor     单元素可变偏移；输入指向 tail magic，成功后推进到 tail 末尾。
     * @param recordType 已由公共前缀验证的主 undo type，决定唯一合法 secondary action。
     * @return 按 index id 严格递增的不可变 secondary mutation 列表。
     * @throws UndoLogFormatException tail 截断、协议字段未知、count/identity/action/state 无效时抛出。
     */
    private static List<SecondaryUndoMutation> readSecondaryTail(byte[] buf, int[] cursor,
                                                                  UndoRecordType recordType) {
        // 1. 先守住协议 framing，未知版本不能按当前布局猜测字段边界。
        int magic = readU16(buf, cursor);
        if (magic != SECONDARY_TAIL_MAGIC) {
            throw new UndoLogFormatException("unknown secondary undo tail magic: " + magic);
        }
        int version = readU8(buf, cursor);
        if (version != SECONDARY_TAIL_VERSION) {
            throw new UndoLogFormatException("unknown secondary undo tail version: " + version);
        }
        int count = readU16(buf, cursor);
        if (count == 0) {
            throw new UndoLogFormatException("secondary undo tail must not encode an empty list");
        }
        // 2. 主 record type 是 action 的唯一权威，tail 中每项都必须与之相同。
        List<SecondaryUndoMutation> mutations = new ArrayList<>(count);
        long previousIndexId = -1;
        SecondaryUndoAction expectedAction = switch (recordType) {
            case INSERT_ROW -> SecondaryUndoAction.INSERT_ENTRY;
            case UPDATE_ROW -> SecondaryUndoAction.CHANGE_KEY;
            case DELETE_MARK -> SecondaryUndoAction.DELETE_MARK_ENTRY;
        };
        // 3. 逐项验证稳定顺序与 action/state；任何领域校验失败都升级为磁盘格式异常。
        for (int i = 0; i < count; i++) {
            long indexId = readU64(buf, cursor);
            if (indexId < 0 || indexId <= previousIndexId) {
                throw new UndoLogFormatException(
                        "secondary undo index id is negative/duplicate/out-of-order: " + indexId);
            }
            previousIndexId = indexId;
            try {
                SecondaryUndoAction action = SecondaryUndoAction.fromCode(readU8(buf, cursor));
                SecondaryEntryBeforeState beforeState = SecondaryEntryBeforeState.fromCode(readU8(buf, cursor));
                if (action != expectedAction) {
                    throw new UndoLogFormatException(recordType + " secondary tail contains action " + action
                            + " instead of " + expectedAction);
                }
                mutations.add(new SecondaryUndoMutation(indexId, action, beforeState));
            } catch (UndoLogFormatException formatFailure) {
                throw formatFailure;
            } catch (DatabaseRuntimeException invalidValue) {
                throw new UndoLogFormatException(
                        "invalid secondary undo mutation at position " + i, invalidValue);
            }
        }
        // 4. 防御性冻结后返回；EOF/尾随垃圾由 decode 的 requireFullyConsumed 统一判断。
        return List.copyOf(mutations);
    }

    /**
     * 查看指定偏移的下一个 u16 magic 而不推进调用方 cursor。
     *
     * @param buf    完整 undo record 字节。
     * @param offset 待探测 tail 的起始偏移。
     * @return big-endian 无符号 u16 magic。
     * @throws UndoLogFormatException 剩余字节不足两个时抛出，表示截断 tail。
     */
    private static int peekU16(byte[] buf, int offset) {
        int[] cursor = {offset};
        return readU16(buf, cursor);
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
