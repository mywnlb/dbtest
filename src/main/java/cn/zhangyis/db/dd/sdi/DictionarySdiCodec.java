package cn.zhangyis.db.dd.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * `TableDefinition` 聚合与 SDI payload 的确定性 codec。该类只处理 DD 领域对象，
 * page envelope、CRC、MTR 和 durable flush 全部由 storage facade 负责。
 */
public final class DictionarySdiCodec {

    /** payload magic：ASCII `DDS1`。 */
    private static final int MAGIC = 0x44445331;
    /** v2 为 binding 增加独立物理行格式版本；decoder 继续接受 v1。 */
    private static final int FORMAT_VERSION = 2;
    private static final int LEGACY_FORMAT_VERSION = 1;
    /** 单个名称、路径或 ENUM/SET symbol 的 UTF-8 上界。 */
    private static final int MAX_STRING_BYTES = 8 * 1024;
    /** 防止损坏 count 导致无界分配。 */
    private static final int MAX_COLUMNS = 4_096;
    private static final int MAX_INDEXES = 1_024;
    private static final int MAX_KEY_PARTS = 1_024;
    private static final int MAX_SYMBOLS = 65_535;

    /**
     * 编码一张已绑定物理存储的 ACTIVE 表。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先验证 table lifecycle 与 binding；SDI 只冗余可供普通 SQL 打开的 committed 形状。</li>
     *     <li>按固定顺序写聚合根、物理 binding、columns 和 indexes；所有枚举使用显式稳定 code。</li>
     *     <li>关闭流并返回独立字节数组；相同不可变定义不会依赖 HashMap 顺序或平台默认编码。</li>
     * </ol>
     *
     * @param table 待冗余的 ACTIVE 表聚合；必须携带完整 storage binding
     * @return 可作为 storage opaque payload 持久化的确定性 SDI v1 字节
     * @throws DatabaseValidationException table 为空、非 ACTIVE 或缺少 binding 时抛出，调用方不得发布 DD
     * @throws DictionarySdiCorruptionException 字符串超出格式上界或 JVM 编码流异常时抛出
     */
    public byte[] encode(TableDefinition table) {
        // 1. 只允许完整 ACTIVE 聚合进入 SDI；DROP 生命周期保留旧快照直到文件删除，不另写状态。
        if (table == null || table.state() != TableState.ACTIVE || table.storageBinding().isEmpty()) {
            throw new DatabaseValidationException("SDI encode requires an ACTIVE table with storage binding");
        }
        TableStorageBinding binding = table.storageBinding().orElseThrow();

        // 2. 固定字段顺序与显式 code 构成 v1 持久协议，不复用 enum ordinal。
        return write(out -> {
            out.writeInt(MAGIC);
            out.writeInt(FORMAT_VERSION);
            out.writeLong(table.id().value());
            out.writeLong(table.schemaId().value());
            writeName(out, table.name());
            out.writeLong(table.version().value());
            out.writeByte(tableStateCode(table.state()));
            writeBinding(out, binding);
            out.writeInt(table.columns().size());
            for (ColumnDefinition column : table.columns()) {
                writeColumn(out, column);
            }
            out.writeInt(table.indexes().size());
            for (IndexDefinition index : table.indexes()) {
                writeIndex(out, index);
            }
        });
        // 3. write helper 关闭 DataOutputStream 并返回新数组；无共享 mutable buffer。
    }

    /**
     * 解码并重新建立 `TableDefinition` 的全部构造不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 magic/version，未知格式不做最佳努力解析。</li>
     *     <li>按有界 count 读取 root/binding/children，并校验所有稳定 code、字符串和物理 identity。</li>
     *     <li>构造不可变 table 聚合并要求输入恰好 EOF；截断、尾随或构造失败统一转成可诊断的 SDI 损坏。</li>
     * </ol>
     *
     * @param payload 从已通过 page-level CRC 的 SDI body 取得的完整 payload；不能为 {@code null}
     * @return 恢复全部逻辑定义和 storage binding 的 ACTIVE 表聚合
     * @throws DictionarySdiCorruptionException payload 为空、未知、截断、越界、尾随或破坏聚合不变量时抛出
     */
    public TableDefinition decode(byte[] payload) {
        // 1. 入口先拒绝空 payload，再由固定 magic/version 建立唯一解码分支。
        if (payload == null || payload.length == 0) {
            throw new DictionarySdiCorruptionException("SDI payload must not be null or empty");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            if (in.readInt() != MAGIC) {
                throw new DictionarySdiCorruptionException("invalid dictionary SDI payload magic");
            }
            int format = in.readInt();
            if (format != LEGACY_FORMAT_VERSION && format != FORMAT_VERSION) {
                throw new DictionarySdiCorruptionException("unsupported dictionary SDI payload format: " + format);
            }

            // 2. count 和 code 均先验证再分配集合，防止损坏 payload 放大内存占用。
            TableId tableId = TableId.of(in.readLong());
            SchemaId schemaId = SchemaId.of(in.readLong());
            ObjectName name = readName(in);
            DictionaryVersion version = DictionaryVersion.of(in.readLong());
            TableState state = tableStateFromCode(in.readUnsignedByte());
            if (state != TableState.ACTIVE) {
                throw new DictionarySdiCorruptionException("SDI v1 table state must be ACTIVE: " + state);
            }
            TableStorageBinding binding = readBinding(in, tableId, version, format);
            int columnCount = boundedPositiveCount(in.readInt(), MAX_COLUMNS, "column");
            List<ColumnDefinition> columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(readColumn(in));
            }
            int indexCount = boundedPositiveCount(in.readInt(), MAX_INDEXES, "index");
            List<IndexDefinition> indexes = new ArrayList<>(indexCount);
            for (int i = 0; i < indexCount; i++) {
                indexes.add(readIndex(in));
            }

            // 3. TableDefinition 构造器复核 child/binding 集合；EOF 防止未知尾部被静默接受。
            TableDefinition decoded = new TableDefinition(tableId, schemaId, name, version, state,
                    columns, indexes, Optional.of(binding));
            if (in.available() != 0) {
                throw new DictionarySdiCorruptionException("dictionary SDI payload has trailing bytes");
            }
            return decoded;
        } catch (DictionarySdiCorruptionException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new DictionarySdiCorruptionException("decode dictionary SDI payload failed", e);
        }
    }

    private static void writeBinding(DataOutputStream out, TableStorageBinding binding) throws IOException {
        out.writeLong(binding.tableId());
        out.writeInt(binding.spaceId().value());
        writeString(out, binding.path().toString());
        out.writeInt(binding.indexes().size());
        for (IndexStorageBinding index : binding.indexes()) {
            out.writeLong(index.indexId());
            out.writeLong(index.rootPageId().pageNo().value());
            out.writeInt(index.rootLevel());
            writeSegment(out, index.leafSegment());
            writeSegment(out, index.nonLeafSegment());
        }
        out.writeBoolean(binding.lobSegment().isPresent());
        if (binding.lobSegment().isPresent()) {
            writeSegment(out, binding.lobSegment().orElseThrow());
        }
        out.writeLong(binding.rowFormatVersion());
    }

    private static TableStorageBinding readBinding(DataInputStream in, TableId expectedTableId,
                                                   DictionaryVersion tableVersion, int format) throws IOException {
        long tableId = in.readLong();
        if (tableId != expectedTableId.value()) {
            throw new DictionarySdiCorruptionException("SDI root/binding table identity mismatch");
        }
        SpaceId spaceId = SpaceId.of(in.readInt());
        Path path = Path.of(readString(in));
        int count = boundedPositiveCount(in.readInt(), MAX_INDEXES, "binding index");
        List<IndexStorageBinding> indexes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long indexId = in.readLong();
            PageId root = PageId.of(spaceId, PageNo.of(in.readLong()));
            int level = in.readInt();
            indexes.add(new IndexStorageBinding(indexId, root, level,
                    readSegment(in, spaceId), readSegment(in, spaceId)));
        }
        Optional<SegmentRef> lob = in.readBoolean()
                ? Optional.of(readSegment(in, spaceId)) : Optional.empty();
        long rowFormatVersion = format == LEGACY_FORMAT_VERSION ? tableVersion.value() : in.readLong();
        return new TableStorageBinding(tableId, spaceId, path, rowFormatVersion, indexes, lob);
    }

    private static void writeColumn(DataOutputStream out, ColumnDefinition column) throws IOException {
        out.writeLong(column.columnId());
        writeName(out, column.name());
        out.writeInt(column.ordinal());
        ColumnTypeDefinition type = column.type();
        out.writeInt(type.typeId().stableCode());
        out.writeBoolean(type.unsigned());
        out.writeBoolean(type.nullable());
        out.writeInt(type.length());
        out.writeInt(type.scale());
        out.writeInt(type.charsetId());
        out.writeInt(type.collationId());
        out.writeInt(type.symbols().size());
        for (String symbol : type.symbols()) {
            writeString(out, symbol);
        }
    }

    private static ColumnDefinition readColumn(DataInputStream in) throws IOException {
        long columnId = in.readLong();
        ObjectName name = readName(in);
        int ordinal = in.readInt();
        DictionaryTypeId typeId = DictionaryTypeId.fromStableCode(in.readInt());
        boolean unsigned = in.readBoolean();
        boolean nullable = in.readBoolean();
        int length = in.readInt();
        int scale = in.readInt();
        int charset = in.readInt();
        int collation = in.readInt();
        int symbolCount = boundedCount(in.readInt(), MAX_SYMBOLS, "symbol");
        List<String> symbols = new ArrayList<>(symbolCount);
        for (int i = 0; i < symbolCount; i++) {
            symbols.add(readString(in));
        }
        return new ColumnDefinition(columnId, name,
                new ColumnTypeDefinition(typeId, unsigned, nullable, length, scale, charset, collation, symbols),
                ordinal);
    }

    private static void writeIndex(DataOutputStream out, IndexDefinition index) throws IOException {
        out.writeLong(index.id().value());
        writeName(out, index.name());
        out.writeBoolean(index.unique());
        out.writeBoolean(index.clustered());
        out.writeInt(index.keyParts().size());
        for (IndexKeyPart part : index.keyParts()) {
            out.writeLong(part.columnId());
            out.writeByte(indexOrderCode(part.order()));
            out.writeInt(part.prefixBytes());
        }
    }

    private static IndexDefinition readIndex(DataInputStream in) throws IOException {
        IndexId id = IndexId.of(in.readLong());
        ObjectName name = readName(in);
        boolean unique = in.readBoolean();
        boolean clustered = in.readBoolean();
        int count = boundedPositiveCount(in.readInt(), MAX_KEY_PARTS, "index key part");
        List<IndexKeyPart> parts = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            parts.add(new IndexKeyPart(in.readLong(), indexOrderFromCode(in.readUnsignedByte()), in.readInt()));
        }
        return new IndexDefinition(id, name, unique, clustered, parts);
    }

    private static void writeSegment(DataOutputStream out, SegmentRef segment) throws IOException {
        out.writeInt(segment.inodeSlot());
        out.writeLong(segment.segmentId().value());
    }

    private static SegmentRef readSegment(DataInputStream in, SpaceId spaceId) throws IOException {
        return new SegmentRef(spaceId, in.readInt(), SegmentId.of(in.readLong()));
    }

    private static void writeName(DataOutputStream out, ObjectName name) throws IOException {
        writeString(out, name.displayName());
        writeString(out, name.canonicalName());
    }

    private static ObjectName readName(DataInputStream in) throws IOException {
        String display = readString(in);
        String canonical = readString(in);
        ObjectName name = ObjectName.of(display);
        if (!name.canonicalName().equals(canonical)) {
            throw new DictionarySdiCorruptionException(
                    "dictionary SDI name canonical form mismatch: " + display);
        }
        return name;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            throw new DictionarySdiCorruptionException("dictionary SDI string must not be null");
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new DictionarySdiCorruptionException("dictionary SDI string exceeds codec bound");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new DictionarySdiCorruptionException("invalid dictionary SDI string length: " + length);
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new DictionarySdiCorruptionException("truncated dictionary SDI string");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int boundedPositiveCount(int count, int maximum, String label) {
        if (count <= 0 || count > maximum) {
            throw new DictionarySdiCorruptionException(
                    "invalid dictionary SDI " + label + " count: " + count);
        }
        return count;
    }

    private static int boundedCount(int count, int maximum, String label) {
        if (count < 0 || count > maximum) {
            throw new DictionarySdiCorruptionException(
                    "invalid dictionary SDI " + label + " count: " + count);
        }
        return count;
    }

    private static int tableStateCode(TableState state) {
        return switch (state) {
            case ACTIVE -> 1;
            case DROP_PENDING -> 2;
            case DROPPED -> 3;
        };
    }

    private static TableState tableStateFromCode(int code) {
        return switch (code) {
            case 1 -> TableState.ACTIVE;
            case 2 -> TableState.DROP_PENDING;
            case 3 -> TableState.DROPPED;
            default -> throw new DictionarySdiCorruptionException("unknown SDI table state code: " + code);
        };
    }

    private static int indexOrderCode(IndexOrder order) {
        return switch (order) {
            case ASC -> 1;
            case DESC -> 2;
        };
    }

    private static IndexOrder indexOrderFromCode(int code) {
        return switch (code) {
            case 1 -> IndexOrder.ASC;
            case 2 -> IndexOrder.DESC;
            default -> throw new DictionarySdiCorruptionException("unknown SDI index order code: " + code);
        };
    }

    private static byte[] write(IoWriter writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writer.write(out);
            }
            return bytes.toByteArray();
        } catch (DictionarySdiCorruptionException e) {
            throw e;
        } catch (IOException e) {
            throw new DictionarySdiCorruptionException("encode dictionary SDI payload failed", e);
        }
    }

    @FunctionalInterface
    private interface IoWriter {
        void write(DataOutputStream out) throws IOException;
    }
}
