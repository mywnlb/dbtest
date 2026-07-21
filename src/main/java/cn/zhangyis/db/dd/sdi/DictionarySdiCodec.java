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
import cn.zhangyis.db.dd.domain.TableOptions;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
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

    /** payload magic：ASCII `DDS1`。
     *
     * 持久格式魔数；读取端用它拒绝错文件或损坏内容，修改会破坏已有数据兼容性。
     */
    private static final int MAGIC = 0x44445331;
    /** v2 为 binding 增加独立物理行格式版本；decoder 继续接受 v1。 */
    private static final int FORMAT_VERSION = 3;
    /**
     * 当前稳定格式版本；编解码与恢复路径共同依赖该值，升级时必须保留旧版本判定。
     */
    private static final int LEGACY_FORMAT_VERSION = 1;
    /** v2 只有 rowFormatVersion，没有 table/default 扩展。 */
    private static final int ROW_FORMAT_VERSION = 2;
    /** 单个名称、路径或 ENUM/SET symbol 的 UTF-8 上界。 */
    private static final int MAX_STRING_BYTES = 8 * 1024;
    /** 防止损坏 count 导致无界分配。 */
    private static final int MAX_COLUMNS = 4_096;
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
    private static final int MAX_INDEXES = 1_024;
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
    private static final int MAX_KEY_PARTS = 1_024;
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
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
            writeString(out, table.options().comment());
            out.writeInt(table.options().defaultCharsetId());
            out.writeInt(table.options().defaultCollationId());
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
            if (format != LEGACY_FORMAT_VERSION && format != ROW_FORMAT_VERSION
                    && format != FORMAT_VERSION) {
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
            TableOptions options = format == FORMAT_VERSION
                    ? new TableOptions(readString(in), in.readInt(), in.readInt())
                    : TableOptions.legacyDefaults();
            TableStorageBinding binding = readBinding(in, tableId, version, format);
            int columnCount = boundedPositiveCount(in.readInt(), MAX_COLUMNS, "column");
            List<ColumnDefinition> columns = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                columns.add(readColumn(in, format));
            }
            int indexCount = boundedPositiveCount(in.readInt(), MAX_INDEXES, "index");
            List<IndexDefinition> indexes = new ArrayList<>(indexCount);
            for (int i = 0; i < indexCount; i++) {
                indexes.add(readIndex(in));
            }

            // 3. TableDefinition 构造器复核 child/binding 集合；EOF 防止未知尾部被静默接受。
            TableDefinition decoded = new TableDefinition(tableId, schemaId, name, version, state,
                    columns, indexes, Optional.of(binding), options);
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

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param out 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param binding 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private static void writeBinding(DataOutputStream out, TableStorageBinding binding) throws IOException {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        out.writeLong(binding.tableId());
        out.writeInt(binding.spaceId().value());
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        writeString(out, binding.path().toString());
        out.writeInt(binding.indexes().size());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
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
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        out.writeLong(binding.rowFormatVersion());
    }

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param expectedTableId 目标表的稳定字典标识；不得为 {@code null}，且必须与当前元数据和物理绑定一致
     * @param tableVersion 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param format 参与 {@code readBinding} 的稳定编码 {@code format}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     * @return {@code readBinding} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws DictionarySdiCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static TableStorageBinding readBinding(DataInputStream in, TableId expectedTableId,
                                                   DictionaryVersion tableVersion, int format) throws IOException {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        long tableId = in.readLong();
        if (tableId != expectedTableId.value()) {
            throw new DictionarySdiCorruptionException("SDI root/binding table identity mismatch");
        }
        SpaceId spaceId = SpaceId.of(in.readInt());
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        Path path = Path.of(readString(in));
        int count = boundedPositiveCount(in.readInt(), MAX_INDEXES, "binding index");
        List<IndexStorageBinding> indexes = new ArrayList<>(count);
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
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
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new TableStorageBinding(tableId, spaceId, path, rowFormatVersion, indexes, lob);
    }

    /**
     * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param out 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param column 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private static void writeColumn(DataOutputStream out, ColumnDefinition column) throws IOException {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        out.writeLong(column.columnId());
        writeName(out, column.name());
        out.writeInt(column.ordinal());
        ColumnTypeDefinition type = column.type();
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        out.writeInt(type.typeId().stableCode());
        out.writeBoolean(type.unsigned());
        out.writeBoolean(type.nullable());
        out.writeInt(type.length());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        out.writeInt(type.scale());
        out.writeInt(type.charsetId());
        out.writeInt(type.collationId());
        out.writeInt(type.symbols().size());
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        for (String symbol : type.symbols()) {
            writeString(out, symbol);
        }
        out.writeByte(columnDefaultCode(column.defaultDefinition().kind()));
        if (column.defaultDefinition().constantLiteral().isPresent()) {
            writeString(out, column.defaultDefinition().constantLiteral().orElseThrow());
        }
    }

    /**
     * 根据调用参数创建或转换 {@code readColumn} 返回的 {@code ColumnDefinition}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @return {@code readColumn} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private static ColumnDefinition readColumn(DataInputStream in, int format) throws IOException {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        long columnId = in.readLong();
        ObjectName name = readName(in);
        int ordinal = in.readInt();
        DictionaryTypeId typeId = DictionaryTypeId.fromStableCode(in.readInt());
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        boolean unsigned = in.readBoolean();
        boolean nullable = in.readBoolean();
        int length = in.readInt();
        int scale = in.readInt();
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        int charset = in.readInt();
        int collation = in.readInt();
        int symbolCount = boundedCount(in.readInt(), MAX_SYMBOLS, "symbol");
        List<String> symbols = new ArrayList<>(symbolCount);
        for (int i = 0; i < symbolCount; i++) {
            symbols.add(readString(in));
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        ColumnTypeDefinition type = new ColumnTypeDefinition(
                typeId, unsigned, nullable, length, scale, charset, collation, symbols);
        ColumnDefaultDefinition defaultDefinition = format == FORMAT_VERSION
                ? readColumnDefault(in) : nullable
                ? ColumnDefaultDefinition.implicitNull() : ColumnDefaultDefinition.required();
        return new ColumnDefinition(columnId, name, type, ordinal, defaultDefinition);
    }

    private static int columnDefaultCode(ColumnDefaultDefinition.Kind kind) {
        return switch (kind) {
            case REQUIRED -> 1;
            case IMPLICIT_NULL -> 2;
            case CONSTANT -> 3;
        };
    }

    private static ColumnDefaultDefinition readColumnDefault(DataInputStream in) throws IOException {
        return switch (in.readUnsignedByte()) {
            case 1 -> ColumnDefaultDefinition.required();
            case 2 -> ColumnDefaultDefinition.implicitNull();
            case 3 -> ColumnDefaultDefinition.constant(readString(in));
            default -> throw new DictionarySdiCorruptionException(
                    "unknown SDI column default code");
        };
    }

    /**
     * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param out 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param index 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private static void writeIndex(DataOutputStream out, IndexDefinition index) throws IOException {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        out.writeLong(index.id().value());
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        writeName(out, index.name());
        out.writeBoolean(index.unique());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        out.writeBoolean(index.clustered());
        out.writeInt(index.keyParts().size());
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        for (IndexKeyPart part : index.keyParts()) {
            out.writeLong(part.columnId());
            out.writeByte(indexOrderCode(part.order()));
            out.writeInt(part.prefixBytes());
        }
    }

    /**
     * 根据调用参数创建或转换 {@code readIndex} 返回的 {@code IndexDefinition}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @return {@code readIndex} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     */
    private static IndexDefinition readIndex(DataInputStream in) throws IOException {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        IndexId id = IndexId.of(in.readLong());
        ObjectName name = readName(in);
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        boolean unique = in.readBoolean();
        boolean clustered = in.readBoolean();
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        int count = boundedPositiveCount(in.readInt(), MAX_KEY_PARTS, "index key part");
        List<IndexKeyPart> parts = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            parts.add(new IndexKeyPart(in.readLong(), indexOrderFromCode(in.readUnsignedByte()), in.readInt()));
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
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

    /**
     * 根据调用参数创建或转换 {@code readName} 返回的 {@code ObjectName}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @return {@code readName} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws DictionarySdiCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
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

    /**
     * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param out 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param value 传给 {@code writeString} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws DictionarySdiCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
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

    /**
     * 根据调用参数创建或转换 {@code readString} 返回的 {@code String}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param in 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @return {@code readString} 生成的非空文本表示；字符顺序保持 SQL、标识符或诊断格式约定，无结果时返回空串而非 {@code null}
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws DictionarySdiCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
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
            case DISCARD_PENDING, DISCARDED, IMPORT_PENDING ->
                    throw new DictionarySdiCorruptionException("non-active tablespace state cannot enter SDI: " + state);
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

    /**
     * 根据调用参数创建或转换 {@code write} 返回的 {@code byte[]}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param writer 由组合根提供的 {@code IoWriter} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code write} 调用
     * @return {@code write} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     * @throws DictionarySdiCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
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

    /**
     * 定义数据字典的 {@code IoWriter} 稳定协作契约；调用方只依赖该接口，不读取实现内部状态或资源。
     */
    @FunctionalInterface
    private interface IoWriter {
        /**
         * 校验输入与当前状态后修改数据字典领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
         *
         * @param out 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
         * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
         */
        void write(DataOutputStream out) throws IOException;
    }
}
