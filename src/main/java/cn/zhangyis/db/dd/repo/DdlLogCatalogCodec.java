package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.DdlCancellation;
import cn.zhangyis.db.dd.ddl.DdlCancellationReason;
import cn.zhangyis.db.dd.ddl.DdlBatchManifest;
import cn.zhangyis.db.dd.ddl.DdlBatchSchemaEntry;
import cn.zhangyis.db.dd.ddl.DdlBatchTableEntry;
import cn.zhangyis.db.dd.ddl.DdlControlState;
import cn.zhangyis.db.dd.ddl.DdlDigestAlgorithm;
import cn.zhangyis.db.dd.ddl.DdlExecutionProtocol;
import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.ddl.DdlRetiredResource;
import cn.zhangyis.db.dd.ddl.DdlRetiredResourceKind;
import cn.zhangyis.db.dd.ddl.DdlRetirementFence;
import cn.zhangyis.db.dd.ddl.DdlSchemaCanonicalFormat;
import cn.zhangyis.db.dd.ddl.DdlSchemaDigest;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.storage.fil.state.TablespaceType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * DDL marker 与 InternalCatalogStore 无语义 record 之间的版本化 codec。v4 首次启用 key 中的
 * chunk ordinal，v5 在同一逻辑 payload 尾部增加批量 DROP manifest。一个逻辑 marker 的全部
 * chunks 位于同一 catalog batch，frame CRC 与 batch SHA 共同保护原子性。
 */
final class DdlLogCatalogCodec {

    /** v1 key没有secondary identity。 */
    private static final int KEY_V1_BYTES = 1 + Long.BYTES * 3 + Integer.BYTES * 2;
    /** v2+ key加入secondary identity并保留末尾chunk ordinal。 */
    private static final int KEY_V2_BYTES = KEY_V1_BYTES + Long.BYTES;
    /** payload magic "DDL1"；format byte紧随其后。 */
    private static final int MAGIC = 0x44444C31;
    /** 当前写格式：v4 checkpoint/control 字段后追加 v5 可选 batch manifest。 */
    private static final int FORMAT_VERSION = 5;
    /** 最老可读table marker格式。 */
    private static final int LEGACY_FORMAT_VERSION = 1;
    /** catalog物理层单record payload上限；必须与FileInternalCatalogStore一致。 */
    private static final int MAX_CHUNK_BYTES = 1024;
    /** 防止损坏chunk count造成无界聚合分配；批量 DROP 最多容纳 4096 张表的恢复清单。 */
    private static final int MAX_LOGICAL_PAYLOAD_BYTES = 4 * 1024 * 1024;
    /** 由逻辑上限与单块上限共同确定的最大chunk数。 */
    private static final int MAX_CHUNKS = MAX_LOGICAL_PAYLOAD_BYTES / MAX_CHUNK_BYTES;
    /** 主路径和辅助路径各自沿用v3的UTF-8上限。 */
    private static final int MAX_PATH_BYTES = 900;
    /**
     * 把一个完整marker编码为同一catalog batch中的连续chunks。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>严格编码路径、checkpoint 与可选 batch manifest，并按 v5 grammar 生成有界逻辑 payload。</li>
     *     <li>在任何catalog副作用前校验总长度和所有optional/fixed-width领域字段。</li>
     *     <li>按1024-byte上限连续切块，每个key重复immutable/phase并写从0开始的ordinal。</li>
     *     <li>返回不可变record列表；repository必须把整组交给一次store.append。</li>
     * </ol>
     *
     * @param record 已由领域构造器验证、路径已规范化的完整marker
     * @return 非空、chunk ordinal连续的catalog records
     * @throws DatabaseValidationException 路径 UTF-8、总长度或字段超出当前格式上限时抛出
     */
    List<CatalogRecord> encode(DdlLogRecord record) {
        if (record == null) {
            throw new DatabaseValidationException("DDL log record must not be null");
        }
        if (record.executionProtocol() == DdlExecutionProtocol.LEGACY_PHASE_ONLY) {
            throw new DatabaseValidationException(
                    "DDL legacy protocol is decode-only and cannot be encoded by current format");
        }
        // 1、路径必须使用REPORT，不能把未配对surrogate替换成相同的U+FFFD后持久化。
        byte[] path = strictUtf8(record.path().toString(), "path");
        byte[] auxiliary = record.auxiliaryPath()
                .map(value -> strictUtf8(value.toString(), "auxiliary path"))
                .orElseGet(() -> new byte[0]);
        requirePathBound(path, "path");
        requirePathBound(auxiliary, "auxiliary path");

        // 2、先在内存形成有明确总上限的逻辑payload；失败不会留下部分catalog batch。
        byte[] payload = encodeLogical(record, path, auxiliary);
        if (payload.length <= 0 || payload.length > MAX_LOGICAL_PAYLOAD_BYTES) {
            throw new DatabaseValidationException(
                    "DDL log logical payload exceeds bound: " + payload.length);
        }

        // 3、chunk key重复identity并携带ordinal；batch顺序就是重组顺序。
        int chunkCount = (payload.length + MAX_CHUNK_BYTES - 1) / MAX_CHUNK_BYTES;
        List<CatalogRecord> records = new ArrayList<>(chunkCount);
        for (int ordinal = 0; ordinal < chunkCount; ordinal++) {
            int from = ordinal * MAX_CHUNK_BYTES;
            int to = Math.min(payload.length, from + MAX_CHUNK_BYTES);
            records.add(new CatalogRecord(key(record, ordinal),
                    java.util.Arrays.copyOfRange(payload, from, to)));
        }
        // 4、List.copyOf避免repository append前被调用方替换任一chunk。
        return List.copyOf(records);
    }

    /**
     * 尝试解码一个committed batch。普通DD batch返回empty；DDL batch任一key/chunk/payload损坏均fail-closed。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>识别首key kind并解析全部chunk key，要求identity完全相同且ordinal从0连续。</li>
     *     <li>限制chunk数、单块和总字节后重组逻辑payload；legacy格式仍只允许单块。</li>
     *     <li>按format分支读取字段并与key逐项交叉校验，所有flag/stable code严格拒绝未知值。</li>
     *     <li>要求逻辑payload精确EOF后构造不可变record；异常统一转换为catalog corruption。</li>
     * </ol>
     *
     * @param batch storage已经完成frame CRC与batch SHA校验的非空原子批次
     * @return 解码后的DDL marker，或batch不属于DDL log时为空
     * @throws DictionaryCatalogCorruptionException DDL batch缺块、漂移、截断或语义非法时抛出
     */
    Optional<DdlLogRecord> decode(CatalogBatch batch) {
        if (batch == null || batch.records().isEmpty()) {
            throw new DictionaryCatalogCorruptionException("DDL log batch must not be null/empty");
        }
        // 1、只看首key决定kind；一旦属于DDL，后续任一混入record都按损坏处理。
        byte[] firstKeyBytes = batch.records().getFirst().key();
        if (firstKeyBytes.length == 0
                || Byte.toUnsignedInt(firstKeyBytes[0]) != CatalogEntityKind.DDL_LOG.stableCode()) {
            return Optional.empty();
        }
        if (batch.records().size() > MAX_CHUNKS) {
            throw new DictionaryCatalogCorruptionException("DDL log chunk count exceeds bound");
        }
        KeyFields firstKey = parseKey(firstKeyBytes);
        if (firstKey.chunkOrdinal() != 0) {
            throw new DictionaryCatalogCorruptionException("DDL log first chunk ordinal must be zero");
        }
        ByteArrayOutputStream logical = new ByteArrayOutputStream();
        for (int ordinal = 0; ordinal < batch.records().size(); ordinal++) {
            CatalogRecord chunk = batch.records().get(ordinal);
            KeyFields current = parseKey(chunk.key());
            if (!firstKey.sameIdentityAndPhase(current) || current.chunkOrdinal() != ordinal
                    || chunk.payload().length <= 0 || chunk.payload().length > MAX_CHUNK_BYTES) {
                throw new DictionaryCatalogCorruptionException(
                        "DDL log chunks are missing, reordered or identity-mismatched");
            }
            logical.writeBytes(chunk.payload());
            if (logical.size() > MAX_LOGICAL_PAYLOAD_BYTES) {
                throw new DictionaryCatalogCorruptionException(
                        "DDL log logical payload exceeds current bound");
            }
        }

        // 2、format位于chunk0固定header；v1-v3永远是单record/chunk0。
        byte[] payload = logical.toByteArray();
        if (payload.length < Integer.BYTES + 1) {
            throw new DictionaryCatalogCorruptionException("truncated DDL log payload header");
        }
        int format = Byte.toUnsignedInt(payload[Integer.BYTES]);
        if (format < 4 && batch.records().size() != 1) {
            throw new DictionaryCatalogCorruptionException("legacy DDL log cannot use chunks");
        }

        // 3、所有底层解析/领域异常都保留cause并提升为启动期catalog corruption。
        try {
            return Optional.of(decodeLogical(payload, format, firstKey));
        } catch (DictionaryCatalogCorruptionException error) {
            throw error;
        } catch (IOException | InvalidPathException | DatabaseValidationException error) {
            throw new DictionaryCatalogCorruptionException(
                    "truncated/invalid DDL log payload", error);
        }
    }

    /** 写出当前 v5 逻辑 payload；DataOutputStream 固定 big-endian。 */
    private static byte[] encodeLogical(DdlLogRecord record, byte[] path, byte[] auxiliary) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                DdlUndoMarker marker = record.marker();
                out.writeInt(MAGIC);
                out.writeByte(FORMAT_VERSION);
                out.writeLong(marker.ddlOperationId());
                out.writeLong(marker.dictionaryVersion());
                out.writeLong(marker.affectedObjectId());
                out.writeLong(record.secondaryObjectId());
                out.writeByte(record.operation().stableCode());
                out.writeByte(record.phase().stableCode());
                out.writeByte(record.executionProtocol().stableCode());
                out.writeByte(record.controlState().stableCode());
                out.writeInt(record.spaceId().value());
                writeShortBytes(out, path);
                writeOptionalPath(out, record.auxiliaryPath().isPresent(), auxiliary);
                writeFileIdentity(out, record.fileIdentity());
                writeDigest(out, record.sourceSchemaDigest());
                writeDigest(out, record.intermediateSchemaDigest());
                writeDigest(out, record.targetSchemaDigest());
                writeCancellation(out, record.cancellation());
                writeRetirementFence(out, record.retirementFence());
                writeBatchManifest(out, record.batchManifest());
            }
            return bytes.toByteArray();
        } catch (IOException error) {
            throw new DatabaseValidationException("failed to encode DDL log payload", error);
        }
    }

    /** 按format读取逻辑payload并与key交叉校验。 */
    private static DdlLogRecord decodeLogical(byte[] bytes, int format, KeyFields key) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readInt() != MAGIC || format != Byte.toUnsignedInt(in.readByte())
                    || format < LEGACY_FORMAT_VERSION || format > FORMAT_VERSION) {
                throw new DictionaryCatalogCorruptionException("invalid DDL log payload header/version");
            }
            if (format == LEGACY_FORMAT_VERSION && key.keyLength() != KEY_V1_BYTES
                    || format >= 2 && key.keyLength() != KEY_V2_BYTES) {
                throw new DictionaryCatalogCorruptionException("DDL key length does not match format");
            }
            long ddlId = in.readLong();
            long dictionaryVersion = in.readLong();
            long objectId = in.readLong();
            long secondaryObjectId = format >= 2 ? in.readLong() : 0L;
            int operationCode = in.readUnsignedByte();
            int phaseCode = in.readUnsignedByte();
            DdlExecutionProtocol protocol = format >= 4
                    ? DdlExecutionProtocol.fromStableCode(in.readUnsignedByte())
                    : DdlExecutionProtocol.LEGACY_PHASE_ONLY;
            if (format >= 4 && protocol == DdlExecutionProtocol.LEGACY_PHASE_ONLY) {
                throw new DictionaryCatalogCorruptionException(
                        "DDL v4 payload cannot declare decode-only legacy protocol");
            }
            DdlControlState control = format >= 4
                    ? DdlControlState.fromStableCode(in.readUnsignedByte())
                    : DdlControlState.OPEN;
            int spaceId = in.readInt();
            Path path = Path.of(strictUtf8(readShortBytes(in, "path"), "path"));
            Optional<Path> auxiliary = format >= 3 ? readOptionalPath(in) : Optional.empty();
            Optional<TablespaceFileIdentity> identity = format >= 3
                    ? readFileIdentity(in, spaceId) : Optional.empty();
            Optional<DdlSchemaDigest> source = format >= 4 ? readDigest(in, "source") : Optional.empty();
            Optional<DdlSchemaDigest> intermediate = format >= 4
                    ? readDigest(in, "intermediate") : Optional.empty();
            Optional<DdlSchemaDigest> target = format >= 4 ? readDigest(in, "target") : Optional.empty();
            Optional<DdlCancellation> cancellation = format >= 4
                    ? readCancellation(in) : Optional.empty();
            Optional<DdlRetirementFence> retirementFence = format >= 4
                    ? readRetirementFence(in) : Optional.empty();
            Optional<DdlBatchManifest> batchManifest = format >= 5
                    ? readBatchManifest(in) : Optional.empty();

            if (in.available() != 0) {
                throw new DictionaryCatalogCorruptionException("DDL log payload has trailing bytes");
            }
            if (key.operationCode() != operationCode || key.ddlId() != ddlId
                    || key.dictionaryVersion() != dictionaryVersion
                    || key.secondaryObjectId() != secondaryObjectId || key.phaseCode() != phaseCode) {
                throw new DictionaryCatalogCorruptionException("DDL log key/payload identity mismatch");
            }
            return new DdlLogRecord(new DdlUndoMarker(ddlId, dictionaryVersion, objectId),
                    secondaryObjectId, DdlLogOperation.fromStableCode(operationCode),
                    DdlLogPhase.fromStableCode(phaseCode), SpaceId.of(spaceId), path,
                    auxiliary, identity, protocol, source, intermediate, target,
                    control, cancellation, retirementFence, batchManifest);
        }
    }

    /** 构造一个重复identity且携带chunk ordinal的v2 key。 */
    private static byte[] key(DdlLogRecord record, int chunkOrdinal) {
        DdlUndoMarker marker = record.marker();
        return ByteBuffer.allocate(KEY_V2_BYTES).order(ByteOrder.BIG_ENDIAN)
                .put((byte) CatalogEntityKind.DDL_LOG.stableCode())
                .putLong(record.operation().stableCode())
                .putLong(marker.ddlOperationId())
                .putLong(marker.dictionaryVersion())
                .putLong(record.secondaryObjectId())
                .putInt(record.phase().stableCode())
                .putInt(chunkOrdinal).array();
    }

    /** 解析v1/v2 key；未知长度、kind或负chunk均按catalog损坏处理。 */
    private static KeyFields parseKey(byte[] bytes) {
        if (bytes == null || bytes.length != KEY_V1_BYTES && bytes.length != KEY_V2_BYTES) {
            throw new DictionaryCatalogCorruptionException(
                    "DDL log key length is invalid: " + (bytes == null ? -1 : bytes.length));
        }
        ByteBuffer key = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (Byte.toUnsignedInt(key.get()) != CatalogEntityKind.DDL_LOG.stableCode()) {
            throw new DictionaryCatalogCorruptionException("DDL log batch mixes catalog entity kinds");
        }
        long operation = key.getLong();
        long ddlId = key.getLong();
        long version = key.getLong();
        long secondary = bytes.length == KEY_V2_BYTES ? key.getLong() : 0L;
        int phase = key.getInt();
        int chunk = key.getInt();
        if (chunk < 0) {
            throw new DictionaryCatalogCorruptionException("DDL log chunk ordinal must be non-negative");
        }
        return new KeyFields(bytes.length, operation, ddlId, version, secondary, phase, chunk);
    }

    /** 严格编码path；未配对surrogate必须在进入catalog前失败。 */
    private static byte[] strictUtf8(String value, String field) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException error) {
            throw new DatabaseValidationException(
                    "DDL log " + field + " is not strict UTF-8 encodable", error);
        }
    }

    /** 严格解码path bytes；损坏序列不能由replacement字符掩盖。 */
    private static String strictUtf8(byte[] bytes, String field) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException error) {
            throw new DictionaryCatalogCorruptionException(
                    "DDL log " + field + " is malformed UTF-8", error);
        }
    }

    /** 校验单条path限制；auxiliary absent的空数组合法。 */
    private static void requirePathBound(byte[] bytes, String field) {
        if (bytes.length > MAX_PATH_BYTES) {
            throw new DatabaseValidationException(
                    "DDL log " + field + " exceeds UTF-8 bound: " + bytes.length);
        }
    }

    /** 写入unsigned-short长度与bytes。 */
    private static void writeShortBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    /** 读取有界unsigned-short长度与bytes。 */
    private static byte[] readShortBytes(DataInputStream in, String field) throws IOException {
        int length = in.readUnsignedShort();
        if (length > MAX_PATH_BYTES || length > in.available()) {
            throw new DictionaryCatalogCorruptionException(
                    "invalid DDL log " + field + " length: " + length);
        }
        return in.readNBytes(length);
    }

    /** 写入0/1 auxiliary path。 */
    private static void writeOptionalPath(DataOutputStream out, boolean present,
                                          byte[] bytes) throws IOException {
        out.writeByte(present ? 1 : 0);
        if (present) {
            writeShortBytes(out, bytes);
        }
    }

    /** 读取0/1 auxiliary path并严格UTF-8解码。 */
    private static Optional<Path> readOptionalPath(DataInputStream in) throws IOException {
        if (!readFlag(in, "auxiliary path")) {
            return Optional.empty();
        }
        return Optional.of(Path.of(strictUtf8(
                readShortBytes(in, "auxiliary path"), "auxiliary path")));
    }

    /** 写入0/1 file identity与固定字段。 */
    private static void writeFileIdentity(DataOutputStream out,
                                          Optional<TablespaceFileIdentity> identity) throws IOException {
        out.writeByte(identity.isPresent() ? 1 : 0);
        if (identity.isPresent()) {
            TablespaceFileIdentity value = identity.orElseThrow();
            out.writeInt(value.pageSize().bytes());
            out.writeInt(value.type().code());
            out.writeInt(value.serverVersion());
            out.writeLong(value.spaceVersion());
        }
    }

    /** 读取0/1 file identity并绑定marker space id。 */
    private static Optional<TablespaceFileIdentity> readFileIdentity(
            DataInputStream in, int spaceId) throws IOException {
        if (!readFlag(in, "file identity")) {
            return Optional.empty();
        }
        return Optional.of(new TablespaceFileIdentity(SpaceId.of(spaceId),
                PageSize.ofBytes(in.readInt()), TablespaceType.fromCode(in.readInt()),
                in.readInt(), in.readLong()));
    }

    /** 写入0/1 digest及algorithm/format/length/bytes。 */
    private static void writeDigest(DataOutputStream out,
                                    Optional<DdlSchemaDigest> digest) throws IOException {
        out.writeByte(digest.isPresent() ? 1 : 0);
        if (digest.isPresent()) {
            DdlSchemaDigest value = digest.orElseThrow();
            byte[] bytes = value.bytes();
            out.writeByte(value.algorithm().stableCode());
            out.writeByte(value.canonicalFormat().stableCode());
            out.writeByte(bytes.length);
            out.write(bytes);
        }
    }

    /** 读取0/1 digest并校验算法固定输出长度。 */
    private static Optional<DdlSchemaDigest> readDigest(
            DataInputStream in, String checkpoint) throws IOException {
        if (!readFlag(in, checkpoint + " schema digest")) {
            return Optional.empty();
        }
        DdlDigestAlgorithm algorithm = DdlDigestAlgorithm.fromStableCode(in.readUnsignedByte());
        DdlSchemaCanonicalFormat format = DdlSchemaCanonicalFormat.fromStableCode(in.readUnsignedByte());
        int length = in.readUnsignedByte();
        if (length != algorithm.digestBytes() || length > in.available()) {
            throw new DictionaryCatalogCorruptionException(
                    "invalid " + checkpoint + " DDL schema digest length: " + length);
        }
        return Optional.of(new DdlSchemaDigest(algorithm, format, in.readNBytes(length)));
    }

    /** 写入0/1 cancellation与固定宽度诊断字段。 */
    private static void writeCancellation(DataOutputStream out,
                                          Optional<DdlCancellation> cancellation) throws IOException {
        out.writeByte(cancellation.isPresent() ? 1 : 0);
        if (cancellation.isPresent()) {
            DdlCancellation value = cancellation.orElseThrow();
            out.writeByte(value.reasonCode().stableCode());
            out.writeLong(value.requestedAtEpochMillis());
            out.writeLong(value.requesterId());
        }
    }

    /** 读取0/1 cancellation。 */
    private static Optional<DdlCancellation> readCancellation(DataInputStream in) throws IOException {
        if (!readFlag(in, "cancellation")) {
            return Optional.empty();
        }
        return Optional.of(new DdlCancellation(
                DdlCancellationReason.fromStableCode(in.readUnsignedByte()),
                in.readLong(), in.readLong()));
    }

    /** 写入0/1 retirement fence及有序resource集合。 */
    private static void writeRetirementFence(DataOutputStream out,
                                             Optional<DdlRetirementFence> fence) throws IOException {
        out.writeByte(fence.isPresent() ? 1 : 0);
        if (fence.isPresent()) {
            DdlRetirementFence value = fence.orElseThrow();
            out.writeLong(value.tableId());
            out.writeLong(value.sourceDictionaryVersion());
            out.writeLong(value.retireThroughTransactionNo());
            out.writeLong(value.sourceMetadataPinVersion());
            out.writeLong(value.descriptorGeneration());
            out.writeLong(value.ownerDdlId());
            out.writeInt(value.resources().size());
            for (DdlRetiredResource resource : value.resources()) {
                out.writeByte(resource.kind().stableCode());
                out.writeLong(resource.resourceId());
            }
        }
    }

    /** 读取0/1 retirement fence并在分配前限制resource count。 */
    private static Optional<DdlRetirementFence> readRetirementFence(DataInputStream in) throws IOException {
        if (!readFlag(in, "retirement fence")) {
            return Optional.empty();
        }
        long tableId = in.readLong();
        long sourceVersion = in.readLong();
        long transactionNo = in.readLong();
        long metadataVersion = in.readLong();
        long generation = in.readLong();
        long ownerDdlId = in.readLong();
        int count = in.readInt();
        if (count <= 0 || count > DdlRetirementFence.MAX_RESOURCES
                || (long) count * (1 + Long.BYTES) > in.available()) {
            throw new DictionaryCatalogCorruptionException(
                    "invalid DDL retirement resource count: " + count);
        }
        List<DdlRetiredResource> resources = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            resources.add(new DdlRetiredResource(
                    DdlRetiredResourceKind.fromStableCode(in.readUnsignedByte()), in.readLong()));
        }
        return Optional.of(new DdlRetirementFence(tableId, sourceVersion, transactionNo,
                metadataVersion, generation, ownerDdlId, resources));
    }

    /**
     * 写入可选 v5 批量 DROP manifest。所有路径均为受控单层相对文件名，表项已由领域对象按 table id 排序。
     */
    private static void writeBatchManifest(
            DataOutputStream out,
            Optional<DdlBatchManifest> manifest) throws IOException {
        out.writeByte(manifest.isPresent() ? 1 : 0);
        if (manifest.isEmpty()) {
            return;
        }
        DdlBatchManifest value = manifest.orElseThrow();
        out.writeByte(value.schema().isPresent() ? 1 : 0);
        if (value.schema().isPresent()) {
            DdlBatchSchemaEntry schema = value.schema().orElseThrow();
            byte[] name = strictUtf8(schema.canonicalName(),
                    "batch schema name");
            requirePathBound(name, "batch schema name");
            out.writeLong(schema.schemaId().value());
            writeShortBytes(out, name);
            writeDigest(out, Optional.of(schema.sourceSchemaDigest()));
            writeDigest(out, Optional.of(schema.targetSchemaDigest()));
        }
        out.writeInt(value.tables().size());
        for (DdlBatchTableEntry table : value.tables()) {
            byte[] relativePath = strictUtf8(
                    table.relativePath(), "batch table path");
            requirePathBound(relativePath, "batch table path");
            out.writeLong(table.tableId().value());
            out.writeInt(table.spaceId().value());
            writeShortBytes(out, relativePath);
            out.writeLong(table.rowFormatVersion());
            writeDigest(out, Optional.of(table.sourceSchemaDigest()));
            writeDigest(out, Optional.of(table.pendingSchemaDigest()));
            writeDigest(out, Optional.of(table.targetSchemaDigest()));
        }
    }

    /**
     * 读取 v5 批量 DROP manifest；在分配集合前限制数量，领域构造器继续校验排序后的唯一 identity。
     */
    private static Optional<DdlBatchManifest> readBatchManifest(
            DataInputStream in) throws IOException {
        if (!readFlag(in, "batch manifest")) {
            return Optional.empty();
        }
        Optional<DdlBatchSchemaEntry> schema = Optional.empty();
        if (readFlag(in, "batch schema")) {
            long schemaId = in.readLong();
            String name = strictUtf8(
                    readShortBytes(in, "batch schema name"),
                    "batch schema name");
            DdlSchemaDigest source = requiredDigest(
                    in, "batch schema source");
            DdlSchemaDigest target = requiredDigest(
                    in, "batch schema target");
            schema = Optional.of(new DdlBatchSchemaEntry(
                    SchemaId.of(schemaId), name, source, target));
        }
        int count = in.readInt();
        if (count < 0 || count > DdlBatchManifest.MAX_TABLES) {
            throw new DictionaryCatalogCorruptionException(
                    "invalid DDL batch table count: " + count);
        }
        List<DdlBatchTableEntry> tables = new ArrayList<>(count);
        long previousTableId = 0L;
        for (int index = 0; index < count; index++) {
            long tableId = in.readLong();
            int spaceId = in.readInt();
            String relativePath = strictUtf8(
                    readShortBytes(in, "batch table path"),
                    "batch table path");
            long rowFormatVersion = in.readLong();
            DdlSchemaDigest source = requiredDigest(
                    in, "batch table source");
            DdlSchemaDigest pending = requiredDigest(
                    in, "batch table pending");
            DdlSchemaDigest target = requiredDigest(
                    in, "batch table target");
            if (tableId <= previousTableId) {
                throw new DictionaryCatalogCorruptionException(
                        "DDL batch table ids are not strictly ordered");
            }
            previousTableId = tableId;
            tables.add(new DdlBatchTableEntry(
                    TableId.of(tableId), SpaceId.of(spaceId),
                    relativePath, rowFormatVersion,
                    source, pending, target));
        }
        return Optional.of(new DdlBatchManifest(schema, tables));
    }

    /** 读取 manifest 中必须存在的 digest；absent 是格式损坏而不是默认值。 */
    private static DdlSchemaDigest requiredDigest(
            DataInputStream in, String checkpoint) throws IOException {
        return readDigest(in, checkpoint).orElseThrow(() ->
                new DictionaryCatalogCorruptionException(
                        "DDL batch manifest lacks required digest: "
                                + checkpoint));
    }

    /** 读取只允许0/1的optional flag。 */
    private static boolean readFlag(DataInputStream in, String field) throws IOException {
        int flag = in.readUnsignedByte();
        if (flag != 0 && flag != 1) {
            throw new DictionaryCatalogCorruptionException(
                    "unknown DDL log " + field + " flag: " + flag);
        }
        return flag == 1;
    }

    /** key中重复保存的immutable/phase字段。 */
    private record KeyFields(int keyLength, long operationCode, long ddlId,
                             long dictionaryVersion, long secondaryObjectId,
                             int phaseCode, int chunkOrdinal) {
        /** @return 除chunk ordinal外的key字段是否逐项相同。 */
        private boolean sameIdentityAndPhase(KeyFields other) {
            return other != null && keyLength == other.keyLength
                    && operationCode == other.operationCode && ddlId == other.ddlId
                    && dictionaryVersion == other.dictionaryVersion
                    && secondaryObjectId == other.secondaryObjectId && phaseCode == other.phaseCode;
        }
    }
}
