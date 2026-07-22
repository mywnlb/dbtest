package cn.zhangyis.db.storage.record.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineClusteredIdentityCodec;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordDecoder;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * SHADOW rebuild clustered identity v1 codec。按clustered key part声明顺序构造紧凑非聚簇schema，
 * 因此payload不含普通行值、DB_TRX_ID、roll pointer或跨space LOB reference。
 */
public final class ClusteredIdentityCandidateCodec implements OnlineClusteredIdentityCodec {

    /** ASCII `OCID`。 */
    private static final int MAGIC = 0x4f434944;
    /** 独立格式版本。 */
    private static final short VERSION = 1;
    /** magic/version/key count/record length。 */
    private static final int PREFIX_BYTES = Integer.BYTES + Short.BYTES * 2 + Integer.BYTES;

    /** source clustered完整行schema，用于按key part ordinal投影。 */
    private final TableSchema sourceSchema;
    /** key part对应的source ordinal，保留聚簇声明顺序。 */
    private final List<Integer> sourceOrdinals;
    /** 只含聚簇键列且不带隐藏列的紧凑编码schema。 */
    private final TableSchema identitySchema;
    /** 复用record稳定字段格式。 */
    private final RecordEncoder encoder;
    /** 与encoder对称的严格decoder。 */
    private final RecordDecoder decoder;

    /**
     * @param clustered source exact-version聚簇B+Tree descriptor
     * @param registry 与source record/B+Tree共享的类型codec registry
     */
    public ClusteredIdentityCandidateCodec(BTreeIndex clustered,
                                            TypeCodecRegistry registry) {
        if (clustered == null || !clustered.clustered() || registry == null) {
            throw new DatabaseValidationException(
                    "clustered identity codec requires clustered index/registry");
        }
        this.sourceSchema = clustered.schema();
        List<Integer> ordinals = new ArrayList<>(clustered.keyDef().parts().size());
        List<ColumnDef> columns = new ArrayList<>(clustered.keyDef().parts().size());
        for (int index = 0; index < clustered.keyDef().parts().size(); index++) {
            int sourceOrdinal = clustered.keyDef().parts().get(index).columnId().value();
            ColumnDef source = sourceSchema.column(sourceOrdinal);
            ordinals.add(sourceOrdinal);
            columns.add(new ColumnDef(new ColumnId(index), "pk$" + index + "$" + source.name(),
                    source.type(), index));
        }
        this.sourceOrdinals = List.copyOf(ordinals);
        this.identitySchema = new TableSchema(sourceSchema.schemaVersion(), columns, false);
        this.encoder = new RecordEncoder(registry);
        this.decoder = new RecordDecoder(registry);
    }

    /** INSERT捕获after聚簇identity。 */
    @Override
    public Optional<byte[]> encodeInsert(LogicalRecord after) {
        return Optional.of(encode(project(after, "INSERT")));
    }

    /** UPDATE要求主键不变并只捕获一份identity；v1明确禁止PK ALTER/DML变更。 */
    @Override
    public Optional<byte[]> encodeUpdate(LogicalRecord before, LogicalRecord after) {
        LogicalRecord beforeIdentity = project(before, "UPDATE before");
        LogicalRecord afterIdentity = project(after, "UPDATE after");
        if (!beforeIdentity.columnValues().equals(afterIdentity.columnValues())) {
            throw new DatabaseValidationException(
                    "online shadow rebuild v1 does not support clustered key mutation");
        }
        return Optional.of(encode(afterIdentity));
    }

    /** DELETE捕获before聚簇identity。 */
    @Override
    public Optional<byte[]> encodeDelete(LogicalRecord before) {
        return Optional.of(encode(project(before, "DELETE")));
    }

    /**
     * 解码完整identity并拒绝未知magic/version/count、长度截断或尾随数据。
     *
     * @param payload journal CANDIDATE的完整opaque payload
     * @return 按clustered key part顺序排列的完整SearchKey
     */
    @Override
    public SearchKey decode(byte[] payload) {
        if (payload == null || payload.length < PREFIX_BYTES) {
            throw new DatabaseValidationException(
                    "clustered identity candidate is truncated");
        }
        ByteBuffer input = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int magic = input.getInt();
        int version = Short.toUnsignedInt(input.getShort());
        int keyCount = Short.toUnsignedInt(input.getShort());
        int length = input.getInt();
        if (magic != MAGIC || version != VERSION || keyCount != sourceOrdinals.size()
                || length <= 0 || length != input.remaining()) {
            throw new DatabaseValidationException(
                    "clustered identity candidate header/length is invalid");
        }
        byte[] recordBytes = new byte[length];
        input.get(recordBytes);
        LogicalRecord identity = decoder.decode(recordBytes, identitySchema);
        if (identity.deleted() || identity.hiddenColumns() != null) {
            throw new DatabaseValidationException(
                    "clustered identity candidate record flags are invalid");
        }
        return new SearchKey(identity.columnValues());
    }

    /** 验证完整source row shape并按聚簇key声明顺序投影。 */
    private LogicalRecord project(LogicalRecord row, String operation) {
        if (row == null || row.schemaVersion() != sourceSchema.schemaVersion()
                || row.columnValues().size() != sourceSchema.columnCount()) {
            throw new DatabaseValidationException(
                    "clustered identity " + operation + " row does not match source schema");
        }
        List<ColumnValue> values = sourceOrdinals.stream()
                .map(row.columnValues()::get).toList();
        return new LogicalRecord(identitySchema.schemaVersion(), values,
                false, RecordType.CONVENTIONAL, null);
    }

    /** 编码紧凑record并加独立类型与key count前缀。 */
    private byte[] encode(LogicalRecord identity) {
        byte[] record = encoder.encode(identity, identitySchema);
        return ByteBuffer.allocate(PREFIX_BYTES + record.length).order(ByteOrder.BIG_ENDIAN)
                .putInt(MAGIC).putShort(VERSION).putShort((short) sourceOrdinals.size())
                .putInt(record.length).put(record).array();
    }
}
