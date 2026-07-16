package cn.zhangyis.db.dd.repo;

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
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Table catalog LOB 尾部的 golden/兼容性测试；manifest 每次重算，避免先被批次哈希挡住。 */
class DictionaryCatalogCodecTest {

    private static final DictionaryVersion VERSION = DictionaryVersion.of(2);

    /** 旧 table payload 在 index binding 后 EOF；其前缀字节保持不变并解码为无 LOB segment。 */
    @Test
    void decodesLegacyTablePayloadAndKeepsOldPrefixBytesStable() {
        TableDefinition legacy = table(Optional.empty());
        DictionaryCatalogCodec codec = new DictionaryCatalogCodec();
        List<CatalogRecord> encoded = codec.encode(VERSION, List.of(), List.of(legacy));
        byte[] independentGolden = legacyTablePayload(legacy);
        byte[] newPayload = encoded.getFirst().payload();

        assertArrayEquals(independentGolden, Arrays.copyOf(newPayload, newPayload.length - 1),
                "新 codec 只能在旧 table payload 尾部追加能力位");
        DictionaryCatalogCodec.DecodedMutation decoded = codec.decode(batchWithTablePayload(
                encoded, independentGolden)).orElseThrow();

        assertTrue(decoded.tables().getFirst().storageBinding().orElseThrow().lobSegment().isEmpty());
    }

    /** 新 payload 必须保留完整 LOB segment identity，不能仅持久化一个 capability boolean。 */
    @Test
    void roundTripsNewTableLobSegmentTail() {
        SegmentRef lob = new SegmentRef(SpaceId.of(1024), 3, SegmentId.of(13));
        TableDefinition table = table(Optional.of(lob));
        DictionaryCatalogCodec codec = new DictionaryCatalogCodec();

        DictionaryCatalogCodec.DecodedMutation decoded = codec.decode(new CatalogBatch(1,
                codec.encode(VERSION, List.of(), List.of(table)))).orElseThrow();

        assertEquals(lob, decoded.tables().getFirst().storageBinding().orElseThrow()
                .lobSegment().orElseThrow());
    }

    /** 尾部不是可跳过扩展：未知 flag、截断 segment 和额外字节都表示 catalog 损坏。 */
    @Test
    void rejectsUnknownTruncatedAndTrailingLobTail() {
        DictionaryCatalogCodec codec = new DictionaryCatalogCodec();
        List<CatalogRecord> encoded = codec.encode(VERSION, List.of(), List.of(table(Optional.of(
                new SegmentRef(SpaceId.of(1024), 3, SegmentId.of(13))))));
        byte[] payload = encoded.getFirst().payload();
        int tailOffset = payload.length - (1 + Integer.BYTES + Long.BYTES);

        byte[] unknown = Arrays.copyOf(payload, payload.length);
        unknown[tailOffset] = 2;
        byte[] truncated = Arrays.copyOf(payload, payload.length - 1);
        byte[] trailing = Arrays.copyOf(payload, payload.length + 1);
        trailing[trailing.length - 1] = 42;

        assertThrows(DictionaryCatalogCorruptionException.class,
                () -> codec.decode(batchWithTablePayload(encoded, unknown)));
        assertThrows(DictionaryCatalogCorruptionException.class,
                () -> codec.decode(batchWithTablePayload(encoded, truncated)));
        assertThrows(DictionaryCatalogCorruptionException.class,
                () -> codec.decode(batchWithTablePayload(encoded, trailing)));
    }

    private static TableDefinition table(Optional<SegmentRef> lob) {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        ColumnDefinition body = new ColumnDefinition(2, ObjectName.of("body"),
                new ColumnTypeDefinition(DictionaryTypeId.TEXT, false, true, 65_535, 0, 1, 1, List.of()), 1);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        SpaceId spaceId = SpaceId.of(1024);
        SegmentRef leaf = new SegmentRef(spaceId, 1, SegmentId.of(11));
        SegmentRef nonLeaf = new SegmentRef(spaceId, 2, SegmentId.of(12));
        TableStorageBinding binding = new TableStorageBinding(2, spaceId, Path.of("catalog_lob_1024.ibd"),
                List.of(new IndexStorageBinding(3, PageId.of(spaceId, PageNo.of(64)), 0, leaf, nonLeaf)), lob);
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"), VERSION,
                TableState.ACTIVE, List.of(id, body), List.of(primary), Optional.of(binding));
    }

    /** 独立重建扩展前的固定字段顺序，防止测试从待测 encoder 派生所谓 golden。 */
    private static byte[] legacyTablePayload(TableDefinition table) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                TableStorageBinding binding = table.storageBinding().orElseThrow();
                out.writeLong(table.id().value());
                out.writeLong(table.schemaId().value());
                writeName(out, table.name());
                out.writeLong(table.version().value());
                out.writeByte(table.state().ordinal());
                out.writeInt(table.columns().size());
                out.writeInt(table.indexes().size());
                out.writeBoolean(true);
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
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    /** 替换单 chunk table payload 后重算 manifest，使测试只观察 table decoder。 */
    private static CatalogBatch batchWithTablePayload(List<CatalogRecord> encoded, byte[] tablePayload) {
        List<CatalogRecord> records = new ArrayList<>(encoded);
        CatalogRecord originalTable = records.getFirst();
        records.set(0, new CatalogRecord(originalTable.key(), tablePayload));
        CatalogRecord oldCommit = records.getLast();
        MessageDigest digest = sha256();
        for (int i = 0; i < records.size() - 1; i++) {
            updateDigest(digest, records.get(i));
        }
        byte[] manifest = ByteBuffer.allocate(Integer.BYTES + 32).order(ByteOrder.BIG_ENDIAN)
                .putInt(records.size() - 1).put(digest.digest()).array();
        records.set(records.size() - 1, new CatalogRecord(oldCommit.key(), manifest));
        return new CatalogBatch(1, records);
    }

    private static void writeName(DataOutputStream out, ObjectName name) throws IOException {
        writeString(out, name.displayName());
        writeString(out, name.canonicalName());
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeSegment(DataOutputStream out, SegmentRef segment) throws IOException {
        out.writeInt(segment.inodeSlot());
        out.writeLong(segment.segmentId().value());
    }

    private static void updateDigest(MessageDigest digest, CatalogRecord record) {
        byte[] key = record.key();
        byte[] payload = record.payload();
        digest.update(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(key.length).array());
        digest.update(key);
        digest.update(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(payload.length).array());
        digest.update(payload);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
