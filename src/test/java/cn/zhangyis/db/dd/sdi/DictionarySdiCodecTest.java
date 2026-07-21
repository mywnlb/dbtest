package cn.zhangyis.db.dd.sdi;

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
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SDI payload TDD：完整 table 聚合必须确定性往返，未知格式、截断和尾随字节必须 fail-closed。
 */
class DictionarySdiCodecTest {

    /**
     * 包含 LOB、ENUM symbols、DESC prefix 和多索引 binding 的聚合必须无损往返；
     * 相同不可变定义重复编码还必须得到完全相同的字节。
     */
    @Test
    void roundTripsCompleteTableAggregateDeterministically() {
        DictionarySdiCodec codec = new DictionarySdiCodec();
        TableDefinition expected = table();

        byte[] first = codec.encode(expected);
        byte[] second = codec.encode(expected);
        TableDefinition decoded = codec.decode(first);

        assertArrayEquals(first, second);
        assertEquals(expected, decoded);
    }

    /**
     * format version 是显式迁移边界；未知版本、截断和尾随数据都不能被当成可跳过扩展，
     * 防止 recovery 用部分 table definition 覆盖 committed DD。
     */
    @Test
    void rejectsUnknownVersionTruncationAndTrailingBytes() {
        DictionarySdiCodec codec = new DictionarySdiCodec();
        byte[] encoded = codec.encode(table());
        byte[] unknown = Arrays.copyOf(encoded, encoded.length);
        ByteBuffer.wrap(unknown).order(ByteOrder.BIG_ENDIAN).putInt(Integer.BYTES, 99);
        byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);
        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        trailing[trailing.length - 1] = 42;

        assertThrows(DictionarySdiCorruptionException.class, () -> codec.decode(unknown));
        assertThrows(DictionarySdiCorruptionException.class, () -> codec.decode(truncated));
        assertThrows(DictionarySdiCorruptionException.class, () -> codec.decode(trailing));
    }

    /**
     * SDI v2 必须保存独立物理行格式版本，重启后不能把较新的 DD 版本误当成既有 record schema version。
     */
    @Test
    void roundTripsIndependentPhysicalRowFormatVersion() {
        TableDefinition current = table();
        TableStorageBinding binding = current.storageBinding().orElseThrow();
        TableStorageBinding metadataOnly = new TableStorageBinding(
                binding.tableId(), binding.spaceId(), binding.path(), 4,
                binding.indexes(), binding.lobSegment());
        TableDefinition expected = new TableDefinition(
                current.id(), current.schemaId(), current.name(), current.version(), current.state(),
                current.columns(), current.indexes(), Optional.of(metadataOnly));

        TableDefinition decoded = new DictionarySdiCodec().decode(new DictionarySdiCodec().encode(expected));

        assertEquals(9, decoded.version().value());
        assertEquals(4, decoded.storageBinding().orElseThrow().rowFormatVersion());
    }

    /** v1 SDI 没有独立 row format version；升级 decoder 必须从当时 table version 精确派生。 */
    @Test
    void decodesLegacyV1PayloadAndDerivesPhysicalRowFormatVersion() {
        DictionarySdiCodec codec = new DictionarySdiCodec();
        TableDefinition expected = table();
        byte[] v1 = legacyV1Payload(expected);

        TableDefinition decoded = codec.decode(v1);

        assertEquals(expected, decoded);
        assertEquals(expected.version().value(),
                decoded.storageBinding().orElseThrow().rowFormatVersion());
    }

    /** v3 必须同时保存 table options 与每列 default，不允许只在 catalog 中存在。 */
    @Test
    void roundTripsOptionsAndColumnDefaultsInV3() {
        TableDefinition base = table();
        List<ColumnDefinition> columns = List.of(
                new ColumnDefinition(base.columns().get(0).columnId(), base.columns().get(0).name(),
                        base.columns().get(0).type(), 0, ColumnDefaultDefinition.required()),
                new ColumnDefinition(base.columns().get(1).columnId(), base.columns().get(1).name(),
                        base.columns().get(1).type(), 1,
                        ColumnDefaultDefinition.constant("'paid'")),
                base.columns().get(2));
        TableDefinition expected = new TableDefinition(
                base.id(), base.schemaId(), base.name(), base.version(), base.state(),
                columns, base.indexes(), base.storageBinding(),
                new TableOptions("订单表", 45, 255));

        TableDefinition decoded =
                new DictionarySdiCodec().decode(new DictionarySdiCodec().encode(expected));

        assertEquals(expected, decoded);
    }

    private static TableDefinition table() {
        ColumnDefinition id = new ColumnDefinition(11, ObjectName.of("Id"),
                ColumnTypeDefinition.bigint(true, false), 0);
        ColumnDefinition state = new ColumnDefinition(12, ObjectName.of("State"),
                new ColumnTypeDefinition(DictionaryTypeId.ENUM, false, true,
                        8, 0, 45, 255, List.of("new", "paid", "closed")), 1);
        ColumnDefinition body = new ColumnDefinition(13, ObjectName.of("Body"),
                new ColumnTypeDefinition(DictionaryTypeId.TEXT, false, true,
                        65_535, 0, 45, 255, List.of()), 2);
        IndexDefinition primary = new IndexDefinition(IndexId.of(21), ObjectName.of("PRIMARY"),
                true, true, List.of(new IndexKeyPart(11, IndexOrder.ASC, 0)));
        IndexDefinition secondary = new IndexDefinition(IndexId.of(22), ObjectName.of("idx_state"),
                false, false, List.of(new IndexKeyPart(12, IndexOrder.DESC, 4)));

        SpaceId spaceId = SpaceId.of(1024);
        SegmentRef primaryLeaf = segment(spaceId, 0, 31);
        SegmentRef primaryNonLeaf = segment(spaceId, 1, 32);
        SegmentRef secondaryLeaf = segment(spaceId, 2, 33);
        SegmentRef secondaryNonLeaf = segment(spaceId, 3, 34);
        SegmentRef lob = segment(spaceId, 4, 35);
        TableStorageBinding binding = new TableStorageBinding(7, spaceId,
                Path.of("build/sdi/table_7_space_1024.ibd"), 9,
                List.of(
                        new IndexStorageBinding(21, PageId.of(spaceId, PageNo.of(64)), 0,
                                primaryLeaf, primaryNonLeaf),
                        new IndexStorageBinding(22, PageId.of(spaceId, PageNo.of(65)), 1,
                                secondaryLeaf, secondaryNonLeaf)),
                Optional.of(lob));
        return new TableDefinition(TableId.of(7), SchemaId.of(3), ObjectName.of("Orders"),
                DictionaryVersion.of(9), TableState.ACTIVE,
                List.of(id, state, body), List.of(primary, secondary), Optional.of(binding));
    }

    private static SegmentRef segment(SpaceId spaceId, int inodeSlot, long segmentId) {
        return new SegmentRef(spaceId, inodeSlot, SegmentId.of(segmentId));
    }

    /** 按持久字段顺序定位 v2 binding 尾部的 rowFormatVersion，用于生成独立 v1 shape。 */
    private static int rowFormatOffset(byte[] payload) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            in.readInt();
            in.readInt();
            in.readLong();
            in.readLong();
            skipString(in);
            skipString(in);
            in.readLong();
            in.readByte();
            in.readLong();
            in.readInt();
            skipString(in);
            int indexes = in.readInt();
            for (int i = 0; i < indexes; i++) {
                in.skipNBytes(Long.BYTES + Long.BYTES + Integer.BYTES
                        + Integer.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES);
            }
            if (in.readBoolean()) {
                in.skipNBytes(Integer.BYTES + Long.BYTES);
            }
            return payload.length - in.available();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void skipString(DataInputStream in) throws IOException {
        int length = in.readInt();
        in.skipNBytes(length);
    }

    /**
     * 独立写出历史 v1 shape：无 table options、独立 rowFormatVersion 与 column default。
     * 测试不从当前 encoder 切片，避免当前字段顺序错误同时污染兼容 golden。
     */
    private static byte[] legacyV1Payload(TableDefinition table) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                TableStorageBinding binding = table.storageBinding().orElseThrow();
                out.writeInt(0x44445331);
                out.writeInt(1);
                out.writeLong(table.id().value());
                out.writeLong(table.schemaId().value());
                writeName(out, table.name());
                out.writeLong(table.version().value());
                out.writeByte(1);
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
                out.writeInt(table.columns().size());
                for (ColumnDefinition column : table.columns()) {
                    writeLegacyColumn(out, column);
                }
                out.writeInt(table.indexes().size());
                for (IndexDefinition index : table.indexes()) {
                    out.writeLong(index.id().value());
                    writeName(out, index.name());
                    out.writeBoolean(index.unique());
                    out.writeBoolean(index.clustered());
                    out.writeInt(index.keyParts().size());
                    for (IndexKeyPart part : index.keyParts()) {
                        out.writeLong(part.columnId());
                        out.writeByte(part.order() == IndexOrder.ASC ? 1 : 2);
                        out.writeInt(part.prefixBytes());
                    }
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    /** 写出不含 default 尾部的历史列 shape。 */
    private static void writeLegacyColumn(DataOutputStream out, ColumnDefinition column)
            throws IOException {
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
}
