package cn.zhangyis.db.storage.api.ddl;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.sdi.SdiPageLayout;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 物理 DDL TDD：CREATE 必须产生真实 GENERAL tablespace/INDEX root，DROP 必须排空并删除文件。 */
class TableDdlStorageServiceTest {

    @TempDir
    Path directory;

    /** CREATE 为聚簇与二级索引分别创建持久 root/binding；DROP 后文件和运行时准入都消失。 */
    @Test
    void createsIndexRootAndDropsTablespaceThroughSafeLifecycleBoundary() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("app_orders_1024.ibd");
        try {
            StorageTableDefinition definition = new StorageTableDefinition(2, SpaceId.of(1024), tableFile,
                    2, PageNo.of(128),
                    List.of(new StorageColumnDefinition(1, "id", 0,
                            StorageColumnType.bigint(false, false))),
                    List.of(new StorageIndexDefinition(3, "PRIMARY", true, true,
                                    List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                            new StorageIndexDefinition(4, "idx_id", false, false,
                                    List.of(new StorageIndexKeyPart(1, StorageIndexOrder.DESC, 0)))));

            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition);

            assertTrue(Files.exists(tableFile));
            assertEquals(SpaceId.of(1024), binding.spaceId());
            assertEquals(2, binding.indexes().size());
            assertTrue(binding.lobSegment().isEmpty(), "没有 LOB-capable 列时不得浪费 inode 槽");
            assertEquals(3, binding.indexes().getFirst().indexId());
            MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
            assertEquals(3, engine.indexPageAccess().openIndexPage(read,
                    binding.indexes().getFirst().rootPageId(), PageLatchMode.SHARED).header().indexId());
            assertEquals(4, engine.indexPageAccess().openIndexPage(read,
                    binding.indexes().get(1).rootPageId(), PageLatchMode.SHARED).header().indexId());
            engine.miniTransactionManager().commit(read);

            engine.tableDdlStorageService().dropTable(binding, Duration.ofSeconds(5));

            assertFalse(Files.exists(tableFile));
        } finally {
            engine.close();
        }
    }

    /**
     * 物理 CREATE 必须格式化固定 page3 并让 page0 root 指向它；DD 通过 opaque storage API 写入后，
     * table/version/payload 必须 durable 可读，不能把 DD 类型泄漏到 storage 页仓储。
     */
    @Test
    void persistsSerializedDictionaryInformationOnFixedPageThree() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("sdi_1028.ibd");
        try {
            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition(
                    8, SpaceId.of(1028), tableFile));
            assertTrue(engine.tableDdlStorageService().readSerializedDictionaryInfo(binding).isEmpty(),
                    "物理 CREATE 只格式化 SDI 页，ACTIVE DD 发布前不能伪造快照");

            byte[] payload = "complete-dd-table-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            SerializedDictionaryInfo expected = new SerializedDictionaryInfo(8, 11, payload);
            engine.tableDdlStorageService().writeSerializedDictionaryInfo(
                    binding, expected, Duration.ofSeconds(5));
            SerializedDictionaryInfo actual = engine.tableDdlStorageService()
                    .readSerializedDictionaryInfo(binding).orElseThrow();

            assertEquals(expected.tableId(), actual.tableId());
            assertEquals(expected.dictionaryVersion(), actual.dictionaryVersion());
            assertArrayEquals(expected.payload(), actual.payload());
            ByteBuffer page0 = readPage(tableFile, 0);
            ByteBuffer page3 = readPage(tableFile, SdiPageLayout.PAGE_NO);
            assertEquals(SdiPageLayout.PAGE_NO, page0.getLong(SpaceHeaderLayout.SDI_ROOT));
            assertEquals(PageType.SDI.code(), page3.getInt(PageEnvelopeLayout.PAGE_TYPE));
            assertEquals(SdiPageLayout.MAGIC, page3.getInt(SdiPageLayout.MAGIC_OFFSET));
            assertEquals(SdiPageLayout.FORMAT_VERSION, page3.getInt(SdiPageLayout.FORMAT_OFFSET));
        } finally {
            engine.close();
        }
    }

    /**
     * 新索引的 segment/root 与 page3 build descriptor 必须在同一物理提交中出现；后续 SDI 覆盖不能
     * 擦除 descriptor，只有显式 clear 才能结束 recovery 对 staged 资源的所有权。
     */
    @Test
    void persistsSecondaryIndexBuildDescriptorAcrossSdiRewriteUntilExplicitClear() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("index_build_1029.ibd");
        try {
            TableStorageBinding table = engine.tableDdlStorageService().createTable(definition(
                    9, SpaceId.of(1029), tableFile));

            SecondaryIndexBuildDescriptor staged = engine.tableDdlStorageService().beginSecondaryIndexBuild(
                    table, 41, 12, new StorageIndexDefinition(
                            23, "idx_id", false, false,
                            List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                    Duration.ofSeconds(5));

            assertEquals(41, staged.ddlOperationId());
            assertEquals(12, staged.dictionaryVersion());
            assertEquals(9, staged.tableId());
            assertEquals(23, staged.indexBinding().indexId());
            assertEquals(Optional.of(staged),
                    engine.tableDdlStorageService().readSecondaryIndexBuild(table));
            MiniTransaction inspect = engine.miniTransactionManager().beginReadOnly();
            assertEquals(23, engine.indexPageAccess().openIndexPage(
                    inspect, staged.indexBinding().rootPageId(), PageLatchMode.SHARED).header().indexId());
            engine.miniTransactionManager().commit(inspect);

            engine.tableDdlStorageService().writeSerializedDictionaryInfo(
                    table, new SerializedDictionaryInfo(9, 12, new byte[]{1, 2, 3}),
                    Duration.ofSeconds(5));
            assertEquals(Optional.of(staged),
                    engine.tableDdlStorageService().readSecondaryIndexBuild(table),
                    "SDI publish 早于 DD commit，不能丢失崩溃回收证据");

            engine.tableDdlStorageService().clearSecondaryIndexBuild(table, staged, Duration.ofSeconds(5));
            assertTrue(engine.tableDdlStorageService().readSecondaryIndexBuild(table).isEmpty());
        } finally {
            engine.close();
        }
    }

    /**
     * backfill 必须从聚簇 live rows 投影真实紧凑 entry，并返回实际 root level；不能只创建一棵空树后发布 DD。
     */
    @Test
    void backfillsSecondaryTreeFromExistingClusteredRows() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("index_backfill_1030.ibd");
        try {
            StorageIndexDefinition primary = new StorageIndexDefinition(
                    31, "PRIMARY", true, true,
                    List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)));
            StorageIndexDefinition secondary = new StorageIndexDefinition(
                    32, "idx_tenant", false, false,
                    List.of(new StorageIndexKeyPart(2, StorageIndexOrder.ASC, 0)));
            List<StorageColumnDefinition> columns = List.of(
                    new StorageColumnDefinition(1, "id", 0,
                            StorageColumnType.bigint(false, false)),
                    new StorageColumnDefinition(2, "tenant", 1,
                            StorageColumnType.bigint(false, false)));
            StorageTableDefinition before = new StorageTableDefinition(
                    10, SpaceId.of(1030), tableFile, 2, PageNo.of(128), columns, List.of(primary));
            TableStorageBinding binding = engine.tableDdlStorageService().createTable(before);
            var primaryIndex = new BTreeIndexMetadataFactory().createTable(before, binding).clusteredIndex();
            for (long id = 1; id <= 3; id++) {
                MiniTransaction insert = engine.miniTransactionManager().begin(
                        engine.miniTransactionManager().budgetFor(
                                RedoBudgetPurpose.CLUSTERED_INSERT,
                                BTreeRedoBudgetEstimator.insert(primaryIndex.rootLevel())));
                engine.btreeService().insertClustered(insert, primaryIndex,
                        new LogicalRecord(2, List.of(
                                new ColumnValue.IntValue(id),
                                new ColumnValue.IntValue(id == 3 ? 8 : 7)),
                                false, RecordType.CONVENTIONAL),
                        TransactionId.of(id), RollPointer.NULL);
                engine.miniTransactionManager().commit(insert);
            }
            SecondaryIndexBuildDescriptor staged = engine.tableDdlStorageService().beginSecondaryIndexBuild(
                    binding, 51, 13, secondary, Duration.ofSeconds(5));
            StorageTableDefinition during = new StorageTableDefinition(
                    10, SpaceId.of(1030), tableFile, 2, PageNo.of(128),
                    columns, List.of(primary, secondary));

            IndexStorageBinding completed = engine.tableDdlStorageService().backfillSecondaryIndex(
                    during, binding, staged, Duration.ofSeconds(5));

            TableStorageBinding finalBinding = new TableStorageBinding(
                    binding.tableId(), binding.spaceId(), binding.path(), binding.rowFormatVersion(),
                    List.of(binding.indexes().getFirst(), completed), binding.lobSegment());
            var secondaryMetadata = new BTreeIndexMetadataFactory()
                    .createTable(during, finalBinding).requireSecondary(32);
            MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
            List<LogicalRecord> entries = engine.btreeService()
                    .scanAll(read, secondaryMetadata.index(), 10).stream()
                    .map(result -> result.record()).toList();
            engine.miniTransactionManager().commit(read);

            assertEquals(3, entries.size());
            assertEquals(List.of(7L, 7L, 8L), entries.stream()
                    .map(entry -> ((ColumnValue.IntValue) entry.columnValues().getFirst()).value())
                    .toList());
        } finally {
            engine.close();
        }
    }

    /**
     * UNIQUE backfill 遇到重复非 NULL logical key 时不得发布成功；descriptor 必须保留到显式物理回滚完成。
     */
    @Test
    void rejectsDuplicateUniqueBackfillAndRollsBackStagedSegments() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("unique_index_backfill_1031.ibd");
        try {
            StorageIndexDefinition primary = new StorageIndexDefinition(
                    41, "PRIMARY", true, true,
                    List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)));
            StorageIndexDefinition unique = new StorageIndexDefinition(
                    42, "uk_tenant", true, false,
                    List.of(new StorageIndexKeyPart(2, StorageIndexOrder.ASC, 0)));
            List<StorageColumnDefinition> columns = List.of(
                    new StorageColumnDefinition(1, "id", 0,
                            StorageColumnType.bigint(false, false)),
                    new StorageColumnDefinition(2, "tenant", 1,
                            StorageColumnType.bigint(false, false)));
            StorageTableDefinition before = new StorageTableDefinition(
                    11, SpaceId.of(1031), tableFile, 2, PageNo.of(128), columns, List.of(primary));
            TableStorageBinding binding = engine.tableDdlStorageService().createTable(before);
            var primaryIndex = new BTreeIndexMetadataFactory().createTable(before, binding).clusteredIndex();
            for (long id = 1; id <= 2; id++) {
                MiniTransaction insert = engine.miniTransactionManager().begin(
                        engine.miniTransactionManager().budgetFor(
                                RedoBudgetPurpose.CLUSTERED_INSERT,
                                BTreeRedoBudgetEstimator.insert(primaryIndex.rootLevel())));
                engine.btreeService().insertClustered(insert, primaryIndex,
                        new LogicalRecord(2, List.of(
                                new ColumnValue.IntValue(id), new ColumnValue.IntValue(7)),
                                false, RecordType.CONVENTIONAL),
                        TransactionId.of(id), RollPointer.NULL);
                engine.miniTransactionManager().commit(insert);
            }
            SecondaryIndexBuildDescriptor staged = engine.tableDdlStorageService().beginSecondaryIndexBuild(
                    binding, 61, 14, unique, Duration.ofSeconds(5));
            StorageTableDefinition during = new StorageTableDefinition(
                    11, SpaceId.of(1031), tableFile, 2, PageNo.of(128),
                    columns, List.of(primary, unique));

            assertThrows(SecondaryIndexBuildDuplicateKeyException.class,
                    () -> engine.tableDdlStorageService().backfillSecondaryIndex(
                            during, binding, staged, Duration.ofSeconds(5)));
            assertEquals(Optional.of(staged),
                    engine.tableDdlStorageService().readSecondaryIndexBuild(binding));

            engine.tableDdlStorageService().rollbackSecondaryIndexBuild(
                    binding, staged, Duration.ofSeconds(5));

            assertTrue(engine.tableDdlStorageService().readSecondaryIndexBuild(binding).isEmpty());
        } finally {
            engine.close();
        }
    }

    /** 多个 LOB-capable 列共享一个表级 LOB segment；其 identity/purpose 必须与索引 segment 隔离。 */
    @Test
    void createsExactlyOneDedicatedLobSegmentForLobCapableTable() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("lob_orders_1027.ibd");
        try {
            StorageTableDefinition definition = new StorageTableDefinition(7, SpaceId.of(1027), tableFile,
                    2, PageNo.of(128),
                    List.of(new StorageColumnDefinition(1, "id", 0,
                                    StorageColumnType.bigint(false, false)),
                            new StorageColumnDefinition(2, "description", 1,
                                    lobType(StorageColumnTypeId.TEXT, 65_535)),
                            new StorageColumnDefinition(3, "document", 2,
                                    lobType(StorageColumnTypeId.JSON, Integer.MAX_VALUE))),
                    List.of(new StorageIndexDefinition(17, "PRIMARY", true, true,
                            List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)))));

            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition);
            SegmentRef lob = binding.lobSegment().orElseThrow();
            IndexStorageBinding primary = binding.indexes().getFirst();

            assertEquals(binding.spaceId(), lob.spaceId());
            assertNotEquals(primary.leafSegment(), lob);
            assertNotEquals(primary.nonLeafSegment(), lob);
            MiniTransaction inspect = engine.miniTransactionManager().beginReadOnly();
            engine.diskSpaceManager().requireSegmentPurposeForWrite(inspect, lob, SegmentPurpose.LOB);
            engine.miniTransactionManager().commit(inspect);

            engine.tableDdlStorageService().dropTable(binding, Duration.ofSeconds(5));
            assertFalse(Files.exists(tableFile), "DROP 仍以整个 tablespace 为物理回收边界");
        } finally {
            engine.close();
        }
    }

    /** durable DISCARDED 之后崩溃必须在 page0 留下可解释意图，重试 DROP 可跳过 marker 并完成删文件。 */
    @Test
    void resumesDropAfterDurableDiscardedMarker() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("discarded_1025.ibd");
        try {
            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition(
                    5, SpaceId.of(1025), tableFile));

            assertThrows(TableDdlStorageException.class, () -> engine.tableDdlStorageService().dropTable(
                    binding, Duration.ofSeconds(5), ignored -> {
                        throw new TableDdlStorageException("injected crash after durable DISCARDED");
                    }));

            assertTrue(Files.exists(tableFile));
            assertEquals(TablespaceState.DISCARDED, lifecycle(tableFile).state());
            engine.tableDdlStorageService().dropTable(binding, Duration.ofSeconds(5));
            assertFalse(Files.exists(tableFile));
        } finally {
            engine.close();
        }
    }

    /** catalog binding 路径必须与已打开 space 的真实文件一致，否则不得标记/关闭错误对象。 */
    @Test
    void rejectsDropBindingWhosePathDoesNotMatchOpenTablespace() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("bound_1026.ibd");
        try {
            TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition(
                    6, SpaceId.of(1026), tableFile));
            TableStorageBinding mismatched = new TableStorageBinding(binding.tableId(), binding.spaceId(),
                    directory.resolve("other_1026.ibd"), binding.rowFormatVersion(),
                    binding.indexes(), binding.lobSegment());

            assertThrows(TableDdlStorageException.class, () -> engine.tableDdlStorageService()
                    .dropTable(mismatched, Duration.ofSeconds(5)));
            assertThrows(TableDdlStorageException.class, () -> engine.tableDdlStorageService()
                    .readSerializedDictionaryInfo(mismatched));
            assertThrows(TableDdlStorageException.class, () -> engine.tableDdlStorageService()
                    .writeSerializedDictionaryInfo(mismatched,
                            new SerializedDictionaryInfo(binding.tableId(), 3, new byte[]{1}),
                            Duration.ofSeconds(5)));
            assertTrue(Files.exists(tableFile));

            engine.tableDdlStorageService().dropTable(binding, Duration.ofSeconds(5));
        } finally {
            engine.close();
        }
    }

    private static StorageTableDefinition definition(long tableId, SpaceId spaceId, Path path) {
        return new StorageTableDefinition(tableId, spaceId, path, 2, PageNo.of(128),
                List.of(new StorageColumnDefinition(1, "id", 0,
                        StorageColumnType.bigint(false, false))),
                List.of(new StorageIndexDefinition(tableId + 10, "PRIMARY", true, true,
                        List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)))));
    }

    /** 构造 storage DTO 的 LOB 类型；长度语义与 Record ColumnType 保持一致。 */
    private static StorageColumnType lobType(StorageColumnTypeId typeId, int maxBytes) {
        return new StorageColumnType(typeId, true, maxBytes, 0, false, 1, 1, List.of());
    }

    private static cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader lifecycle(Path path) {
        ByteBuffer page = readPage(path, 0);
        return TablespaceLifecycleRawCodec.read(page).orElseThrow();
    }

    /** 从已 force 的测试表空间完整读取一页，用于核对稳定物理字段而不借用待测 buffer 状态。 */
    private static ByteBuffer readPage(Path path, long pageNo) {
        ByteBuffer page = ByteBuffer.allocate(16 * 1024);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long position = pageNo * page.capacity();
            while (page.hasRemaining()) {
                int read = channel.read(page, position);
                if (read < 0) {
                    throw new AssertionError("unexpected EOF while reading tablespace page " + pageNo);
                }
                position += read;
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return page;
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }
}
