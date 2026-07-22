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
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;
import cn.zhangyis.db.storage.sdi.SdiPageLayout;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
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
import java.util.ArrayList;
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
     * 多ADD/DROP必须由一个ALT anchor和一个versioned descriptor chain共同拥有；SDI重写不得清除该owner。
     */
    @Test
    void persistsOneDescriptorSetForMultipleOrderedIndexActions() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("general_alter_descriptor_1090.ibd");
        try {
            StorageTableDefinition sourceDefinition =
                    twoIndexDefinition(90, SpaceId.of(1090), tableFile, 50, 51);
            TableStorageBinding table = engine.tableDdlStorageService().createTable(
                    sourceDefinition);
            byte[] digest = new byte[32];
            java.util.Arrays.fill(digest, (byte) 7);
            List<OnlineAlterIndexAddRequest> additions = List.of(
                    new OnlineAlterIndexAddRequest(0, secondary(60, "idx_a")),
                    new OnlineAlterIndexAddRequest(4, secondary(61, "idx_b")));

            OnlineAlterDescriptorSet staged = engine.tableDdlStorageService()
                    .beginOnlineAlterIndexDescriptors(table, 801, 91, 3,
                            additions,
                            List.of(new OnlineAlterIndexDropRequest(
                                    2, table.indexes().get(1))),
                            digest, Duration.ofSeconds(5));

            staged = engine.tableDdlStorageService().backfillOnlineAlterIndexes(
                    sourceDefinition, table, staged, additions, Duration.ofSeconds(5));

            assertEquals(List.of(0, 2, 4), staged.descriptors().stream()
                    .map(OnlineAlterIndexDescriptor::actionOrdinal).toList());
            assertEquals(List.of(OnlineAlterIndexDescriptorAction.ADD,
                            OnlineAlterIndexDescriptorAction.DROP,
                            OnlineAlterIndexDescriptorAction.ADD),
                    staged.descriptors().stream()
                            .map(OnlineAlterIndexDescriptor::action).toList());
            assertEquals(Optional.of(staged).map(OnlineAlterDescriptorSet::ddlOperationId),
                    engine.tableDdlStorageService().readOnlineAlterDescriptorSet(table)
                            .map(OnlineAlterDescriptorSet::ddlOperationId));

            engine.tableDdlStorageService().writeSerializedDictionaryInfo(table,
                    new SerializedDictionaryInfo(90, 91, new byte[]{9, 1}),
                    Duration.ofSeconds(5));
            OnlineAlterDescriptorSet reread = engine.tableDdlStorageService()
                    .readOnlineAlterDescriptorSet(table).orElseThrow();
            assertArrayEquals(digest, reread.manifestDigest());
            assertEquals(staged.descriptors(), reread.descriptors());

            assertEquals(PageType.DDL_DESCRIPTOR,
                    PageType.fromCode(readPage(tableFile,
                            staged.descriptorPages().getFirst().pageNo().value())
                            .getInt(PageEnvelopeLayout.PAGE_TYPE)));

            IndexStorageBinding added = staged.descriptors().getFirst().indexBinding();
            IndexStorageBinding retainedDrop = table.indexes().get(1);
            engine.tableDdlStorageService().rollbackOnlineAlterIndexDescriptors(
                    table, reread, Duration.ofSeconds(5));
            assertTrue(engine.tableDdlStorageService()
                    .readOnlineAlterDescriptorSet(table).isEmpty());
            MiniTransaction removedAdd = engine.miniTransactionManager().beginReadOnly();
            assertThrows(FspMetadataException.class, () ->
                    engine.diskSpaceManager().inspectDropSegmentPlan(
                            removedAdd, added.leafSegment()));
            engine.miniTransactionManager().rollbackUncommitted(removedAdd);
            MiniTransaction retainedSource = engine.miniTransactionManager().beginReadOnly();
            assertEquals(retainedDrop.indexId(), engine.indexPageAccess().openIndexPage(
                    retainedSource, retainedDrop.rootPageId(), PageLatchMode.SHARED)
                    .header().indexId());
            engine.miniTransactionManager().commit(retainedSource);
        } finally {
            engine.close();
        }
    }

    /**
     * DROP INDEX 在字典提交前只持久化 descriptor；若上层仍以旧 DD 裁决回滚，必须只清 footer，
     * 原 root 与两个 segment 继续有效。
     */
    @Test
    void rollsBackSecondaryIndexDropWithoutReclaimingReferencedSegments() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("index_drop_rollback_1032.ibd");
        try {
            StorageTableDefinition definition = twoIndexDefinition(
                    12, SpaceId.of(1032), tableFile, 51, 52);
            TableStorageBinding table = engine.tableDdlStorageService().createTable(definition);
            IndexStorageBinding secondary = table.indexes().get(1);

            SecondaryIndexDropDescriptor staged =
                    engine.tableDdlStorageService().beginSecondaryIndexDrop(
                            table, 71, 15, secondary, Duration.ofSeconds(5));

            assertEquals(Optional.of(staged),
                    engine.tableDdlStorageService().readSecondaryIndexDrop(table));
            engine.tableDdlStorageService().rollbackSecondaryIndexDrop(
                    table, staged, Duration.ofSeconds(5));

            assertTrue(engine.tableDdlStorageService().readSecondaryIndexDrop(table).isEmpty());
            MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
            assertEquals(secondary.indexId(), engine.indexPageAccess().openIndexPage(
                    read, secondary.rootPageId(), PageLatchMode.SHARED).header().indexId());
            engine.miniTransactionManager().commit(read);
            MiniTransaction readLeaf = engine.miniTransactionManager().beginReadOnly();
            engine.diskSpaceManager().requireSegmentPurposeForWrite(
                    readLeaf, secondary.leafSegment(), SegmentPurpose.INDEX_LEAF);
            engine.miniTransactionManager().commit(readLeaf);
            MiniTransaction readNonLeaf = engine.miniTransactionManager().beginReadOnly();
            engine.diskSpaceManager().requireSegmentPurposeForWrite(
                    readNonLeaf, secondary.nonLeafSegment(), SegmentPurpose.INDEX_NON_LEAF);
            engine.miniTransactionManager().commit(readNonLeaf);
        } finally {
            engine.close();
        }
    }

    /**
     * 新 DD 已移除目标索引后，finish 必须在同一 MTR 回收 leaf/non-leaf segment 并清 footer；
     * 旧 segment handle 随后必须因 inode identity 消失而 fail-closed。
     */
    @Test
    void finishesSecondaryIndexDropByReclaimingBothSegmentsAndDescriptor() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path tableFile = directory.resolve("index_drop_finish_1033.ibd");
        try {
            StorageTableDefinition definition = twoIndexDefinition(
                    13, SpaceId.of(1033), tableFile, 61, 62);
            TableStorageBinding oldBinding = engine.tableDdlStorageService().createTable(definition);
            IndexStorageBinding secondary = oldBinding.indexes().get(1);
            SecondaryIndexDropDescriptor staged =
                    engine.tableDdlStorageService().beginSecondaryIndexDrop(
                            oldBinding, 81, 16, secondary, Duration.ofSeconds(5));
            TableStorageBinding newBinding = new TableStorageBinding(
                    oldBinding.tableId(), oldBinding.spaceId(), oldBinding.path(),
                    oldBinding.rowFormatVersion(), List.of(oldBinding.indexes().getFirst()),
                    oldBinding.lobSegment());

            engine.tableDdlStorageService().finishSecondaryIndexDrop(
                    newBinding, staged, Duration.ofSeconds(5));

            assertTrue(engine.tableDdlStorageService().readSecondaryIndexDrop(newBinding).isEmpty());
            MiniTransaction readLeaf = engine.miniTransactionManager().beginReadOnly();
            assertThrows(FspMetadataException.class, () ->
                    engine.diskSpaceManager().inspectDropSegmentPlan(
                            readLeaf, secondary.leafSegment()));
            engine.miniTransactionManager().rollbackUncommitted(readLeaf);
            MiniTransaction readNonLeaf = engine.miniTransactionManager().beginReadOnly();
            assertThrows(FspMetadataException.class, () ->
                    engine.diskSpaceManager().inspectDropSegmentPlan(
                            readNonLeaf, secondary.nonLeafSegment()));
            engine.miniTransactionManager().rollbackUncommitted(readNonLeaf);
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

            OnlineIndexScanBatch firstBatch = engine.tableDdlStorageService()
                    .scanSecondaryIndexBuildBatch(during, binding, staged, Optional.empty(), 2);
            OnlineIndexScanBatch secondBatch = engine.tableDdlStorageService()
                    .scanSecondaryIndexBuildBatch(during, binding, staged,
                            firstBatch.continuation(), 2);

            assertEquals(2, firstBatch.rows().size());
            assertFalse(firstBatch.complete());
            assertTrue(firstBatch.continuation().isPresent());
            assertEquals(1, secondBatch.rows().size());
            assertTrue(secondBatch.complete());

            LogicalRecord sourceRow = new LogicalRecord(2, List.of(
                    new ColumnValue.IntValue(1), new ColumnValue.IntValue(7)),
                    false, RecordType.CONVENTIONAL);
            SecondaryIndexBuildDescriptor afterEnsure = engine.tableDdlStorageService()
                    .ensureSecondaryIndexLive(during, binding, staged, sourceRow);
            afterEnsure = engine.tableDdlStorageService()
                    .ensureSecondaryIndexLive(during, binding, afterEnsure, sourceRow);
            OnlineIndexEntryBatch stagedEntries = engine.tableDdlStorageService()
                    .scanSecondaryIndexBuildEntriesIncludingDeleted(
                            during, binding, afterEnsure, Optional.empty(), 10);
            assertEquals(1, stagedEntries.entries().size());
            assertFalse(stagedEntries.entries().getFirst().deleted());

            SecondaryIndexBuildDescriptor afterRemove = engine.tableDdlStorageService()
                    .removeSecondaryIndexEntryExact(during, binding, afterEnsure,
                            stagedEntries.entries().getFirst());
            assertTrue(engine.tableDdlStorageService()
                    .scanSecondaryIndexBuildEntriesIncludingDeleted(
                            during, binding, afterRemove, Optional.empty(), 10)
                    .entries().isEmpty());

            IndexStorageBinding completed = engine.tableDdlStorageService().backfillSecondaryIndex(
                    during, binding, afterRemove, Duration.ofSeconds(5));

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
            StorageTableDefinition during = new StorageTableDefinition(
                    11, SpaceId.of(1031), tableFile, 2, PageNo.of(128),
                    columns, List.of(primary, unique));

            SecondaryIndexBuildDescriptor scanStaged = engine.tableDdlStorageService()
                    .beginSecondaryIndexBuild(
                            binding, 61, 14, unique, Duration.ofSeconds(5));
            OnlineIndexScanBatch batch = engine.tableDdlStorageService()
                    .scanSecondaryIndexBuildBatch(
                            during, binding, scanStaged, Optional.empty(), 10);
            for (LogicalRecord row : batch.rows()) {
                scanStaged = engine.tableDdlStorageService()
                        .ensureSecondaryIndexLiveForBaseScan(
                                during, binding, scanStaged, row);
            }
            SecondaryIndexBuildDescriptor duplicateTarget = scanStaged;
            assertThrows(SecondaryIndexBuildDuplicateKeyException.class,
                    () -> engine.tableDdlStorageService().verifySecondaryIndexBuild(
                            during, binding, duplicateTarget, 1));
            engine.tableDdlStorageService().rollbackSecondaryIndexBuild(
                    binding, duplicateTarget, Duration.ofSeconds(5));

            SecondaryIndexBuildDescriptor staged = engine.tableDdlStorageService().beginSecondaryIndexBuild(
                    binding, 62, 15, unique, Duration.ofSeconds(5));

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

    /**
     * shadow rebuild 的源扫描必须跨过 256 行批次边界且不重不漏；continuation 使用上一批最后一条
     * 完整聚簇 physical key，而不是页号或不稳定 RecordRef。
     */
    @Test
    void rebuildsMoreThanOneContinuationBatchExactlyOnce() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path sourcePath = directory.resolve("rebuild_source_1040.ibd");
        Path targetPath = directory.resolve("rebuild_target_1041.ibd");
        try {
            StorageTableDefinition sourceDefinition =
                    definition(40, SpaceId.of(1040), sourcePath);
            TableStorageBinding sourceBinding =
                    engine.tableDdlStorageService().createTable(
                            sourceDefinition);
            var sourceIndex = new BTreeIndexMetadataFactory()
                    .createTable(sourceDefinition, sourceBinding)
                    .clusteredIndex();
            for (long id = 1; id <= 270; id++) {
                MiniTransaction insert =
                        engine.miniTransactionManager().begin(
                                engine.miniTransactionManager().budgetFor(
                                        RedoBudgetPurpose.CLUSTERED_INSERT,
                                        BTreeRedoBudgetEstimator.insert(
                                                sourceIndex.rootLevel())));
                var inserted = engine.btreeService().insertClustered(
                        insert, sourceIndex,
                        new LogicalRecord(
                                sourceDefinition.schemaVersion(),
                                List.of(new ColumnValue.IntValue(id)),
                                false, RecordType.CONVENTIONAL),
                        TransactionId.of(id), RollPointer.NULL);
                engine.miniTransactionManager().commit(insert);
                sourceIndex = inserted.indexAfterInsert();
            }

            StorageTableDefinition targetDefinition =
                    new StorageTableDefinition(
                            sourceDefinition.tableId(),
                            SpaceId.of(1041), targetPath, 3,
                            PageNo.of(128), sourceDefinition.columns(),
                            sourceDefinition.indexes());
            List<Long> observedBatchRows = new ArrayList<>();
            List<SearchKey> observedContinuations = new ArrayList<>();
            TableStorageBinding targetBinding =
                    engine.tableDdlStorageService().rebuildTableOnline(
                            new StorageTableRebuildRequest(
                                    sourceDefinition, sourceBinding,
                                    targetDefinition,
                                    List.of(StorageColumnRewrite.source(0))),
                            Duration.ofSeconds(10), (rowsInBatch, continuation) -> {
                                observedBatchRows.add(rowsInBatch);
                                observedContinuations.add(continuation);
                            });

            var targetIndex = new BTreeIndexMetadataFactory()
                    .createTable(targetDefinition, targetBinding)
                    .clusteredIndex();
            MiniTransaction read =
                    engine.miniTransactionManager().beginReadOnly();
            List<LogicalRecord> rows = engine.btreeService()
                    .scanAll(read, targetIndex, 300).stream()
                    .map(result -> result.record()).toList();
            engine.miniTransactionManager().commit(read);

            assertEquals(270, rows.size());
            assertEquals(List.of(256L, 14L), observedBatchRows);
            assertEquals(2, observedContinuations.size());
            assertEquals(270, rows.stream()
                    .map(row -> ((ColumnValue.IntValue)
                            row.columnValues().getFirst()).value())
                    .distinct().count());
            assertEquals(1L, ((ColumnValue.IntValue)
                    rows.getFirst().columnValues().getFirst()).value());
            assertEquals(270L, ((ColumnValue.IntValue)
                    rows.getLast().columnValues().getFirst()).value());
        } finally {
            engine.close();
        }
    }

    /**
     * Online shadow copy期间产生的UPDATE、DELETE和INSERT只记录聚簇identity；final reconciliation必须先删除
     * shadow旧像，再以source current truth重新投影整行和全部target secondary。重复消费同一identity仍须收敛。
     */
    @Test
    void reconcilesShadowFromCurrentSourceTruthIdempotently() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Path sourcePath = directory.resolve("online_shadow_source_1050.ibd");
        Path targetPath = directory.resolve("online_shadow_target_1051.ibd");
        try {
            StorageTableDefinition sourceDefinition = new StorageTableDefinition(
                    50, SpaceId.of(1050), sourcePath, 2, PageNo.of(128),
                    List.of(new StorageColumnDefinition(1, "id", 0,
                                    StorageColumnType.bigint(false, false)),
                            new StorageColumnDefinition(2, "value", 1,
                                    varchar(64))),
                    List.of(new StorageIndexDefinition(60, "PRIMARY", true, true,
                            List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)))));
            TableStorageBinding sourceBinding = engine.tableDdlStorageService()
                    .createTable(sourceDefinition);
            var sourceIndex = new BTreeIndexMetadataFactory()
                    .createTable(sourceDefinition, sourceBinding).clusteredIndex();
            sourceIndex = insertClustered(engine, sourceIndex, 1, "before");
            sourceIndex = insertClustered(engine, sourceIndex, 2, "deleted");

            StorageTableDefinition targetDefinition = new StorageTableDefinition(
                    50, SpaceId.of(1051), targetPath, 3, PageNo.of(128),
                    List.of(new StorageColumnDefinition(1, "id", 0,
                                    StorageColumnType.bigint(false, false)),
                            new StorageColumnDefinition(2, "value", 1,
                                    varchar(64)),
                            new StorageColumnDefinition(3, "generation", 2,
                                    StorageColumnType.bigint(false, false))),
                    List.of(new StorageIndexDefinition(60, "PRIMARY", true, true,
                                    List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                            new StorageIndexDefinition(61, "idx_value", false, false,
                                    List.of(new StorageIndexKeyPart(2, StorageIndexOrder.ASC, 0)))));
            StorageTableRebuildRequest request = new StorageTableRebuildRequest(
                    sourceDefinition, sourceBinding, targetDefinition,
                    List.of(StorageColumnRewrite.source(0), StorageColumnRewrite.source(1),
                            StorageColumnRewrite.added(Optional.of(
                                    new StorageDefaultValue.IntegerValue(
                                            java.math.BigInteger.valueOf(7))))));
            TableStorageBinding shadow = engine.tableDdlStorageService()
                    .rebuildTableOnline(request, Duration.ofSeconds(10));

            SearchKey one = new SearchKey(List.of(new ColumnValue.IntValue(1)));
            MiniTransaction readOne = engine.miniTransactionManager().beginReadOnly();
            LogicalRecord oldOne = engine.btreeService().lookup(readOne, sourceIndex, one)
                    .orElseThrow().record();
            engine.miniTransactionManager().commit(readOne);
            MiniTransaction update = engine.miniTransactionManager().begin(
                    engine.miniTransactionManager().budgetFor(
                            RedoBudgetPurpose.CLUSTERED_UPDATE,
                            BTreeRedoBudgetEstimator.pointRewrite()));
            assertTrue(engine.btreeService().replaceClustered(update, sourceIndex, one,
                    new LogicalRecord(2, List.of(new ColumnValue.IntValue(1),
                            new ColumnValue.StringValue("after")), false,
                            RecordType.CONVENTIONAL, oldOne.hiddenColumns()),
                    oldOne.hiddenColumns().dbTrxId(), oldOne.hiddenColumns().dbRollPtr()).replaced());
            engine.miniTransactionManager().commit(update);

            SearchKey two = new SearchKey(List.of(new ColumnValue.IntValue(2)));
            MiniTransaction readTwo = engine.miniTransactionManager().beginReadOnly();
            LogicalRecord oldTwo = engine.btreeService().lookup(readTwo, sourceIndex, two)
                    .orElseThrow().record();
            engine.miniTransactionManager().commit(readTwo);
            MiniTransaction delete = engine.miniTransactionManager().begin(
                    engine.miniTransactionManager().budgetFor(
                            RedoBudgetPurpose.CLUSTERED_DELETE,
                            BTreeRedoBudgetEstimator.structuralDelete(sourceIndex.rootLevel())));
            assertTrue(engine.btreeService().deleteClustered(delete, sourceIndex, two,
                    oldTwo.hiddenColumns().dbTrxId(), oldTwo.hiddenColumns().dbRollPtr()).removed());
            engine.miniTransactionManager().commit(delete);
            sourceIndex = insertClustered(engine, sourceIndex, 3, "inserted");

            // 第一遍完整结束前shadow只允许删除，不得提前把id=1按current truth重插。
            for (long id : List.of(1L, 2L, 3L, 1L, 2L, 3L)) {
                shadow = engine.tableDdlStorageService().deleteOnlineShadowIdentity(
                        request, shadow,
                        new SearchKey(List.of(new ColumnValue.IntValue(id))));
            }
            var deletedIndexes = new BTreeIndexMetadataFactory()
                    .createTable(targetDefinition, shadow);
            MiniTransaction deletedRead = engine.miniTransactionManager().beginReadOnly();
            assertTrue(engine.btreeService().scanAll(
                    deletedRead, deletedIndexes.clusteredIndex(), 10).isEmpty());
            engine.miniTransactionManager().commit(deletedRead);

            // 第二遍才从source current truth确保；重复identity验证幂等secondary收敛。
            for (long id : List.of(1L, 2L, 3L, 1L, 2L, 3L)) {
                shadow = engine.tableDdlStorageService().ensureOnlineShadowIdentityCurrent(
                        request, shadow,
                        new SearchKey(List.of(new ColumnValue.IntValue(id))));
            }
            engine.tableDdlStorageService().verifyOnlineShadow(
                    request, shadow, 16);

            var targetIndexes = new BTreeIndexMetadataFactory()
                    .createTable(targetDefinition, shadow);
            MiniTransaction targetRead = engine.miniTransactionManager().beginReadOnly();
            List<LogicalRecord> rows = engine.btreeService()
                    .scanAll(targetRead, targetIndexes.clusteredIndex(), 10).stream()
                    .map(result -> result.record()).toList();
            engine.miniTransactionManager().commit(targetRead);
            assertEquals(List.of(1L, 3L), rows.stream()
                    .map(row -> ((ColumnValue.IntValue) row.columnValues().getFirst()).value())
                    .toList());
            assertEquals("after", ((ColumnValue.StringValue)
                    rows.getFirst().columnValues().get(1)).value());
            assertEquals(7L, ((ColumnValue.IntValue)
                    rows.getFirst().columnValues().get(2)).value());
        } finally {
            engine.close();
        }
    }

    /** 在测试源聚簇树插入一行，并返回可能因split改变level后的descriptor。 */
    private static cn.zhangyis.db.storage.btree.BTreeIndex insertClustered(
            StorageEngine engine, cn.zhangyis.db.storage.btree.BTreeIndex index,
            long id, String value) {
        MiniTransaction insert = engine.miniTransactionManager().begin(
                engine.miniTransactionManager().budgetFor(
                        RedoBudgetPurpose.CLUSTERED_INSERT,
                        BTreeRedoBudgetEstimator.insert(index.rootLevel())));
        var result = engine.btreeService().insertClustered(insert, index,
                new LogicalRecord(2, List.of(new ColumnValue.IntValue(id),
                        new ColumnValue.StringValue(value)), false,
                        RecordType.CONVENTIONAL), TransactionId.of(id), RollPointer.NULL);
        engine.miniTransactionManager().commit(insert);
        return result.indexAfterInsert();
    }

    private static StorageTableDefinition definition(long tableId, SpaceId spaceId, Path path) {
        return new StorageTableDefinition(tableId, spaceId, path, 2, PageNo.of(128),
                List.of(new StorageColumnDefinition(1, "id", 0,
                        StorageColumnType.bigint(false, false))),
                List.of(new StorageIndexDefinition(tableId + 10, "PRIMARY", true, true,
                        List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)))));
    }

    /** 构造一个聚簇索引加一个二级索引的稳定物理定义，供 DROP INDEX 生命周期测试共用。 */
    private static StorageTableDefinition twoIndexDefinition(
            long tableId, SpaceId spaceId, Path path,
            long primaryIndexId, long secondaryIndexId) {
        return new StorageTableDefinition(tableId, spaceId, path,
                2, PageNo.of(128),
                List.of(new StorageColumnDefinition(1, "id", 0,
                        StorageColumnType.bigint(false, false))),
                List.of(new StorageIndexDefinition(
                                primaryIndexId, "PRIMARY", true, true,
                                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                        new StorageIndexDefinition(
                                secondaryIndexId, "idx_id", false, false,
                                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)))));
    }

    /** 单列测试表使用的非聚簇索引定义。 */
    private static StorageIndexDefinition secondary(long indexId, String name) {
        return new StorageIndexDefinition(indexId, name, false, false,
                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0)));
    }

    /** 构造测试用非空VARCHAR storage DTO。 */
    private static StorageColumnType varchar(int length) {
        return new StorageColumnType(StorageColumnTypeId.VARCHAR, false,
                length, 0, false, 1, 1, List.of());
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
