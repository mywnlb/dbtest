package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectExistsException;
import cn.zhangyis.db.dd.exception.DictionaryTransactionStateException;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.engine.adapter.DictionaryStorageMetadataMapper;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repository/DictionaryTransaction TDD：验证 append-only 版本只有带 manifest 的批次可见，并能跨重启重建名称索引。
 */
class PersistentDictionaryRepositoryTest {

    @TempDir
    Path directory;

    /** schema/table/column/index 作为一个字典版本提交，重启后按 id/name 都能恢复。 */
    @Test
    void commitsAndReloadsDictionaryAggregate() {
        Path path = directory.resolve("mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
            try (DictionaryTransaction transaction = repository.begin(DictionaryVersion.of(2))) {
                transaction.createSchema(schema(2));
                transaction.createTable(table(2));
                transaction.commit();
            }
            assertEquals(TableId.of(2), repository.findTable(SchemaId.of(1), ObjectName.of("ORDERS"))
                    .orElseThrow().id());
        }

        try (FileInternalCatalogStore store = FileInternalCatalogStore.openExisting(path)) {
            PersistentDictionaryRepository reopened = new PersistentDictionaryRepository(store);
            assertEquals("app", reopened.findSchema(ObjectName.of("APP")).orElseThrow().name().canonicalName());
            assertEquals(2, reopened.findTable(TableId.of(2)).orElseThrow().columns().size());
            assertEquals(SpaceId.of(1024), reopened.findTable(TableId.of(2)).orElseThrow()
                    .storageBinding().orElseThrow().spaceId());
            assertEquals(SegmentId.of(13), new DictionaryStorageMetadataMapper()
                    .map(reopened.findTable(TableId.of(2)).orElseThrow())
                    .lobSegment().orElseThrow().segmentId(),
                    "真实 close/reopen 后 mapper 必须得到 catalog 中同一个 LOB segment identity");
            assertEquals(DictionaryVersion.of(2), reopened.snapshot().publishedVersion());
        }
    }

    /** storage 批次 durable 但缺少 DD CATALOG_COMMIT manifest 时属于 staged crash residue，普通读取必须忽略。 */
    @Test
    void ignoresCatalogBatchWithoutDictionaryCommitManifest() {
        Path path = directory.resolve("mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            store.append(List.of(new CatalogRecord("uncommitted".getBytes(StandardCharsets.UTF_8),
                    "payload".getBytes(StandardCharsets.UTF_8))));
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);

            assertFalse(repository.findSchema(ObjectName.of("app")).isPresent());
            assertEquals(DictionaryVersion.of(1), repository.snapshot().publishedVersion());
        }
    }

    /** close 未提交 Unit of Work 必须只丢弃 staging，不占用版本也不追加 catalog batch。 */
    @Test
    void rollsBackUncommittedDictionaryTransactionOnClose() {
        Path path = directory.resolve("mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
            DictionaryTransaction rolledBack = repository.begin(DictionaryVersion.of(2));
            rolledBack.createSchema(schema(2));
            rolledBack.close();

            assertTrue(repository.findSchema(SchemaId.of(1)).isEmpty());
            assertTrue(store.readCommittedBatches().isEmpty());
            assertThrows(DictionaryTransactionStateException.class, rolledBack::commit);

            try (DictionaryTransaction retry = repository.begin(DictionaryVersion.of(2))) {
                retry.createSchema(schema(2));
                retry.commit();
            }
            assertTrue(repository.findSchema(SchemaId.of(1)).isPresent());
        }
    }

    /** canonical name 是唯一性权威；失败发生在 append 前，不能留下部分版本。 */
    @Test
    void rejectsDuplicateCanonicalSchemaNameBeforePersistence() {
        Path path = directory.resolve("mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
            try (DictionaryTransaction transaction = repository.begin(DictionaryVersion.of(2))) {
                transaction.createSchema(schema(2));
                transaction.commit();
            }

            DictionaryTransaction duplicate = repository.begin(DictionaryVersion.of(3));
            duplicate.createSchema(new SchemaDefinition(SchemaId.of(9), ObjectName.of("APP"), 1, 1,
                    DictionaryVersion.of(3)));
            assertThrows(DictionaryObjectExistsException.class, duplicate::commit);
            duplicate.close();

            assertTrue(repository.findSchema(SchemaId.of(9)).isEmpty());
            assertEquals(1, store.readCommittedBatches().size());
        }
    }

    /** DROP 的 DROP_PENDING/DROPPED 版本保留物理 binding 给恢复，普通 lookup 只暴露 ACTIVE。 */
    @Test
    void persistsTableLifecycleReplacementAndHidesDroppedTable() {
        Path path = directory.resolve("mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
            try (DictionaryTransaction create = repository.begin(DictionaryVersion.of(2))) {
                create.createSchema(schema(2));
                create.createTable(table(2));
                create.commit();
            }
            TableDefinition active = repository.findTable(TableId.of(2)).orElseThrow();
            try (DictionaryTransaction pending = repository.begin(DictionaryVersion.of(3))) {
                pending.updateTable(new TableDefinition(active.id(), active.schemaId(), active.name(),
                        DictionaryVersion.of(3), TableState.DROP_PENDING, active.columns(), active.indexes(),
                        active.storageBinding()));
                pending.commit();
            }
            TableDefinition dropPending = repository.findTableForRecovery(TableId.of(2)).orElseThrow();
            try (DictionaryTransaction dropped = repository.begin(DictionaryVersion.of(4))) {
                dropped.updateTable(new TableDefinition(dropPending.id(), dropPending.schemaId(), dropPending.name(),
                        DictionaryVersion.of(4), TableState.DROPPED, dropPending.columns(), dropPending.indexes(),
                        dropPending.storageBinding()));
                dropped.commit();
            }

            assertTrue(repository.findTable(TableId.of(2)).isEmpty());
            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(TableId.of(2)).orElseThrow().state());
        }

        try (FileInternalCatalogStore store = FileInternalCatalogStore.openExisting(path)) {
            PersistentDictionaryRepository reopened = new PersistentDictionaryRepository(store);
            assertTrue(reopened.findTable(SchemaId.of(1), ObjectName.of("orders")).isEmpty());
            assertEquals(TableState.DROPPED,
                    reopened.findTableForRecovery(TableId.of(2)).orElseThrow().state());
        }
    }

    /**
     * repository 只允许 ACTIVE→ACTIVE 精确追加一个二级索引及对应 binding；列、聚簇索引和既有物理绑定必须不变。
     */
    @Test
    void persistsExactSecondaryIndexAdditionAndRemovalAndRejectsBindingDrift() {
        Path path = directory.resolve("create-index-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(path)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(store);
            try (DictionaryTransaction create = repository.begin(DictionaryVersion.of(2))) {
                create.createSchema(schema(2));
                create.createTable(table(2));
                create.commit();
            }
            TableDefinition before = repository.findTable(TableId.of(2)).orElseThrow();
            IndexDefinition secondary = new IndexDefinition(
                    IndexId.of(4), ObjectName.of("idx_body"), false, false,
                    List.of(new IndexKeyPart(2, IndexOrder.ASC, 0)));
            TableStorageBinding oldBinding = before.storageBinding().orElseThrow();
            SpaceId space = oldBinding.spaceId();
            IndexStorageBinding secondaryBinding = new IndexStorageBinding(
                    4, PageId.of(space, PageNo.of(65)), 0,
                    new SegmentRef(space, 4, SegmentId.of(14)),
                    new SegmentRef(space, 5, SegmentId.of(15)));
            TableStorageBinding newBinding = new TableStorageBinding(
                    oldBinding.tableId(), space, oldBinding.path(), oldBinding.rowFormatVersion(),
                    java.util.stream.Stream.concat(oldBinding.indexes().stream(),
                            java.util.stream.Stream.of(secondaryBinding)).toList(),
                    oldBinding.lobSegment());
            TableDefinition after = new TableDefinition(
                    before.id(), before.schemaId(), before.name(), DictionaryVersion.of(3), TableState.ACTIVE,
                    before.columns(),
                    java.util.stream.Stream.concat(before.indexes().stream(),
                            java.util.stream.Stream.of(secondary)).toList(),
                    Optional.of(newBinding));

            try (DictionaryTransaction addIndex = repository.begin(DictionaryVersion.of(3))) {
                addIndex.updateTable(after);
                addIndex.commit();
            }

            assertEquals(IndexId.of(4), repository.findIndex(IndexId.of(4)).orElseThrow().id());
            TableDefinition committed = repository.findTable(TableId.of(2)).orElseThrow();
            IndexStorageBinding driftedPrimary = new IndexStorageBinding(
                    committed.primaryIndex().id().value(), PageId.of(space, PageNo.of(66)), 0,
                    new SegmentRef(space, 6, SegmentId.of(16)),
                    new SegmentRef(space, 7, SegmentId.of(17)));
            TableDefinition driftedRemoval = new TableDefinition(
                    committed.id(), committed.schemaId(), committed.name(), DictionaryVersion.of(4),
                    TableState.ACTIVE, committed.columns(), List.of(committed.primaryIndex()),
                    Optional.of(new TableStorageBinding(
                            oldBinding.tableId(), space, oldBinding.path(), oldBinding.rowFormatVersion(),
                            List.of(driftedPrimary), oldBinding.lobSegment())));
            try (DictionaryTransaction drift = repository.begin(DictionaryVersion.of(4))) {
                drift.updateTable(driftedRemoval);
                assertThrows(cn.zhangyis.db.dd.exception.DictionaryVersionConflictException.class,
                        drift::commit);
            }

            TableDefinition exactRemoval = new TableDefinition(
                    committed.id(), committed.schemaId(), committed.name(), DictionaryVersion.of(4),
                    TableState.ACTIVE, committed.columns(), List.of(committed.primaryIndex()),
                    Optional.of(new TableStorageBinding(
                            oldBinding.tableId(), space, oldBinding.path(), oldBinding.rowFormatVersion(),
                            List.of(oldBinding.indexes().getFirst()), oldBinding.lobSegment())));
            try (DictionaryTransaction remove = repository.begin(DictionaryVersion.of(4))) {
                remove.updateTable(exactRemoval);
                remove.commit();
            }
            assertTrue(repository.findIndex(IndexId.of(4)).isEmpty());

            TableDefinition removed = repository.findTable(TableId.of(2)).orElseThrow();
            TableDefinition illegal = new TableDefinition(
                    removed.id(), removed.schemaId(), ObjectName.of("renamed"), DictionaryVersion.of(5),
                    TableState.ACTIVE, removed.columns(), removed.indexes(), removed.storageBinding());
            try (DictionaryTransaction rename = repository.begin(DictionaryVersion.of(5))) {
                rename.updateTable(illegal);
                assertThrows(cn.zhangyis.db.dd.exception.DictionaryVersionConflictException.class, rename::commit);
            }
        }
    }

    private static SchemaDefinition schema(long version) {
        return new SchemaDefinition(SchemaId.of(1), ObjectName.of("app"), 1, 1,
                DictionaryVersion.of(version));
    }

    private static TableDefinition table(long version) {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        ColumnDefinition body = new ColumnDefinition(2, ObjectName.of("body"),
                new ColumnTypeDefinition(DictionaryTypeId.TEXT, false, true, 65_535, 0, 1, 1, List.of()), 1);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        SegmentRef leaf = new SegmentRef(SpaceId.of(1024), 1, SegmentId.of(11));
        SegmentRef nonLeaf = new SegmentRef(SpaceId.of(1024), 2, SegmentId.of(12));
        SegmentRef lob = new SegmentRef(SpaceId.of(1024), 3, SegmentId.of(13));
        TableStorageBinding binding = new TableStorageBinding(2, SpaceId.of(1024),
                Path.of("app_orders_1024.ibd"), version, List.of(new IndexStorageBinding(3,
                PageId.of(SpaceId.of(1024), PageNo.of(64)), 0, leaf, nonLeaf)), Optional.of(lob));
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(version), TableState.ACTIVE, List.of(id, body), List.of(primary),
                Optional.of(binding));
    }
}
