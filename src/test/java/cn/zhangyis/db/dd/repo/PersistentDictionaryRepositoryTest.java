package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
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
            assertEquals(1, reopened.findTable(TableId.of(2)).orElseThrow().columns().size());
            assertEquals(SpaceId.of(1024), reopened.findTable(TableId.of(2)).orElseThrow()
                    .storageBinding().orElseThrow().spaceId());
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

    private static SchemaDefinition schema(long version) {
        return new SchemaDefinition(SchemaId.of(1), ObjectName.of("app"), 1, 1,
                DictionaryVersion.of(version));
    }

    private static TableDefinition table(long version) {
        ColumnDefinition id = new ColumnDefinition(1, ObjectName.of("id"),
                ColumnTypeDefinition.bigint(false, false), 0);
        IndexDefinition primary = new IndexDefinition(IndexId.of(3), ObjectName.of("PRIMARY"), true, true,
                List.of(new IndexKeyPart(1, IndexOrder.ASC, 0)));
        SegmentRef leaf = new SegmentRef(SpaceId.of(1024), 1, SegmentId.of(11));
        SegmentRef nonLeaf = new SegmentRef(SpaceId.of(1024), 2, SegmentId.of(12));
        TableStorageBinding binding = new TableStorageBinding(2, SpaceId.of(1024),
                Path.of("app_orders_1024.ibd"), List.of(new IndexStorageBinding(3,
                PageId.of(SpaceId.of(1024), PageNo.of(64)), 0, leaf, nonLeaf)));
        return new TableDefinition(TableId.of(2), SchemaId.of(1), ObjectName.of("orders"),
                DictionaryVersion.of(version), TableState.ACTIVE, List.of(id), List.of(primary), Optional.of(binding));
    }
}
