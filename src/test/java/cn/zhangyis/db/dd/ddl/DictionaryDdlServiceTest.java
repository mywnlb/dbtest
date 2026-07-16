package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.recovery.DictionaryDdlRecoveryService;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogPersistenceException;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** DD DDL 协调器 TDD：验证 MDL、ID/version、物理 DDL、catalog publish 和 cache 失效的完整顺序。 */
class DictionaryDdlServiceTest {

    @TempDir
    Path directory;

    /** CREATE 仅在物理 root durable 后发布 ACTIVE；DROP 经 pending 状态删除物理文件后发布 DROPPED。 */
    @Test
    void createsAndDropsTableAcrossDictionaryAndPhysicalStorage() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(directory.resolve("mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), directory.resolve("tables"));

            ddl.createSchema(MdlOwnerId.of(1), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            var table = ddl.createTable(MdlOwnerId.of(1), new CreateTableCommand(
                    QualifiedTableName.of("app", "orders"), PageNo.of(128),
                    List.of(new CreateColumnSpec(ObjectName.of("id"),
                            ColumnTypeDefinition.bigint(false, false))),
                    List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                            List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0))))),
                    Duration.ofSeconds(5));

            Path tableFile = table.storageBinding().orElseThrow().path();
            assertTrue(Files.exists(tableFile));
            assertEquals(TableState.ACTIVE, repository.findTable(table.id()).orElseThrow().state());
            DataDictionaryService dictionary = new DataDictionaryService(repository, cache, locks);
            try (var lease = dictionary.openTable(MdlOwnerId.of(2),
                    QualifiedTableName.of("APP", "ORDERS"), Duration.ofSeconds(1))) {
                assertEquals(table.id(), lease.table().id());
            }

            ddl.dropTable(MdlOwnerId.of(1), QualifiedTableName.of("app", "orders"),
                    Duration.ofSeconds(5));

            assertFalse(Files.exists(tableFile));
            assertTrue(repository.findTable(table.id()).isEmpty());
            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(table.id()).orElseThrow().state());
        } finally {
            storage.close();
        }
    }

    /** DROP_PENDING durable 后崩溃时，恢复使用持久 binding 删除文件并以新的单调版本收敛 DROPPED。 */
    @Test
    void recoversCrashAfterDropPendingPublish() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("pending-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("pending-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            Path tables = directory.resolve("pending-tables");
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(10), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            var table = ddl.createTable(MdlOwnerId.of(10), command(), Duration.ofSeconds(5));
            Path file = table.storageBinding().orElseThrow().path();
            DictionaryDdlService crashing = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables,
                    pending -> { throw new DictionaryDdlException("injected crash after DROP_PENDING"); });

            assertThrows(DictionaryDdlException.class, () -> crashing.dropTable(MdlOwnerId.of(10),
                    QualifiedTableName.of("app", "orders"), Duration.ofSeconds(5)));
            assertEquals(TableState.DROP_PENDING,
                    repository.findTableForRecovery(table.id()).orElseThrow().state());
            assertTrue(Files.exists(file));
            Path orphan = tables.resolve("table_999_space_1999.ibd");
            try {
                Files.createDirectories(tables);
                Files.write(orphan, new byte[]{1, 2, 3});
            } catch (java.io.IOException e) {
                throw new AssertionError(e);
            }

            new DictionaryDdlRecoveryService(control, repository, cache, storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            assertFalse(Files.exists(file));
            assertFalse(Files.exists(orphan), "uncommitted CREATE orphan must be removed before OPEN");
            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(table.id()).orElseThrow().state());
            assertTrue(repository.snapshot().publishedVersion().value() > 5,
                    "recovery version must remain monotonic even when reserved version 5 was never published");
        } finally {
            storage.close();
        }
    }

    /**
     * catalog header force 已可能 durable 但 append 向调用方报错时，CREATE 不得删物理文件。重启重建 catalog 后，
     * 已提交则继续使用；若未提交才由 orphan discovery 清理。
     */
    @Test
    void retainsPhysicalCreateWhenCatalogCommitOutcomeIsUncertain() {
        Path catalogPath = directory.resolve("mysql.ibd");
        Path controlPath = directory.resolve("mysql.dd.ctrl");
        Path tables = directory.resolve("tables");
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(catalogPath);
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     controlPath, SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository initial = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, initial, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(20), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));

            InternalCatalogStore uncertain = durableThenThrow(catalog);
            PersistentDictionaryRepository uncertainRepository = new PersistentDictionaryRepository(uncertain);
            DictionaryDdlService uncertainDdl = new DictionaryDdlService(control, uncertainRepository, cache,
                    locks, storage.tableDdlStorageService(), tables);

            assertThrows(InternalCatalogPersistenceException.class, () -> uncertainDdl.createTable(
                    MdlOwnerId.of(20), command(), Duration.ofSeconds(5)));
            assertTrue(Files.exists(tables.resolve("table_1_space_1024.ibd")),
                    "uncertain catalog commit must retain physical storage until startup reconciliation");
        } finally {
            storage.close();
        }

        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            try (var lease = database.dictionary().openTable(MdlOwnerId.of(21),
                    QualifiedTableName.of("app", "orders"), Duration.ofSeconds(2))) {
                assertEquals(TableState.ACTIVE, lease.table().state());
            }
        }
    }

    /**
     * DROP_PENDING 的 catalog header 可能已 durable 但 append 报错；当前进程必须保留 cache 准入屏障，
     * 不能从仍为 ACTIVE 的 repository 内存快照复活表，重启后再续作物理删除。
     */
    @Test
    void blocksTableUntilRestartWhenDropCatalogOutcomeIsUncertain() {
        Path catalogPath = directory.resolve("mysql.ibd");
        Path controlPath = directory.resolve("mysql.dd.ctrl");
        Path tables = directory.resolve("tables");
        Path tableFile = tables.resolve("table_1_space_1024.ibd");
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(catalogPath);
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     controlPath, SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(30), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            ddl.createTable(MdlOwnerId.of(30), command(), Duration.ofSeconds(5));

            PersistentDictionaryRepository uncertainRepository =
                    new PersistentDictionaryRepository(durableThenThrow(catalog));
            DictionaryDdlService uncertainDdl = new DictionaryDdlService(control, uncertainRepository, cache,
                    locks, storage.tableDdlStorageService(), tables);
            DataDictionaryService liveDictionary = new DataDictionaryService(uncertainRepository, cache, locks);

            assertThrows(InternalCatalogPersistenceException.class, () -> uncertainDdl.dropTable(
                    MdlOwnerId.of(30), QualifiedTableName.of("app", "orders"), Duration.ofSeconds(5)));
            assertTrue(Files.exists(tableFile), "uncertain DROP publish must not start physical deletion");
            assertThrows(DictionaryObjectNotFoundException.class, () -> liveDictionary.openTable(
                    MdlOwnerId.of(31), QualifiedTableName.of("app", "orders"), Duration.ofSeconds(2)));
        } finally {
            storage.close();
        }

        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            assertFalse(Files.exists(tableFile), "startup must resume durable DROP_PENDING physical deletion");
            assertThrows(DictionaryObjectNotFoundException.class, () -> database.dictionary().openTable(
                    MdlOwnerId.of(32), QualifiedTableName.of("app", "orders"), Duration.ofSeconds(2)));
        }
    }

    private static InternalCatalogStore durableThenThrow(InternalCatalogStore delegate) {
        return new InternalCatalogStore() {
            @Override
            public long append(List<CatalogRecord> records) {
                delegate.append(records);
                throw new InternalCatalogPersistenceException("injected post-durable catalog failure");
            }

            @Override
            public List<CatalogBatch> readCommittedBatches() {
                return delegate.readCommittedBatches();
            }

            @Override
            public long committedLength() {
                return delegate.committedLength();
            }

            @Override
            public void close() {
                // delegate 由测试外层 try-with-resources 拥有，wrapper 不重复关闭。
            }
        };
    }

    private static CreateTableCommand command() {
        return new CreateTableCommand(QualifiedTableName.of("app", "orders"), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                        List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0)))));
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }
}
