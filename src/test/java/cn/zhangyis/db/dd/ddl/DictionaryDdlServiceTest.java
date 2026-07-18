package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.recovery.DictionaryDdlRecoveryService;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryException;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.dd.sdi.DictionarySdiCodec;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.engine.adapter.DictionaryIndexMetadataResolver;
import cn.zhangyis.db.engine.adapter.DictionaryStorageMetadataMapper;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.api.TablePurgeBarrier;
import cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfo;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfoException;
import cn.zhangyis.db.storage.api.dml.DmlCommitCommand;
import cn.zhangyis.db.storage.api.dml.TableInsertCommand;
import cn.zhangyis.db.storage.api.dml.TableUpdateCommand;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.redo.DurabilityPolicy;
import cn.zhangyis.db.storage.trx.TransactionOptions;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogPersistenceException;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import cn.zhangyis.db.session.SessionOptions;
import cn.zhangyis.db.sql.executor.QueryResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** DD DDL 协调器 TDD：验证 MDL、ID/version、物理 DDL、catalog publish 和 cache 失效的完整顺序。 */
class DictionaryDdlServiceTest {

    @TempDir
    Path directory;

    /**
     * CREATE INDEX 必须回填既有行、发布新 exact DD/SDI/binding 并在重启后继续供 SQL 二级访问；
     * metadata-only 版本推进不能改变物理 row format version。
     */
    @Test
    void createsBackfilledSecondaryIndexAndKeepsItAcrossRestart() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(100), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition before = database.ddl().createTable(
                    MdlOwnerId.of(100), updateCommand(), Duration.ofSeconds(5));
            try (var session = database.openSession(SessionOptions.defaults())) {
                session.execute("INSERT INTO app.orders (id,value) VALUES (1,7)");
                session.execute("INSERT INTO app.orders (id,value) VALUES (2,7)");
                session.execute("INSERT INTO app.orders (id,value) VALUES (3,8)");
            }

            TableDefinition after = database.ddl().createSecondaryIndex(
                    MdlOwnerId.of(101),
                    new CreateSecondaryIndexCommand(
                            QualifiedTableName.of("app", "orders"),
                            new CreateIndexSpec(ObjectName.of("idx_value"), false, false,
                                    List.of(new CreateIndexKeyPartSpec(
                                            ObjectName.of("value"), IndexOrder.ASC, 0)))),
                    Duration.ofSeconds(5));

            assertEquals(2, after.indexes().size());
            assertTrue(after.version().compareTo(before.version()) > 0);
            assertEquals(before.storageBinding().orElseThrow().rowFormatVersion(),
                    after.storageBinding().orElseThrow().rowFormatVersion());
            try (var session = database.openSession(SessionOptions.defaults())) {
                QueryResult result = assertInstanceOf(
                        QueryResult.class,
                        session.execute("SELECT id FROM app.orders WHERE value=7"));
                assertEquals(2, result.rows().size());
            }
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config())) {
            reopened.open();
            try (var session = reopened.openSession(SessionOptions.defaults())) {
                QueryResult result = assertInstanceOf(
                        QueryResult.class,
                        session.execute("SELECT id FROM app.orders WHERE value=7"));
                assertEquals(2, result.rows().size());
            }
        }
    }

    /** ENGINE_DONE 前后 DD 仍是旧版本时，恢复必须回收 staged segments/footer 并回滚 marker。 */
    @Test
    void rollsBackCreateIndexAfterEngineDoneCrash() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("index-engine-done-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("index-engine-done-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("index-engine-done-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(110), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition table = base.createTable(
                    MdlOwnerId.of(110), updateCommand(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterCreateIndexEngineDone(DdlLogRecord engineDone) {
                            throw new DictionaryDdlException("injected crash after CREATE INDEX ENGINE_DONE");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> crashing.createSecondaryIndex(
                    MdlOwnerId.of(111), secondaryIndexCommand(), Duration.ofSeconds(5)));
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(table.storageBinding().orElseThrow()).isPresent());

            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            assertEquals(1, repository.findTable(table.id()).orElseThrow().indexes().size());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(table.storageBinding().orElseThrow()).isEmpty());
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** 新 DD 已 durable 时恢复必须保留 index binding，只清 footer、发布 cache 并补齐 terminal marker。 */
    @Test
    void finishesCreateIndexAfterDictionaryCommitCrash() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("index-dd-committed-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("index-dd-committed-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("index-dd-committed-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(120), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition table = base.createTable(
                    MdlOwnerId.of(120), updateCommand(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterCreateIndexDictionaryCommitted(TableDefinition active) {
                            throw new DictionaryDdlException("injected crash after CREATE INDEX DD commit");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> crashing.createSecondaryIndex(
                    MdlOwnerId.of(121), secondaryIndexCommand(), Duration.ofSeconds(5)));
            assertEquals(2, repository.findTable(table.id()).orElseThrow().indexes().size());

            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            TableDefinition recovered = repository.findTable(table.id()).orElseThrow();
            assertEquals(ObjectName.of("idx_value"), recovered.indexes().getLast().name());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(recovered.storageBinding().orElseThrow()).isEmpty());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

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
            SerializedDictionaryInfo storedSdi = storage.tableDdlStorageService()
                    .readSerializedDictionaryInfo(table.storageBinding().orElseThrow()).orElseThrow();
            assertEquals(table.id().value(), storedSdi.tableId());
            assertEquals(table.version().value(), storedSdi.dictionaryVersion());
            assertEquals(table, new DictionarySdiCodec().decode(storedSdi.payload()));
            DataDictionaryService dictionary = new DataDictionaryService(repository, cache, locks);
            try (var lease = dictionary.openTable(MdlOwnerId.of(2),
                    QualifiedTableName.of("APP", "ORDERS"), TableAccessIntent.READ,
                    Duration.ofSeconds(1))) {
                assertEquals(table.id(), lease.table().id());
            }

            ddl.dropTable(MdlOwnerId.of(1), QualifiedTableName.of("app", "orders"),
                    Duration.ofSeconds(5));

            assertFalse(Files.exists(tableFile));
            assertTrue(repository.findTable(table.id()).isEmpty());
            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(table.id()).orElseThrow().state());
            assertEquals(3, repository.ddlLog().highestDdlId());
            assertEquals(DdlLogPhase.COMMITTED,
                    repository.ddlLog().find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());
            assertEquals(DdlLogPhase.COMMITTED,
                    repository.ddlLog().find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /**
     * SDI 不是新的提交真相：即使 page3 保存更高 version 和不可解码 payload，
     * 启动 DDL recovery 也必须按 committed ACTIVE DD 覆盖，而不能把文件内容反向发布进 catalog。
     */
    @Test
    void recoveryRewritesMismatchedSdiFromCommittedDictionary() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("sdi-rewrite-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("sdi-rewrite-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("sdi-rewrite-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(90), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition table = ddl.createTable(MdlOwnerId.of(90), command(), Duration.ofSeconds(5));
            var binding = table.storageBinding().orElseThrow();
            storage.tableDdlStorageService().writeSerializedDictionaryInfo(binding,
                    new SerializedDictionaryInfo(table.id().value(), table.version().value() + 100,
                            new byte[]{1, 2, 3, 4}), Duration.ofSeconds(5));

            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));

            SerializedDictionaryInfo repaired = storage.tableDdlStorageService()
                    .readSerializedDictionaryInfo(binding).orElseThrow();
            assertEquals(table.version().value(), repaired.dictionaryVersion());
            assertEquals(table, new DictionarySdiCodec().decode(repaired.payload()));
        } finally {
            storage.close();
        }
    }

    /**
     * 单页 SDI v1 的容量是显式 DDL 边界：超大聚合可先完成物理 CREATE，但不得发布 ACTIVE DD；
     * durable ENGINE_DONE marker 使下一次 recovery 能精确删除该物理 orphan。
     */
    @Test
    void oversizedSinglePageSdiPreventsDictionaryPublishAndIsRecovered() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("sdi-oversized-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("sdi-oversized-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("sdi-oversized-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(91), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            List<CreateColumnSpec> columns = new java.util.ArrayList<>();
            for (int i = 0; i < 200; i++) {
                String name = "c%03d_".formatted(i) + "x".repeat(58);
                columns.add(new CreateColumnSpec(ObjectName.of(name),
                        ColumnTypeDefinition.bigint(false, false)));
            }
            CreateTableCommand oversized = new CreateTableCommand(
                    QualifiedTableName.of("app", "wide_table"), PageNo.of(128), columns,
                    List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                            List.of(new CreateIndexKeyPartSpec(columns.getFirst().name(),
                                    IndexOrder.ASC, 0)))));

            assertThrows(SerializedDictionaryInfoException.class, () -> ddl.createTable(
                    MdlOwnerId.of(91), oversized, Duration.ofSeconds(5)));
            Path physicalFile = tables.resolve("table_1_space_1024.ibd");
            assertTrue(Files.exists(physicalFile), "committed physical MTR is retained for DDL recovery");
            assertTrue(repository.findTableForRecovery(cn.zhangyis.db.dd.domain.TableId.of(1)).isEmpty());
            assertEquals(DdlLogPhase.ENGINE_DONE, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());

            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));
            assertFalse(Files.exists(physicalFile));
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** DROP 必须在发布 DROP_PENDING 前等待 purge barrier；超时后 catalog/cache/文件仍保持 ACTIVE 可用状态。 */
    @Test
    void dropTimeoutAtPurgeBarrierKeepsTableActive() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("barrier-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("barrier-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            Path tables = directory.resolve("barrier-tables");
            DictionaryDdlService creator = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            creator.createSchema(MdlOwnerId.of(40), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            var table = creator.createTable(MdlOwnerId.of(40), command(), Duration.ofSeconds(5));
            Path tableFile = table.storageBinding().orElseThrow().path();
            TablePurgeBarrier blocking = new TablePurgeBarrier() {
                @Override
                public void awaitUnreferenced(long tableId, Duration timeout) {
                    throw new TablePurgeBarrierTimeoutException(
                            "synthetic table history reference: " + tableId);
                }

                @Override
                public int referenceCount(long tableId) {
                    return 1;
                }
            };
            DictionaryDdlService blocked = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables, blocking);

            assertThrows(TablePurgeBarrierTimeoutException.class, () -> blocked.dropTable(
                    MdlOwnerId.of(40), QualifiedTableName.of("app", "orders"), Duration.ofMillis(20)));

            assertEquals(TableState.ACTIVE, repository.findTableForRecovery(table.id()).orElseThrow().state());
            assertTrue(Files.exists(tableFile));
            try (var lease = new DataDictionaryService(repository, cache, locks).openTable(
                    MdlOwnerId.of(41), QualifiedTableName.of("app", "orders"), TableAccessIntent.READ,
                    Duration.ofSeconds(1))) {
                assertEquals(table.id(), lease.table().id());
            }
        } finally {
            storage.close();
        }
    }

    /** DROP 在真实 history 引用上等待；purge finalization 发布引用减少后唤醒同一 Condition 并继续物理删除。 */
    @Test
    void purgeFinalizationWakesWaitingDrop() throws Exception {
        Path tables = directory.resolve("purge-wake-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("purge-wake-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("purge-wake-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            StorageEngine storage = new StorageEngine(config());
            storage.configureIndexMetadataResolver(new DictionaryIndexMetadataResolver(repository));
            storage.open();
            try {
                DictionaryObjectCache cache = new DictionaryObjectCache(16);
                MetadataLockManager locks = new MetadataLockManager(8, 128);
                CountDownLatch barrierEntered = new CountDownLatch(1);
                TablePurgeBarrier observedBarrier = new TablePurgeBarrier() {
                    private final TablePurgeBarrier delegate = storage.tablePurgeBarrier();

                    @Override
                    public void awaitUnreferenced(long tableId, Duration timeout) {
                        barrierEntered.countDown();
                        delegate.awaitUnreferenced(tableId, timeout);
                    }

                    @Override
                    public int referenceCount(long tableId) {
                        return delegate.referenceCount(tableId);
                    }
                };
                DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                        storage.tableDdlStorageService(), tables, observedBarrier);
                ddl.createSchema(MdlOwnerId.of(60), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
                var table = ddl.createTable(MdlOwnerId.of(60), updateCommand(), Duration.ofSeconds(5));
                var mapped = new DictionaryStorageMetadataMapper().map(table);
                long schemaVersion = mapped.tableIndexes().schemaVersion();
                LogicalRecord before = new LogicalRecord(schemaVersion,
                        List.of(new ColumnValue.IntValue(1), new ColumnValue.IntValue(10)),
                        false, RecordType.CONVENTIONAL);
                LogicalRecord after = new LogicalRecord(schemaVersion,
                        List.of(new ColumnValue.IntValue(1), new ColumnValue.IntValue(20)),
                        false, RecordType.CONVENTIONAL);
                var insert = storage.transactionManager().begin(TransactionOptions.defaults());
                storage.tableDmlService().insert(new TableInsertCommand(insert, mapped.tableIndexes(), before,
                        Optional.empty(), Duration.ofSeconds(5)));
                storage.tableDmlService().commit(new DmlCommitCommand(
                        insert, DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofSeconds(5)));
                var update = storage.transactionManager().begin(TransactionOptions.defaults());
                storage.tableDmlService().update(new TableUpdateCommand(update, mapped.tableIndexes(),
                        new SearchKey(List.of(new ColumnValue.IntValue(1))), after, Duration.ofSeconds(5)));
                storage.tableDmlService().commit(new DmlCommitCommand(
                        update, DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofSeconds(5)));
                assertEquals(1, storage.tablePurgeBarrier().referenceCount(table.id().value()));
                Path tableFile = table.storageBinding().orElseThrow().path();

                try (var executor = Executors.newSingleThreadExecutor()) {
                    var drop = executor.submit(() -> ddl.dropTable(MdlOwnerId.of(60),
                            QualifiedTableName.of("app", "orders"), Duration.ofSeconds(2)));
                    assertTrue(barrierEntered.await(1, TimeUnit.SECONDS));
                    assertFalse(drop.isDone(), "DROP must remain blocked while committed history references table");

                    assertEquals(1, storage.purgeCoordinator().runBatch(1).purgedLogs());
                    drop.get(2, TimeUnit.SECONDS);
                }

                assertEquals(0, storage.tablePurgeBarrier().referenceCount(table.id().value()));
                assertFalse(Files.exists(tableFile));
                assertEquals(TableState.DROPPED,
                        repository.findTableForRecovery(table.id()).orElseThrow().state());
            } finally {
                storage.close();
            }
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
            assertEquals(DdlLogPhase.DICTIONARY_COMMITTED,
                    repository.ddlLog().find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
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
            assertEquals(DdlLogPhase.COMMITTED,
                    repository.ddlLog().find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
            assertTrue(repository.snapshot().publishedVersion().value() > 5,
                    "recovery version must remain monotonic even when reserved version 5 was never published");
        } finally {
            storage.close();
        }
    }

    /** CREATE 只写 PREPARED 后崩溃时没有物理副作用，恢复必须把 marker 终结为 ROLLED_BACK。 */
    @Test
    void rollsBackCreatePreparedMarker() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("create-prepared-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("create-prepared-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("create-prepared-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService creator = ddlWithFault(storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterCreatePrepared(DdlLogRecord prepared) {
                            throw new DictionaryDdlException("injected crash after CREATE PREPARED");
                        }
                    });
            creator.createSchema(MdlOwnerId.of(70), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));

            assertThrows(DictionaryDdlException.class, () -> creator.createTable(
                    MdlOwnerId.of(70), command(), Duration.ofSeconds(5)));
            assertFalse(Files.exists(tables.resolve("table_1_space_1024.ibd")));
            assertEquals(DdlLogPhase.PREPARED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());

            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** 物理 CREATE 与 ENGINE_DONE durable、ACTIVE 未提交时，恢复只删除 marker 的 exact path 并回滚。 */
    @Test
    void rollsBackCreateEngineDoneAndDeletesExactPath() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("create-engine-done-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("create-engine-done-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("create-engine-done-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService creator = ddlWithFault(storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterCreateEngineDone(DdlLogRecord engineDone) {
                            throw new DictionaryDdlException("injected crash after CREATE ENGINE_DONE");
                        }
                    });
            creator.createSchema(MdlOwnerId.of(71), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));

            assertThrows(DictionaryDdlException.class, () -> creator.createTable(
                    MdlOwnerId.of(71), command(), Duration.ofSeconds(5)));
            Path created = tables.resolve("table_1_space_1024.ibd");
            assertTrue(Files.exists(created));
            assertEquals(DdlLogPhase.ENGINE_DONE, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());

            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));
            assertFalse(Files.exists(created));
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** ACTIVE DD 与 DICTIONARY_COMMITTED durable 后崩溃时，恢复保留文件、发布 cache 并补 COMMITTED。 */
    @Test
    void finishesCreateAfterActiveDictionaryPublish() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("create-dd-committed-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("create-dd-committed-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("create-dd-committed-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService creator = ddlWithFault(storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterCreateDictionaryCommitted(TableDefinition active) {
                            throw new DictionaryDdlException("injected crash after CREATE DD commit");
                        }
                    });
            creator.createSchema(MdlOwnerId.of(72), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));

            assertThrows(DictionaryDdlException.class, () -> creator.createTable(
                    MdlOwnerId.of(72), command(), Duration.ofSeconds(5)));
            TableDefinition active = repository.findTableForRecovery(
                    cn.zhangyis.db.dd.domain.TableId.of(1)).orElseThrow();
            Path created = active.storageBinding().orElseThrow().path();
            assertTrue(Files.exists(created));
            assertEquals(DdlLogPhase.DICTIONARY_COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());

            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));
            assertTrue(Files.exists(created));
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(2)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** DROP 只写 PREPARED、DD 仍 ACTIVE 时没有越过提交裁决点，恢复不得删除原表文件。 */
    @Test
    void rollsBackDropPreparedMarker() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("drop-prepared-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("drop-prepared-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("drop-prepared-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(73), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition table = ddl.createTable(MdlOwnerId.of(73), command(), Duration.ofSeconds(5));
            Path file = table.storageBinding().orElseThrow().path();
            DictionaryDdlService dropper = ddlWithFault(storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterDropPrepared(DdlLogRecord prepared) {
                            throw new DictionaryDdlException("injected crash after DROP PREPARED");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> dropper.dropTable(MdlOwnerId.of(73),
                    QualifiedTableName.of("app", "orders"), Duration.ofSeconds(5)));
            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));

            assertTrue(Files.exists(file));
            assertEquals(TableState.ACTIVE, repository.findTable(table.id()).orElseThrow().state());
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** DROP 物理文件已删除且 ENGINE_DONE durable 时，恢复发布 DROPPED 并终结原 marker。 */
    @Test
    void finishesDropAfterEngineDone() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("drop-engine-done-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("drop-engine-done-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("drop-engine-done-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(74), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition table = ddl.createTable(MdlOwnerId.of(74), command(), Duration.ofSeconds(5));
            Path file = table.storageBinding().orElseThrow().path();
            DictionaryDdlService dropper = ddlWithFault(storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterDropEngineDone(DdlLogRecord engineDone) {
                            throw new DictionaryDdlException("injected crash after DROP ENGINE_DONE");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> dropper.dropTable(MdlOwnerId.of(74),
                    QualifiedTableName.of("app", "orders"), Duration.ofSeconds(5)));
            assertFalse(Files.exists(file));
            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));

            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(table.id()).orElseThrow().state());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** DROPPED DD 已 durable、terminal marker 未写时，恢复只补日志并保持表不可见。 */
    @Test
    void terminalizesDropAfterDroppedDictionaryPublish() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("drop-dd-committed-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("drop-dd-committed-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("drop-dd-committed-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(75), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition table = ddl.createTable(MdlOwnerId.of(75), command(), Duration.ofSeconds(5));
            DictionaryDdlService dropper = ddlWithFault(storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterDropDictionaryCommitted(TableDefinition dropped) {
                            throw new DictionaryDdlException("injected crash after DROPPED DD commit");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> dropper.dropTable(MdlOwnerId.of(75),
                    QualifiedTableName.of("app", "orders"), Duration.ofSeconds(5)));
            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(table.id()).orElseThrow().state());
            assertEquals(DdlLogPhase.ENGINE_DONE, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());

            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** CREATE rollback 只能删除 marker identity 推导出的 exact path；受控目录内的错绑路径同样必须 fail-closed。 */
    @Test
    void rejectsCreateMarkerWhosePathDoesNotMatchIdentity() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("mismatched-marker-tables");
        Path mismatched = tables.resolve("table_999_space_1024.ibd").toAbsolutePath().normalize();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("mismatched-marker-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("mismatched-marker-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            try {
                Files.createDirectories(tables);
                Files.write(mismatched, new byte[]{1});
            } catch (java.io.IOException e) {
                throw new AssertionError(e);
            }
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            repository.ddlLog().prepare(new DdlLogRecord(
                    new cn.zhangyis.db.storage.api.ddl.DdlUndoMarker(1, 2, 41),
                    DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED, SpaceId.of(1024), mismatched));

            assertThrows(DictionaryRecoveryException.class,
                    () -> new DictionaryDdlRecoveryService(control, repository,
                            new DictionaryObjectCache(16), storage.tableDdlStorageService(), tables)
                            .recover(Duration.ofSeconds(5)));
            assertTrue(Files.exists(mismatched), "mismatched marker must never authorize file deletion");
            assertEquals(DdlLogPhase.PREPARED,
                    repository.ddlLog().find(cn.zhangyis.db.dd.domain.DdlId.of(1)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /** DROP_PENDING recovery 物理删除前必须再次检查 barrier；超时保持 pending 和文件，阻止上层 OPEN。 */
    @Test
    void pendingDropRecoveryDoesNotCrossPurgeBarrier() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("pending-barrier-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("pending-barrier-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("pending-barrier-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(50), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            var table = ddl.createTable(MdlOwnerId.of(50), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = new DictionaryDdlService(control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables,
                    pending -> { throw new DictionaryDdlException("injected crash after DROP_PENDING"); });
            assertThrows(DictionaryDdlException.class, () -> crashing.dropTable(MdlOwnerId.of(50),
                    QualifiedTableName.of("app", "orders"), Duration.ofSeconds(5)));
            Path tableFile = table.storageBinding().orElseThrow().path();
            TablePurgeBarrier blocking = new TablePurgeBarrier() {
                @Override
                public void awaitUnreferenced(long tableId, Duration timeout) {
                    throw new TablePurgeBarrierTimeoutException(
                            "synthetic recovered table history reference: " + tableId);
                }

                @Override
                public int referenceCount(long tableId) {
                    return 1;
                }
            };

            assertThrows(TablePurgeBarrierTimeoutException.class,
                    () -> new DictionaryDdlRecoveryService(control, repository, cache,
                            storage.tableDdlStorageService(), tables, blocking).recover(Duration.ofMillis(20)));
            assertEquals(TableState.DROP_PENDING,
                    repository.findTableForRecovery(table.id()).orElseThrow().state());
            assertTrue(Files.exists(tableFile));
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
                    QualifiedTableName.of("app", "orders"), TableAccessIntent.READ,
                    Duration.ofSeconds(2))) {
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
                    MdlOwnerId.of(31), QualifiedTableName.of("app", "orders"), TableAccessIntent.READ,
                    Duration.ofSeconds(2)));
        } finally {
            storage.close();
        }

        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            assertFalse(Files.exists(tableFile), "startup must resume durable DROP_PENDING physical deletion");
            assertThrows(DictionaryObjectNotFoundException.class, () -> database.dictionary().openTable(
                    MdlOwnerId.of(32), QualifiedTableName.of("app", "orders"), TableAccessIntent.READ,
                    Duration.ofSeconds(2)));
        }
    }

    private static InternalCatalogStore durableThenThrow(InternalCatalogStore delegate) {
        return new InternalCatalogStore() {
            @Override
            public long append(List<CatalogRecord> records) {
                long committedLength = delegate.append(records);
                // 本接缝只模拟“普通 DD commit 已 durable 但响应丢失”。DDL_LOG(7) 是独立批次，
                // 必须正常返回，才能把故障精确放在 ACTIVE/DROP_PENDING 字典提交边界。
                if (records.size() == 1 && Byte.toUnsignedInt(records.getFirst().key()[0]) == 7) {
                    return committedLength;
                }
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

    private static DictionaryDdlService ddlWithFault(
            StorageEngine storage,
            DictionaryControlStore control,
            PersistentDictionaryRepository repository,
            DictionaryObjectCache cache,
            MetadataLockManager locks,
            Path tables,
            DictionaryDdlFaultInjector faultInjector) {
        return new DictionaryDdlService(control, repository, cache, locks,
                storage.tableDdlStorageService(), tables, TablePurgeBarrier.NONE, faultInjector);
    }

    private static CreateTableCommand command() {
        return new CreateTableCommand(QualifiedTableName.of("app", "orders"), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"), ColumnTypeDefinition.bigint(false, false))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                        List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0)))));
    }

    private static CreateTableCommand updateCommand() {
        return new CreateTableCommand(QualifiedTableName.of("app", "orders"), PageNo.of(128),
                List.of(new CreateColumnSpec(ObjectName.of("id"),
                                ColumnTypeDefinition.bigint(false, false)),
                        new CreateColumnSpec(ObjectName.of("value"),
                                ColumnTypeDefinition.integer(false, false))),
                List.of(new CreateIndexSpec(ObjectName.of("PRIMARY"), true, true,
                        List.of(new CreateIndexKeyPartSpec(ObjectName.of("id"), IndexOrder.ASC, 0)))));
    }

    private static CreateSecondaryIndexCommand secondaryIndexCommand() {
        return new CreateSecondaryIndexCommand(
                QualifiedTableName.of("app", "orders"),
                new CreateIndexSpec(ObjectName.of("idx_value"), false, false,
                        List.of(new CreateIndexKeyPartSpec(
                                ObjectName.of("value"), IndexOrder.ASC, 0))));
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }
}
