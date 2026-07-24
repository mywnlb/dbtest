package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaState;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.recovery.DictionaryDdlRecoveryService;
import cn.zhangyis.db.dd.recovery.DictionaryCleanSnapshotPublisher;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryException;
import cn.zhangyis.db.dd.recovery.OnlineIndexRecoveryRuntime;
import cn.zhangyis.db.dd.recovery.OnlineAlterRecoveryRuntime;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.dd.sdi.DictionarySdiCodec;
import cn.zhangyis.db.engine.DatabaseEngine;
import cn.zhangyis.db.engine.DatabaseAccessMode;
import cn.zhangyis.db.engine.adapter.DictionaryIndexMetadataResolver;
import cn.zhangyis.db.engine.adapter.DictionaryStorageMetadataMapper;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.engine.EngineTablespaceConfig;
import cn.zhangyis.db.common.exception.RecoveryExportWriteRejectedException;
import cn.zhangyis.db.dd.exception.TableRecoveryUnavailableException;
import cn.zhangyis.db.dd.recovery.backup.RecoveryBackupArtifact;
import cn.zhangyis.db.storage.api.TablePurgeBarrier;
import cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfo;
import cn.zhangyis.db.storage.api.ddl.SerializedDictionaryInfoException;
import cn.zhangyis.db.storage.api.ddl.StorageDefaultValue;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlAbortReason;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogHeader;
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
import cn.zhangyis.db.storage.fil.online.OnlineIndexChangeLogFiles;
import cn.zhangyis.db.storage.fil.online.OnlineAlterChangeLogFiles;
import cn.zhangyis.db.session.SessionOptions;
import cn.zhangyis.db.sql.executor.QueryResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
     * FORCE 隔离必须跨重启成为 DD 长期状态；可信备份只能经 HMAC incoming pair 把
     * RECOVERY_UNAVAILABLE→RECOVERY_DISCARDED→ACTIVE 完整收敛，导出实例本身不得写入。
     */
    @Test
    void isolatesObjectInForceModeAndRestoresOnlyTrustedBackup() throws Exception {
        QualifiedTableName name = QualifiedTableName.of("app", "orders");
        RecoveryBackupArtifact backup;
        SpaceId spaceId;
        long tableId;
        Path canonicalPath;

        // 1. 健康实例先写入可观察数据并生成本实例签名的 clean backup。
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(600), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            TableDefinition table = database.ddl().createTable(
                    MdlOwnerId.of(601), updateCommand(), Duration.ofSeconds(5));
            spaceId = table.storageBinding().orElseThrow().spaceId();
            tableId = table.id().value();
            canonicalPath = table.storageBinding().orElseThrow().path();
            try (var session = database.openSession(SessionOptions.defaults())) {
                session.execute("INSERT INTO app.orders (id,value) VALUES (1,77)");
            }
            backup = database.ddl().createRecoveryBackup(
                    MdlOwnerId.of(602), name, Duration.ofSeconds(5));
            assertTrue(Files.isRegularFile(backup.dataPath()));
            assertTrue(Files.isRegularFile(backup.manifestPath()));
        }

        // 2. FORCE 在 storage discovery 前原子持久化隔离，并把所有写入口封为导出只读。
        try (DatabaseEngine forced = new DatabaseEngine(
                config().withForceSkipRecovery(Set.of(spaceId)))) {
            forced.open();
            assertEquals(DatabaseAccessMode.RECOVERY_EXPORT_READ_ONLY,
                    forced.accessMode());
            assertEquals(1, forced.unavailableTables().size());
            assertThrows(RecoveryExportWriteRejectedException.class, forced::ddl);
            try (var session = forced.openSession(SessionOptions.defaults())) {
                assertThrows(TableRecoveryUnavailableException.class,
                        () -> session.execute("SELECT id FROM app.orders WHERE id=1"));
                assertThrows(RecoveryExportWriteRejectedException.class,
                        () -> session.execute(
                                "INSERT INTO app.orders (id,value) VALUES (2,88)"));
            }
        }

        // 3. 普通降级启动不挂载隔离空间；Java DDL raw DISCARD 不读取 page0。
        try (DatabaseEngine degraded = new DatabaseEngine(config())) {
            degraded.open();
            assertEquals(DatabaseAccessMode.DEGRADED, degraded.accessMode());
            degraded.ddl().discardTablespace(
                    MdlOwnerId.of(603), name, Duration.ofSeconds(5));
            assertFalse(Files.exists(canonicalPath));
            assertTrue(Files.exists(directory.resolve("tablespace-transfer")
                    .resolve("discarded")
                    .resolve("table_" + tableId + "_space_" + spaceId.value() + ".ibd")));

            // 管理员显式把 archive pair 放入固定 recovery-incoming；SQL/Java API 不能传任意主机路径。
            Path incomingRoot = directory.resolve("tablespace-transfer")
                    .resolve("recovery-incoming");
            Files.createDirectories(incomingRoot);
            String stem = "table_" + tableId + "_space_" + spaceId.value();
            Files.copy(backup.dataPath(), incomingRoot.resolve(stem + ".ibd"),
                    StandardCopyOption.COPY_ATTRIBUTES);
            Files.copy(backup.manifestPath(), incomingRoot.resolve(stem + ".manifest"),
                    StandardCopyOption.COPY_ATTRIBUTES);

            degraded.ddl().importTablespace(
                    MdlOwnerId.of(604), name, Duration.ofSeconds(5));
            assertEquals(DatabaseAccessMode.DEGRADED, degraded.accessMode(),
                    "组合根访问模式在本次 open 生命周期内保持稳定快照");
            try (var session = degraded.openSession(SessionOptions.defaults())) {
                QueryResult result = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT value FROM app.orders WHERE id=1"));
                assertEquals(1, result.rows().size());
            }
        }

        // 4. 再次普通启动从 ACTIVE DD discovery 挂载 replacement，访问模式恢复 NORMAL。
        try (DatabaseEngine reopened = new DatabaseEngine(config())) {
            reopened.open();
            assertEquals(DatabaseAccessMode.NORMAL, reopened.accessMode());
            try (var session = reopened.openSession(SessionOptions.defaults())) {
                QueryResult result = assertInstanceOf(QueryResult.class,
                        session.execute("SELECT value FROM app.orders WHERE id=1"));
                assertEquals(1, result.rows().size());
            }
        }
    }

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

    /** 正常 DROP INDEX 必须先发布不含目标的 exact DD，再回收 descriptor/segments，并可跨重启保持删除结果。 */
    @Test
    void dropsSecondaryIndexAndKeepsRemovalAcrossRestart() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(102), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            database.ddl().createTable(
                    MdlOwnerId.of(102), updateCommand(), Duration.ofSeconds(5));
            TableDefinition withIndex = database.ddl().createSecondaryIndex(
                    MdlOwnerId.of(103), secondaryIndexCommand(), Duration.ofSeconds(5));

            TableDefinition withoutIndex = database.ddl().dropSecondaryIndex(
                    MdlOwnerId.of(104), dropIndexCommand(), Duration.ofSeconds(5));

            assertEquals(1, withoutIndex.indexes().size());
            assertEquals(ObjectName.of("PRIMARY"), withoutIndex.indexes().getFirst().name());
            assertTrue(withoutIndex.version().compareTo(withIndex.version()) > 0);
            assertTrue(database.storage().tableDdlStorageService()
                    .readSecondaryIndexDrop(withoutIndex.storageBinding().orElseThrow()).isEmpty());
            OnlineDdlOperationSnapshot marker = database.onlineDdlControl().list().stream()
                    .filter(snapshot -> snapshot.identity().operation() == DdlLogOperation.DROP_INDEX)
                    .findFirst().orElseThrow();
            assertTrue(marker.cancelCapable());
            assertEquals(DdlControlState.FORWARD_ONLY, marker.controlState());
            assertTrue(marker.retirementFencePresent());
            assertEquals(withIndex.version().value(),
                    marker.identity().sourceVersion());
        }

        try (DatabaseEngine reopened = new DatabaseEngine(config())) {
            reopened.open();
            try (var lease = reopened.dictionary().openTable(
                    MdlOwnerId.of(105), QualifiedTableName.of("app", "orders"),
                    TableAccessIntent.READ, Duration.ofSeconds(5))) {
                assertEquals(List.of(ObjectName.of("PRIMARY")),
                        lease.table().indexes().stream().map(index -> index.name()).toList());
            }
        }
    }

    /** 聚簇索引与不存在索引必须在 identity reserve/footer 写入前拒绝，不能生成误导性的未决 DDL。 */
    @Test
    void rejectsClusteredOrMissingIndexBeforePreparingDrop() {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(106), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition table = database.ddl().createTable(
                    MdlOwnerId.of(106), updateCommand(), Duration.ofSeconds(5));

            assertThrows(DictionaryDdlException.class, () -> database.ddl().dropSecondaryIndex(
                    MdlOwnerId.of(107),
                    new DropSecondaryIndexCommand(
                            QualifiedTableName.of("app", "orders"), ObjectName.of("PRIMARY")),
                    Duration.ofSeconds(5)));
            assertThrows(DictionaryObjectNotFoundException.class,
                    () -> database.ddl().dropSecondaryIndex(
                            MdlOwnerId.of(108),
                            new DropSecondaryIndexCommand(
                                    QualifiedTableName.of("app", "orders"), ObjectName.of("missing_idx")),
                            Duration.ofSeconds(5)));

            try (var lease = database.dictionary().openTable(
                    MdlOwnerId.of(109), QualifiedTableName.of("app", "orders"),
                    TableAccessIntent.READ, Duration.ofSeconds(5))) {
                assertEquals(1, lease.table().indexes().size());
            }
            assertTrue(database.storage().tableDdlStorageService()
                    .readSecondaryIndexDrop(table.storageBinding().orElseThrow()).isEmpty());
        }
    }

    /**
     * 已打开的 READ metadata lease 持有 table shared MDL 时，DROP INDEX 的 table X 必须有界等待；
     * lease 释放后继续执行，期间不得提前改 DD 或释放 segment。
     *
     * @throws Exception future 的有界等待或测试线程调度失败时保留原始异常
     */
    @Test
    void waitsForExistingMetadataLeaseBeforeDroppingIndex() throws Exception {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(115), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            database.ddl().createTable(
                    MdlOwnerId.of(115), updateCommand(), Duration.ofSeconds(5));
            TableDefinition withIndex = database.ddl().createSecondaryIndex(
                    MdlOwnerId.of(116), secondaryIndexCommand(), Duration.ofSeconds(5));
            var lease = database.dictionary().openTable(
                    MdlOwnerId.of(117), QualifiedTableName.of("app", "orders"),
                    TableAccessIntent.READ, Duration.ofSeconds(5));
            try (var executor = Executors.newSingleThreadExecutor()) {
                var drop = executor.submit(() -> database.ddl().dropSecondaryIndex(
                        MdlOwnerId.of(118), dropIndexCommand(), Duration.ofSeconds(5)));

                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> drop.get(200, TimeUnit.MILLISECONDS));
                assertEquals(2, lease.table().indexes().size());
                assertTrue(database.storage().tableDdlStorageService()
                        .readSecondaryIndexDrop(withIndex.storageBinding().orElseThrow()).isPresent(),
                        "Online DROP prepares durable ownership while final X waits for the old lease");

                lease.close();
                assertEquals(1, drop.get(3, TimeUnit.SECONDS).indexes().size());
            } finally {
                lease.close();
            }
        }
    }

    /** admin取消final X pending的Online DROP必须只移除DDL升级请求，回滚descriptor并保留source索引与读lease。 */
    @Test
    void cancelsOnlineDropWhileFinalMetadataLockIsPending() throws Exception {
        try (DatabaseEngine database = new DatabaseEngine(config())) {
            database.open();
            database.ddl().createSchema(
                    MdlOwnerId.of(122), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            database.ddl().createTable(
                    MdlOwnerId.of(122), updateCommand(), Duration.ofSeconds(5));
            TableDefinition source = database.ddl().createSecondaryIndex(
                    MdlOwnerId.of(123), secondaryIndexCommand(), Duration.ofSeconds(5));
            var sourceLease = database.dictionary().openTable(
                    MdlOwnerId.of(124), QualifiedTableName.of("app", "orders"),
                    TableAccessIntent.READ, Duration.ofSeconds(5));
            try (var executor = Executors.newSingleThreadExecutor()) {
                var drop = executor.submit(() -> database.ddl().dropSecondaryIndex(
                        MdlOwnerId.of(125), dropIndexCommand(), Duration.ofSeconds(5)));
                long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
                OnlineDdlOperationSnapshot pending = null;
                while (System.nanoTime() < deadline) {
                    pending = database.onlineDdlControl().list().stream()
                            .filter(snapshot -> snapshot.identity().operation()
                                    == DdlLogOperation.DROP_INDEX)
                            .filter(snapshot -> snapshot.runtimePhase()
                                    == OnlineDdlRuntimePhase.WAITING_FINAL_MDL)
                            .findFirst().orElse(null);
                    if (pending != null) {
                        break;
                    }
                    Thread.sleep(10);
                }
                assertTrue(pending != null, "DROP must expose its pending final-MDL phase");
                assertTrue(database.storage().tableDdlStorageService()
                        .readSecondaryIndexDrop(source.storageBinding().orElseThrow()).isPresent());

                OnlineDdlCancelResult cancel = database.onlineDdlControl().requestCancel(
                        pending.identity().ddlId(), OnlineDdlCancelRequest.admin(
                                DdlCancellationReason.USER_REQUEST, 12),
                        Duration.ofSeconds(2));

                assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE, cancel.outcome());
                var executionFailure = assertThrows(
                        java.util.concurrent.ExecutionException.class,
                        () -> drop.get(3, TimeUnit.SECONDS));
                assertInstanceOf(OnlineDdlCancellationException.class,
                        executionFailure.getCause());
                assertEquals(2, sourceLease.table().indexes().size(),
                        "cancel must not revoke the granted source metadata lease");
                assertTrue(database.storage().tableDdlStorageService()
                        .readSecondaryIndexDrop(source.storageBinding().orElseThrow()).isEmpty());
                assertEquals(OnlineDdlTerminalResult.ROLLED_BACK,
                        database.onlineDdlControl().find(pending.identity().ddlId())
                                .orElseThrow().terminalResult());
            } finally {
                sourceLease.close();
            }
        }
    }

    /**
     * target DD发布与segment回收必须分离：高水位内history尚在时descriptor/segment保留，但gate已经清除，
     * 新DML可按不含旧索引的target metadata继续；purge越过fence后DROP才进入terminal。
     *
     * @throws Exception future、latch或测试线程调度失败时保留原始异常
     */
    @Test
    void publishesOnlineDropBeforeRetiringIndexSegments() throws Exception {
        Path tables = directory.resolve("online-drop-retirement-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-drop-retirement-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-drop-retirement-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            StorageEngine storage = new StorageEngine(config());
            storage.configureIndexMetadataResolver(new DictionaryIndexMetadataResolver(repository));
            storage.open();
            try {
                DictionaryObjectCache cache = new DictionaryObjectCache(16);
                MetadataLockManager locks = new MetadataLockManager(8, 128);
                DictionaryDdlService base = new DictionaryDdlService(
                        control, repository, cache, locks,
                        storage.tableDdlStorageService(), tables);
                base.createSchema(MdlOwnerId.of(119), ObjectName.of("app"),
                        1, 1, Duration.ofSeconds(5));
                base.createTable(MdlOwnerId.of(119), updateCommand(), Duration.ofSeconds(5));
                TableDefinition source = base.createSecondaryIndex(
                        MdlOwnerId.of(120), secondaryIndexCommand(), Duration.ofSeconds(5));
                var sourceMetadata = new DictionaryStorageMetadataMapper().map(source);
                long schemaVersion = sourceMetadata.tableIndexes().schemaVersion();
                LogicalRecord before = new LogicalRecord(schemaVersion,
                        List.of(new ColumnValue.IntValue(1), new ColumnValue.IntValue(10)),
                        false, RecordType.CONVENTIONAL);
                LogicalRecord after = new LogicalRecord(schemaVersion,
                        List.of(new ColumnValue.IntValue(1), new ColumnValue.IntValue(20)),
                        false, RecordType.CONVENTIONAL);
                var insert = storage.transactionManager().begin(TransactionOptions.defaults());
                storage.tableDmlService().insert(new TableInsertCommand(
                        insert, sourceMetadata.tableIndexes(), before,
                        Optional.empty(), Duration.ofSeconds(5)));
                storage.tableDmlService().commit(new DmlCommitCommand(
                        insert, DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofSeconds(5)));
                var update = storage.transactionManager().begin(TransactionOptions.defaults());
                storage.tableDmlService().update(new TableUpdateCommand(
                        update, sourceMetadata.tableIndexes(),
                        new SearchKey(List.of(new ColumnValue.IntValue(1))), after,
                        Duration.ofSeconds(5)));
                storage.tableDmlService().commit(new DmlCommitCommand(
                        update, DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofSeconds(5)));
                assertEquals(1, storage.tablePurgeBarrier().referenceCount(source.id().value()));

                CountDownLatch retirementEntered = new CountDownLatch(1);
                CountDownLatch inspectPublishedTarget = new CountDownLatch(1);
                IndexRetirementBarrier delegate = new DefaultIndexRetirementBarrier(
                        storage.indexRetirementHistoryBarrier(), cache);
                IndexRetirementBarrier observed = new IndexRetirementBarrier() {
                    @Override
                    public DdlRetirementFence captureIndexFence(
                            long tableId, long sourceVersion, long indexId,
                            long descriptorGeneration, long ownerDdlId) {
                        return delegate.captureIndexFence(tableId, sourceVersion, indexId,
                                descriptorGeneration, ownerDdlId);
                    }

                    @Override
                    public void awaitIndexSafe(DdlRetirementFence fence, Duration timeout) {
                        retirementEntered.countDown();
                        try {
                            if (!inspectPublishedTarget.await(2, TimeUnit.SECONDS)) {
                                throw new cn.zhangyis.db.common.exception.DatabaseRuntimeException(
                                        "test did not release retirement inspection latch");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new cn.zhangyis.db.common.exception.DatabaseRuntimeException(
                                    "test retirement wait interrupted", interrupted);
                        }
                        delegate.awaitIndexSafe(fence, timeout);
                    }
                };
                OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(8);
                DictionaryDdlService online = new DictionaryDdlService(
                        control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                        storage.tablePurgeBarrier(), DictionaryDdlFaultInjector.NO_OP,
                        DictionaryCleanSnapshotPublisher.noOp(), new OnlineIndexBuildRuntime(
                        storage.onlineDdlTableGate(), config().onlineDdlConfig(),
                        new OnlineIndexChangeLogFiles(
                                config().onlineDdlDirectory(), config().onlineDdlConfig()),
                        storage.typeCodecRegistry()), registry, observed);

                try (var executor = Executors.newSingleThreadExecutor()) {
                    var drop = executor.submit(() -> online.dropSecondaryIndex(
                            MdlOwnerId.of(121), dropIndexCommand(), Duration.ofSeconds(5)));
                    assertTrue(retirementEntered.await(3, TimeUnit.SECONDS));

                    TableDefinition target = repository.findTable(source.id()).orElseThrow();
                    assertEquals(1, target.indexes().size(),
                            "logical target must commit before retirement wait");
                    assertTrue(storage.tableDdlStorageService()
                            .readSecondaryIndexDrop(target.storageBinding().orElseThrow()).isPresent(),
                            "physical owner remains durable until fence becomes safe");
                    assertEquals(cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase.ABSENT,
                            storage.onlineDdlTableGate().phase(source.id().value()),
                            "target DML admission must be reopened before slow retirement");
                    assertFalse(drop.isDone());

                    var targetMetadata = new DictionaryStorageMetadataMapper().map(target);
                    LogicalRecord targetRow = new LogicalRecord(
                            targetMetadata.tableIndexes().schemaVersion(),
                            List.of(new ColumnValue.IntValue(2), new ColumnValue.IntValue(30)),
                            false, RecordType.CONVENTIONAL);
                    var targetInsert = storage.transactionManager().begin(TransactionOptions.defaults());
                    storage.tableDmlService().insert(new TableInsertCommand(
                            targetInsert, targetMetadata.tableIndexes(), targetRow,
                            Optional.empty(), Duration.ofSeconds(5)));
                    storage.tableDmlService().commit(new DmlCommitCommand(
                            targetInsert, DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofSeconds(5)));

                    assertEquals(1, storage.purgeCoordinator().runBatch(1).purgedLogs());
                    inspectPublishedTarget.countDown();
                    assertEquals(1, drop.get(3, TimeUnit.SECONDS).indexes().size());
                } finally {
                    inspectPublishedTarget.countDown();
                }

                assertEquals(OnlineDdlTerminalResult.COMPLETED,
                        registry.list().getFirst().terminalResult());
            } finally {
                storage.close();
            }
        }
    }

    /** target DD已durable而marker仍PREPARED时，恢复必须验证FORWARD_ONLY/fence后继续退休，不能把索引加回DD。 */
    @Test
    void recoversOnlineDropAfterTargetDictionaryPublishCrash() {
        Path tables = directory.resolve("online-drop-target-crash-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-drop-target-crash-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-drop-target-crash-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            StorageEngine storage = new StorageEngine(config());
            storage.configureIndexMetadataResolver(new DictionaryIndexMetadataResolver(repository));
            storage.open();
            try {
                DictionaryObjectCache cache = new DictionaryObjectCache(16);
                MetadataLockManager locks = new MetadataLockManager(8, 128);
                DictionaryDdlService base = new DictionaryDdlService(
                        control, repository, cache, locks,
                        storage.tableDdlStorageService(), tables);
                base.createSchema(MdlOwnerId.of(126), ObjectName.of("app"),
                        1, 1, Duration.ofSeconds(5));
                base.createTable(MdlOwnerId.of(126), updateCommand(), Duration.ofSeconds(5));
                TableDefinition source = base.createSecondaryIndex(
                        MdlOwnerId.of(127), secondaryIndexCommand(), Duration.ofSeconds(5));
                OnlineDdlOperationRegistry liveRegistry = new OnlineDdlOperationRegistry(8);
                OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                        config().onlineDdlDirectory(), config().onlineDdlConfig());
                DictionaryDdlService crashing = new DictionaryDdlService(
                        control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                        storage.tablePurgeBarrier(), new DictionaryDdlFaultInjector() {
                    @Override public void afterDropPendingPublished(TableDefinition pending) { }
                    @Override
                    public void afterDropIndexDictionaryPublished(TableDefinition target) {
                        throw new SimulatedProcessCrashError();
                    }
                }, DictionaryCleanSnapshotPublisher.noOp(), new OnlineIndexBuildRuntime(
                        storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                        storage.typeCodecRegistry()), liveRegistry,
                        new DefaultIndexRetirementBarrier(
                                storage.indexRetirementHistoryBarrier(), cache));

                assertThrows(SimulatedProcessCrashError.class,
                        () -> crashing.dropSecondaryIndex(
                                MdlOwnerId.of(128), dropIndexCommand(), Duration.ofSeconds(5)));
                DdlLogRecord crashed = repository.ddlLog().unresolved().stream()
                        .filter(record -> record.operation() == DdlLogOperation.DROP_INDEX)
                        .findFirst().orElseThrow();
                assertEquals(DdlLogPhase.PREPARED, crashed.phase());
                assertEquals(DdlControlState.FORWARD_ONLY, crashed.controlState());
                assertTrue(crashed.retirementFence().isPresent());
                assertEquals(1, repository.findTable(source.id()).orElseThrow().indexes().size());
                assertTrue(storage.tableDdlStorageService().readSecondaryIndexDrop(
                        source.storageBinding().orElseThrow()).isPresent());

                OnlineDdlOperationRegistry recoveryRegistry = new OnlineDdlOperationRegistry(8);
                new DictionaryDdlRecoveryService(
                        control, repository, cache, storage.tableDdlStorageService(), tables,
                        storage.tablePurgeBarrier(), new OnlineIndexRecoveryRuntime(
                        config().onlineDdlConfig(), logFiles, storage.typeCodecRegistry()),
                        recoveryRegistry, new DefaultIndexRetirementBarrier(
                        storage.indexRetirementHistoryBarrier(), cache))
                        .recover(Duration.ofSeconds(5));

                TableDefinition recovered = repository.findTable(source.id()).orElseThrow();
                assertEquals(1, recovered.indexes().size());
                assertTrue(storage.tableDdlStorageService()
                        .readSecondaryIndexDrop(recovered.storageBinding().orElseThrow()).isEmpty());
                assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                        .find(cn.zhangyis.db.dd.domain.DdlId.of(
                                crashed.marker().ddlOperationId())).orElseThrow().phase());
                assertEquals(OnlineDdlTerminalResult.COMPLETED,
                        recoveryRegistry.list().getFirst().terminalResult());
            } finally {
                storage.close();
            }
        }
    }

    /** source DD仍可见但FORWARD_ONLY已durable时，恢复必须从source派生target并继续退休，禁止回滚descriptor。 */
    @Test
    void recoversOnlineDropFromSourceAfterForwardFenceCrash() {
        Path tables = directory.resolve("online-drop-forward-crash-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-drop-forward-crash-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-drop-forward-crash-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            StorageEngine storage = new StorageEngine(config());
            storage.configureIndexMetadataResolver(new DictionaryIndexMetadataResolver(repository));
            storage.open();
            try {
                DictionaryObjectCache cache = new DictionaryObjectCache(16);
                MetadataLockManager locks = new MetadataLockManager(8, 128);
                DictionaryDdlService base = new DictionaryDdlService(
                        control, repository, cache, locks,
                        storage.tableDdlStorageService(), tables);
                base.createSchema(MdlOwnerId.of(129), ObjectName.of("app"),
                        1, 1, Duration.ofSeconds(5));
                base.createTable(MdlOwnerId.of(129), updateCommand(), Duration.ofSeconds(5));
                TableDefinition source = base.createSecondaryIndex(
                        MdlOwnerId.of(130), secondaryIndexCommand(), Duration.ofSeconds(5));
                OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                        config().onlineDdlDirectory(), config().onlineDdlConfig());
                DictionaryDdlService crashing = new DictionaryDdlService(
                        control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                        storage.tablePurgeBarrier(), new DictionaryDdlFaultInjector() {
                    @Override public void afterDropPendingPublished(TableDefinition pending) { }
                    @Override
                    public void afterDropIndexForwardFenced(DdlLogRecord forwardFenced) {
                        throw new SimulatedProcessCrashError();
                    }
                }, DictionaryCleanSnapshotPublisher.noOp(), new OnlineIndexBuildRuntime(
                        storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                        storage.typeCodecRegistry()), new OnlineDdlOperationRegistry(8),
                        new DefaultIndexRetirementBarrier(
                                storage.indexRetirementHistoryBarrier(), cache));

                assertThrows(SimulatedProcessCrashError.class,
                        () -> crashing.dropSecondaryIndex(
                                MdlOwnerId.of(131), dropIndexCommand(), Duration.ofSeconds(5)));
                DdlLogRecord forward = repository.ddlLog().unresolved().stream()
                        .filter(record -> record.operation() == DdlLogOperation.DROP_INDEX)
                        .findFirst().orElseThrow();
                assertEquals(DdlLogPhase.PREPARED, forward.phase());
                assertEquals(DdlControlState.FORWARD_ONLY, forward.controlState());
                assertEquals(2, repository.findTable(source.id()).orElseThrow().indexes().size(),
                        "crash boundary must still expose source DD");

                new DictionaryDdlRecoveryService(
                        control, repository, cache, storage.tableDdlStorageService(), tables,
                        storage.tablePurgeBarrier(), new OnlineIndexRecoveryRuntime(
                        config().onlineDdlConfig(), logFiles, storage.typeCodecRegistry()),
                        new OnlineDdlOperationRegistry(8), new DefaultIndexRetirementBarrier(
                        storage.indexRetirementHistoryBarrier(), cache))
                        .recover(Duration.ofSeconds(5));

                TableDefinition recovered = repository.findTable(source.id()).orElseThrow();
                assertEquals(1, recovered.indexes().size());
                assertTrue(storage.tableDdlStorageService()
                        .readSecondaryIndexDrop(recovered.storageBinding().orElseThrow()).isEmpty());
                assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                        .find(cn.zhangyis.db.dd.domain.DdlId.of(
                                forward.marker().ddlOperationId())).orElseThrow().phase());
            } finally {
                storage.close();
            }
        }
    }

    /** durable取消在descriptor后立即崩溃，恢复只能清footer并保留source索引，不能因descriptor存在而猜测前滚。 */
    @Test
    void recoversCancelledOnlineDropAfterDescriptorCrash() {
        Path tables = directory.resolve("online-drop-cancel-crash-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-drop-cancel-crash-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-drop-cancel-crash-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            StorageEngine storage = new StorageEngine(config());
            storage.configureIndexMetadataResolver(new DictionaryIndexMetadataResolver(repository));
            storage.open();
            try {
                DictionaryObjectCache cache = new DictionaryObjectCache(16);
                MetadataLockManager locks = new MetadataLockManager(8, 128);
                DictionaryDdlService base = new DictionaryDdlService(
                        control, repository, cache, locks,
                        storage.tableDdlStorageService(), tables);
                base.createSchema(MdlOwnerId.of(132), ObjectName.of("app"),
                        1, 1, Duration.ofSeconds(5));
                base.createTable(MdlOwnerId.of(132), updateCommand(), Duration.ofSeconds(5));
                TableDefinition source = base.createSecondaryIndex(
                        MdlOwnerId.of(133), secondaryIndexCommand(), Duration.ofSeconds(5));
                OnlineDdlOperationRegistry liveRegistry = new OnlineDdlOperationRegistry(8);
                OnlineDdlControlService controlService = new OnlineDdlControlService(
                        repository.ddlLog(), liveRegistry, identity -> {
                    locks.cancelPending(MdlOwnerId.of(identity.ownerId()));
                    var phase = storage.onlineDdlTableGate().phase(identity.tableId());
                    if (phase != cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase.ABSENT
                            && phase != cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase.ABORTING) {
                        storage.onlineDdlTableGate().beginAbort(
                                OnlineIndexBuildId.of(identity.ddlId().value()),
                                OnlineDdlAbortReason.CANCELLED);
                    }
                });
                AtomicReference<OnlineDdlCancelResult> accepted = new AtomicReference<>();
                OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                        config().onlineDdlDirectory(), config().onlineDdlConfig());
                DictionaryDdlService crashing = new DictionaryDdlService(
                        control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                        storage.tablePurgeBarrier(), new DictionaryDdlFaultInjector() {
                    @Override public void afterDropPendingPublished(TableDefinition pending) { }
                    @Override
                    public void afterDropIndexStaged(
                            cn.zhangyis.db.storage.api.ddl.SecondaryIndexDropDescriptor descriptor) {
                        accepted.set(controlService.requestCancel(
                                cn.zhangyis.db.dd.domain.DdlId.of(descriptor.ddlOperationId()),
                                OnlineDdlCancelRequest.admin(
                                        DdlCancellationReason.USER_REQUEST, 13),
                                Duration.ofSeconds(2)));
                        throw new SimulatedProcessCrashError();
                    }
                }, DictionaryCleanSnapshotPublisher.noOp(), new OnlineIndexBuildRuntime(
                        storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                        storage.typeCodecRegistry()), liveRegistry,
                        new DefaultIndexRetirementBarrier(
                                storage.indexRetirementHistoryBarrier(), cache));

                assertThrows(SimulatedProcessCrashError.class,
                        () -> crashing.dropSecondaryIndex(
                                MdlOwnerId.of(134), dropIndexCommand(), Duration.ofSeconds(5)));
                assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE,
                        accepted.get().outcome());
                DdlLogRecord cancelled = repository.ddlLog().unresolved().stream()
                        .filter(record -> record.operation() == DdlLogOperation.DROP_INDEX)
                        .findFirst().orElseThrow();
                assertEquals(DdlControlState.CANCEL_REQUESTED, cancelled.controlState());
                assertTrue(storage.tableDdlStorageService()
                        .readSecondaryIndexDrop(source.storageBinding().orElseThrow()).isPresent());

                new DictionaryDdlRecoveryService(
                        control, repository, cache, storage.tableDdlStorageService(), tables,
                        storage.tablePurgeBarrier(), new OnlineIndexRecoveryRuntime(
                        config().onlineDdlConfig(), logFiles, storage.typeCodecRegistry()),
                        new OnlineDdlOperationRegistry(8), new DefaultIndexRetirementBarrier(
                        storage.indexRetirementHistoryBarrier(), cache))
                        .recover(Duration.ofSeconds(5));

                assertEquals(2, repository.findTable(source.id()).orElseThrow().indexes().size());
                assertTrue(storage.tableDdlStorageService()
                        .readSecondaryIndexDrop(source.storageBinding().orElseThrow()).isEmpty());
                DdlLogRecord rolledBack = repository.ddlLog().find(
                        cn.zhangyis.db.dd.domain.DdlId.of(
                                cancelled.marker().ddlOperationId())).orElseThrow();
                assertEquals(DdlLogPhase.ROLLED_BACK, rolledBack.phase());
                assertEquals(DdlControlState.CANCEL_REQUESTED, rolledBack.controlState());
            } finally {
                storage.close();
            }
        }
    }

    /** 旧 DD 仍包含目标索引时，恢复只能清 DROP descriptor，不能回收仍被 committed metadata 引用的 segment。 */
    @Test
    void rollsBackDropIndexAfterDescriptorStagingCrash() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("drop-index-staged-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("drop-index-staged-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("drop-index-staged-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(130), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            base.createTable(MdlOwnerId.of(130), updateCommand(), Duration.ofSeconds(5));
            TableDefinition withIndex = base.createSecondaryIndex(
                    MdlOwnerId.of(131), secondaryIndexCommand(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterDropIndexStaged(
                                cn.zhangyis.db.storage.api.ddl.SecondaryIndexDropDescriptor descriptor) {
                            throw new DictionaryDdlException(
                                    "injected crash after DROP INDEX descriptor stage");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> crashing.dropSecondaryIndex(
                    MdlOwnerId.of(132), dropIndexCommand(), Duration.ofSeconds(5)));
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexDrop(withIndex.storageBinding().orElseThrow()).isPresent());

            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            TableDefinition recovered = repository.findTable(withIndex.id()).orElseThrow();
            assertEquals(2, recovered.indexes().size());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexDrop(recovered.storageBinding().orElseThrow()).isEmpty());
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(4)).orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /**
     * 新 DD 已 durable 而 DDL marker 仍为 PREPARED 时，恢复必须以前者为提交真相，完成两个 segment 回收并补终态。
     */
    @Test
    void finishesDropIndexFromPreparedMarkerAfterDictionaryPublishCrash() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("drop-index-prepared-new-dd-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("drop-index-prepared-new-dd-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("drop-index-prepared-new-dd-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(140), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            base.createTable(MdlOwnerId.of(140), updateCommand(), Duration.ofSeconds(5));
            TableDefinition withIndex = base.createSecondaryIndex(
                    MdlOwnerId.of(141), secondaryIndexCommand(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterDropIndexDictionaryPublished(TableDefinition active) {
                            throw new DictionaryDdlException(
                                    "injected crash after DROP INDEX DD publish");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> crashing.dropSecondaryIndex(
                    MdlOwnerId.of(142), dropIndexCommand(), Duration.ofSeconds(5)));
            TableDefinition committed = repository.findTable(withIndex.id()).orElseThrow();
            assertEquals(1, committed.indexes().size());
            assertEquals(DdlLogPhase.PREPARED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(4)).orElseThrow().phase());

            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            TableDefinition recovered = repository.findTable(withIndex.id()).orElseThrow();
            assertEquals(1, recovered.indexes().size());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexDrop(recovered.storageBinding().orElseThrow()).isEmpty());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(4)).orElseThrow().phase());
        } finally {
            storage.close();
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

    /**
     * PREPARED marker force 后取消必须在创建 gate/row-log/descriptor 前被 coordinator 观察，并把
     * durable marker 收敛为 ROLLED_BACK；tracker 终点不能把用户取消误报为发布失败。
     */
    @Test
    void cancelsOnlineCreateIndexAfterDurablePrepareBeforePhysicalResources() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("online-index-cancel-prepared-tables");
        OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                config().onlineDdlDirectory(), config().onlineDdlConfig());
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-index-cancel-prepared-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-index-cancel-prepared-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(160), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            TableDefinition source = base.createTable(
                    MdlOwnerId.of(160), updateCommand(), Duration.ofSeconds(5));
            OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(8);
            OnlineDdlControlService controlService = new OnlineDdlControlService(
                    repository.ddlLog(), registry,
                    identity -> locks.cancelPending(MdlOwnerId.of(identity.ownerId())));
            AtomicReference<OnlineDdlCancelResult> accepted = new AtomicReference<>();
            DictionaryDdlService cancellable = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new DictionaryDdlFaultInjector() {
                @Override public void afterDropPendingPublished(TableDefinition pending) { }
                @Override
                public void afterCreateIndexPrepared(DdlLogRecord prepared) {
                    accepted.set(controlService.requestCancel(
                            cn.zhangyis.db.dd.domain.DdlId.of(
                                    prepared.marker().ddlOperationId()),
                            OnlineDdlCancelRequest.admin(
                                    DdlCancellationReason.USER_REQUEST, 7),
                            Duration.ofSeconds(2)));
                }
            }, DictionaryCleanSnapshotPublisher.noOp(), new OnlineIndexBuildRuntime(
                    storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                    storage.typeCodecRegistry()), registry);

            assertThrows(OnlineDdlCancellationException.class,
                    () -> cancellable.createSecondaryIndex(
                            MdlOwnerId.of(161), secondaryIndexCommand(), Duration.ofSeconds(5)));

            assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE,
                    accepted.get().outcome());
            DdlLogRecord marker = repository.ddlLog().records().stream()
                    .filter(record -> record.operation() == DdlLogOperation.CREATE_INDEX)
                    .findFirst().orElseThrow();
            assertEquals(DdlLogPhase.ROLLED_BACK, marker.phase());
            assertEquals(DdlControlState.CANCEL_REQUESTED, marker.controlState());
            assertEquals(1, repository.findTable(source.id()).orElseThrow().indexes().size());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(source.storageBinding().orElseThrow()).isEmpty());
            assertFalse(Files.exists(marker.auxiliaryPath().orElseThrow()));
            assertEquals(OnlineDdlTerminalResult.ROLLED_BACK,
                    registry.find(cn.zhangyis.db.dd.domain.DdlId.of(
                            marker.marker().ddlOperationId())).orElseThrow().terminalResult());
        } finally {
            storage.close();
        }
    }

    /**
     * durable cancel 已成功但 coordinator 尚未把 ABORT_REQUIRED 写入 row-log 时立即崩溃，
     * recovery 仍必须以 marker control 为真相回收 descriptor/segments，不得重建并前滚索引。
     */
    @Test
    void recoversDurableCancelBeforeRowLogAbortFrame() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("online-index-cancel-crash-tables");
        OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                config().onlineDdlDirectory(), config().onlineDdlConfig());
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-index-cancel-crash-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-index-cancel-crash-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(162), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            TableDefinition source = base.createTable(
                    MdlOwnerId.of(162), updateCommand(), Duration.ofSeconds(5));
            OnlineDdlOperationRegistry liveRegistry = new OnlineDdlOperationRegistry(8);
            OnlineDdlControlService controlService = new OnlineDdlControlService(
                    repository.ddlLog(), liveRegistry, identity -> {
                locks.cancelPending(MdlOwnerId.of(identity.ownerId()));
                var phase = storage.onlineDdlTableGate().phase(identity.tableId());
                if (phase != cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase.ABSENT
                        && phase != cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase.ABORTING) {
                    storage.onlineDdlTableGate().beginAbort(
                            OnlineIndexBuildId.of(identity.ddlId().value()),
                            OnlineDdlAbortReason.CANCELLED);
                }
            });
            DictionaryDdlService crashing = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new DictionaryDdlFaultInjector() {
                @Override public void afterDropPendingPublished(TableDefinition pending) { }
                @Override
                public void afterCreateIndexCaptureDurable(DdlLogRecord prepared) {
                    OnlineDdlCancelResult result = controlService.requestCancel(
                            cn.zhangyis.db.dd.domain.DdlId.of(
                                    prepared.marker().ddlOperationId()),
                            OnlineDdlCancelRequest.admin(
                                    DdlCancellationReason.USER_REQUEST, 8),
                            Duration.ofSeconds(2));
                    assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE, result.outcome());
                    throw new SimulatedProcessCrashError();
                }
            }, DictionaryCleanSnapshotPublisher.noOp(), new OnlineIndexBuildRuntime(
                    storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                    storage.typeCodecRegistry()), liveRegistry);

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.createSecondaryIndex(
                    MdlOwnerId.of(163), secondaryIndexCommand(), Duration.ofSeconds(5)));
            DdlLogRecord cancelled = repository.ddlLog().unresolved().getFirst();
            assertEquals(DdlControlState.CANCEL_REQUESTED, cancelled.controlState());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(source.storageBinding().orElseThrow()).isPresent());
            assertTrue(Files.exists(cancelled.auxiliaryPath().orElseThrow()));

            OnlineDdlOperationRegistry recoveryRegistry = new OnlineDdlOperationRegistry(8);
            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new OnlineIndexRecoveryRuntime(
                    config().onlineDdlConfig(), logFiles, storage.typeCodecRegistry()),
                    recoveryRegistry).recover(Duration.ofSeconds(5));

            DdlLogRecord rolledBack = repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(
                            cancelled.marker().ddlOperationId())).orElseThrow();
            assertEquals(DdlLogPhase.ROLLED_BACK, rolledBack.phase());
            assertEquals(DdlControlState.CANCEL_REQUESTED, rolledBack.controlState());
            assertEquals(1, repository.findTable(source.id()).orElseThrow().indexes().size());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(source.storageBinding().orElseThrow()).isEmpty());
            assertFalse(Files.exists(cancelled.auxiliaryPath().orElseThrow()));
            assertEquals(OnlineDdlTerminalResult.ROLLED_BACK,
                    recoveryRegistry.find(cn.zhangyis.db.dd.domain.DdlId.of(
                            cancelled.marker().ddlOperationId())).orElseThrow().terminalResult());
        } finally {
            storage.close();
        }
    }

    /**
     * Online CREATE INDEX 越过 ENGINE_DONE 后，旧 DD 不能再触发 staged rollback；同步启动恢复必须以
     * durable RECONCILED 证据前滚 exact target DD，并在 footer 清空后删除 build-owned row-log。
     */
    @Test
    void finishesOnlineCreateIndexFromEngineDoneDuringRecovery() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("online-index-engine-done-tables");
        OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                config().onlineDdlDirectory(), config().onlineDdlConfig());
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-index-engine-done-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-index-engine-done-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(112), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            TableDefinition table = base.createTable(
                    MdlOwnerId.of(112), updateCommand(), Duration.ofSeconds(5));
            OnlineIndexBuildRuntime buildRuntime = new OnlineIndexBuildRuntime(
                    storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                    storage.typeCodecRegistry());
            DictionaryDdlService crashing = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new DictionaryDdlFaultInjector() {
                @Override
                public void afterDropPendingPublished(TableDefinition pending) {
                }

                @Override
                public void afterCreateIndexEngineDone(DdlLogRecord engineDone) {
                    throw new DictionaryDdlException(
                            "injected online crash after CREATE INDEX ENGINE_DONE");
                }
            }, DictionaryCleanSnapshotPublisher.noOp(), buildRuntime);

            assertThrows(DictionaryDdlException.class, () -> crashing.createSecondaryIndex(
                    MdlOwnerId.of(113), secondaryIndexCommand(), Duration.ofSeconds(5)));
            DdlLogRecord unresolved = repository.ddlLog().unresolved().getFirst();
            assertEquals(DdlLogPhase.ENGINE_DONE, unresolved.phase());
            assertTrue(Files.exists(unresolved.auxiliaryPath().orElseThrow()));
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(table.storageBinding().orElseThrow()).isPresent());

            OnlineDdlOperationRegistry recoveryRegistry = new OnlineDdlOperationRegistry(8);
            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new OnlineIndexRecoveryRuntime(
                    config().onlineDdlConfig(), logFiles, storage.typeCodecRegistry()),
                    recoveryRegistry)
                    .recover(Duration.ofSeconds(5));

            TableDefinition recovered = repository.findTable(table.id()).orElseThrow();
            assertEquals(ObjectName.of("idx_value"), recovered.indexes().getLast().name());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(recovered.storageBinding().orElseThrow()).isEmpty());
            assertFalse(Files.exists(unresolved.auxiliaryPath().orElseThrow()));
            assertEquals(OnlineDdlTerminalResult.COMPLETED,
                    recoveryRegistry.find(cn.zhangyis.db.dd.domain.DdlId.of(3))
                            .orElseThrow().terminalResult());
        } finally {
            storage.close();
        }
    }

    /** committed DD 不得被损坏 row-log 推翻；恢复应以 exact DD binding 验证 B+Tree 后完成 footer/terminal 清理。 */
    @Test
    void finishesCommittedOnlineIndexWhenRowLogIsCorrupted() throws Exception {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("online-index-corrupt-committed-tables");
        OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                config().onlineDdlDirectory(), config().onlineDdlConfig());
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-index-corrupt-committed-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-index-corrupt-committed-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(118), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            TableDefinition table = base.createTable(
                    MdlOwnerId.of(118), updateCommand(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new DictionaryDdlFaultInjector() {
                @Override public void afterDropPendingPublished(TableDefinition pending) { }
                @Override
                public void afterCreateIndexDictionaryCommitted(TableDefinition active) {
                    throw new DictionaryDdlException(
                            "injected online crash after dictionary commit");
                }
            }, DictionaryCleanSnapshotPublisher.noOp(), new OnlineIndexBuildRuntime(
                    storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                    storage.typeCodecRegistry()));

            assertThrows(DictionaryDdlException.class, () -> crashing.createSecondaryIndex(
                    MdlOwnerId.of(119), secondaryIndexCommand(), Duration.ofSeconds(5)));
            DdlLogRecord unresolved = repository.ddlLog().unresolved().getFirst();
            assertEquals(DdlLogPhase.DICTIONARY_COMMITTED, unresolved.phase());
            assertEquals(2, repository.findTable(table.id()).orElseThrow().indexes().size());
            Files.write(unresolved.auxiliaryPath().orElseThrow(), new byte[]{1, 2, 3});

            OnlineDdlOperationRegistry recoveryRegistry = new OnlineDdlOperationRegistry(8);
            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new OnlineIndexRecoveryRuntime(
                    config().onlineDdlConfig(), logFiles, storage.typeCodecRegistry()),
                    recoveryRegistry)
                    .recover(Duration.ofSeconds(5));

            TableDefinition recovered = repository.findTable(table.id()).orElseThrow();
            assertEquals(ObjectName.of("idx_value"), recovered.indexes().getLast().name());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(recovered.storageBinding().orElseThrow()).isEmpty());
            assertFalse(Files.exists(unresolved.auxiliaryPath().orElseThrow()));
        } finally {
            storage.close();
        }
    }

    /**
     * 模拟进程在 PREPARED marker 返回后立即消失：启动恢复不能依赖原 SQL 或内存 gate，必须从 durable
     * manifest 创建全新 generation、重扫聚簇真相，并把同一预留 index/version 前滚到 COMMITTED。
     */
    @Test
    void rebuildsOnlineCreateIndexFromPreparedManifestDuringRecovery() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("online-index-prepared-tables");
        OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                config().onlineDdlDirectory(), config().onlineDdlConfig());
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-index-prepared-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-index-prepared-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(114), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            TableDefinition table = base.createTable(
                    MdlOwnerId.of(114), updateCommand(), Duration.ofSeconds(5));
            OnlineIndexBuildRuntime buildRuntime = new OnlineIndexBuildRuntime(
                    storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                    storage.typeCodecRegistry());
            DictionaryDdlService crashing = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new DictionaryDdlFaultInjector() {
                @Override
                public void afterDropPendingPublished(TableDefinition pending) {
                }

                @Override
                public void afterCreateIndexCaptureDurable(DdlLogRecord prepared) {
                    throw new SimulatedProcessCrashError();
                }
            }, DictionaryCleanSnapshotPublisher.noOp(), buildRuntime);

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.createSecondaryIndex(
                    MdlOwnerId.of(115), secondaryIndexCommand(), Duration.ofSeconds(5)));
            DdlLogRecord unresolved = repository.ddlLog().unresolved().getFirst();
            assertEquals(DdlLogPhase.PREPARED, unresolved.phase());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(table.storageBinding().orElseThrow()).isPresent());

            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new OnlineIndexRecoveryRuntime(
                    config().onlineDdlConfig(), logFiles, storage.typeCodecRegistry()))
                    .recover(Duration.ofSeconds(5));

            TableDefinition recovered = repository.findTable(table.id()).orElseThrow();
            assertEquals(ObjectName.of("idx_value"), recovered.indexes().getLast().name());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(3)).orElseThrow().phase());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(recovered.storageBinding().orElseThrow()).isEmpty());
            assertFalse(Files.exists(unresolved.auxiliaryPath().orElseThrow()));
        } finally {
            storage.close();
        }
    }

    /** PREPARED row-log 已 durable ABORT_REQUIRED 时，恢复只能 exact rollback descriptor 并保留旧 DD。 */
    @Test
    void rollsBackPreparedOnlineIndexWhenDurableAbortExists() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("online-index-abort-tables");
        OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                config().onlineDdlDirectory(), config().onlineDdlConfig());
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-index-abort-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-index-abort-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(116), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            TableDefinition table = base.createTable(
                    MdlOwnerId.of(116), updateCommand(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new DictionaryDdlFaultInjector() {
                @Override public void afterDropPendingPublished(TableDefinition pending) { }
                @Override
                public void afterCreateIndexCaptureDurable(DdlLogRecord prepared) {
                    throw new SimulatedProcessCrashError();
                }
            }, DictionaryCleanSnapshotPublisher.noOp(), new OnlineIndexBuildRuntime(
                    storage.onlineDdlTableGate(), config().onlineDdlConfig(), logFiles,
                    storage.typeCodecRegistry()));

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.createSecondaryIndex(
                    MdlOwnerId.of(117), secondaryIndexCommand(), Duration.ofSeconds(5)));
            DdlLogRecord unresolved = repository.ddlLog().unresolved().getFirst();
            OnlineIndexBuildId buildId = OnlineIndexBuildId.of(
                    unresolved.marker().ddlOperationId());
            try (var log = logFiles.open(buildId, unresolved.auxiliaryPath().orElseThrow())) {
                log.markAbortRequired(OnlineDdlAbortReason.CANCELLED, Duration.ofSeconds(2));
            }

            OnlineDdlOperationRegistry recoveryRegistry = new OnlineDdlOperationRegistry(8);
            new DictionaryDdlRecoveryService(
                    control, repository, cache, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, new OnlineIndexRecoveryRuntime(
                    config().onlineDdlConfig(), logFiles, storage.typeCodecRegistry()),
                    recoveryRegistry)
                    .recover(Duration.ofSeconds(5));

            assertEquals(1, repository.findTable(table.id()).orElseThrow().indexes().size());
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(buildId.value())).orElseThrow().phase());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(table.storageBinding().orElseThrow()).isEmpty());
            assertFalse(Files.exists(unresolved.auxiliaryPath().orElseThrow()));
            assertEquals(OnlineDdlTerminalResult.ROLLED_BACK,
                    recoveryRegistry.find(cn.zhangyis.db.dd.domain.DdlId.of(buildId.value()))
                            .orElseThrow().terminalResult());
        } finally {
            storage.close();
        }
    }

    /** manifest force 早于 PREPARED marker；该窗口崩溃留下的 exact 日志没有 marker/page3 owner，应在启动时删除。 */
    @Test
    void deletesOwnerlessOnlineIndexManifestDuringRecovery() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("online-index-orphan-tables");
        OnlineIndexChangeLogFiles logFiles = new OnlineIndexChangeLogFiles(
                config().onlineDdlDirectory(), config().onlineDdlConfig());
        OnlineIndexBuildId buildId = OnlineIndexBuildId.of(91);
        Path orphan;
        try (var changeLog = logFiles.create(new OnlineIndexLogHeader(
                buildId, 92, 93, 94, 95, 96, new byte[]{1, 2, 3}))) {
            orphan = changeLog.path();
        }
        assertTrue(Files.exists(orphan));

        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("online-index-orphan-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("online-index-orphan-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            new DictionaryDdlRecoveryService(
                    control, repository, new DictionaryObjectCache(16),
                    storage.tableDdlStorageService(), tables, TablePurgeBarrier.NONE,
                    new OnlineIndexRecoveryRuntime(config().onlineDdlConfig(), logFiles,
                            storage.typeCodecRegistry()))
                    .recover(Duration.ofSeconds(5));

            assertFalse(Files.exists(orphan));
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

    /** DROP 在真实 history 引用上等待；purge finalization 发布引用减少后唤醒同一 Condition 并继续物理删除。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
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
            assertEquals(5, repository.snapshot().publishedVersion().value(),
                    "recovery must publish the target version already reserved and covered by marker digest");
        } finally {
            storage.close();
        }
    }

    /**
     * 多表 DROP 的 pending 集合与 v5 manifest 必须共同前滚：故障后不能恢复成一张已删、一张仍 ACTIVE，
     * 且恢复必须复用 live 已预留的 target version 与同一 marker identity。
     */
    @Test
    void recoversAtomicBatchDropAfterPendingCrash() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("batch-pending-tables");
        try (FileInternalCatalogStore catalog =
                     FileInternalCatalogStore.openOrCreate(
                             directory.resolve(
                                     "batch-pending-mysql.ibd"));
             DictionaryControlStore control =
                     DictionaryControlStore.openOrCreate(
                             directory.resolve(
                                     "batch-pending-mysql.dd.ctrl"),
                             SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository =
                    new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache =
                    new DictionaryObjectCache(16);
            MetadataLockManager locks =
                    new MetadataLockManager(8, 128);
            DictionaryDdlService creator =
                    new DictionaryDdlService(
                            control, repository, cache, locks,
                            storage.tableDdlStorageService(), tables);
            creator.createSchema(
                    MdlOwnerId.of(71), ObjectName.of("app"),
                    1, 1, Duration.ofSeconds(5));
            TableDefinition first = creator.createTable(
                    MdlOwnerId.of(71), command(),
                    Duration.ofSeconds(5));
            TableDefinition second = creator.createTable(
                    MdlOwnerId.of(71),
                    new CreateTableCommand(
                            QualifiedTableName.of(
                                    "app", "second_orders"),
                            PageNo.of(128),
                            List.of(new CreateColumnSpec(
                                    ObjectName.of("id"),
                                    ColumnTypeDefinition.bigint(
                                            false, false))),
                            List.of(new CreateIndexSpec(
                                    ObjectName.of("PRIMARY"),
                                    true, true,
                                    List.of(new CreateIndexKeyPartSpec(
                                            ObjectName.of("id"),
                                            IndexOrder.ASC, 0))))),
                    Duration.ofSeconds(5));
            Path firstFile =
                    first.storageBinding().orElseThrow().path();
            Path secondFile =
                    second.storageBinding().orElseThrow().path();
            DictionaryDdlFaultInjector crashAfterPending =
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(
                                TableDefinition pending) {
                        }

                        @Override
                        public void afterBatchDropPending(
                                List<TableDefinition> pending) {
                            throw new DictionaryDdlException(
                                    "injected crash after batch pending");
                        }
                    };
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks,
                    tables, crashAfterPending);

            assertThrows(DictionaryDdlException.class,
                    () -> crashing.dropTables(
                            MdlOwnerId.of(71),
                            List.of(
                                    QualifiedTableName.of(
                                            "app", "orders"),
                                    QualifiedTableName.of(
                                            "app", "second_orders")),
                            false, Duration.ofSeconds(5)));

            assertEquals(TableState.DROP_PENDING,
                    repository.findTableForRecovery(
                            first.id()).orElseThrow().state());
            assertEquals(TableState.DROP_PENDING,
                    repository.findTableForRecovery(
                            second.id()).orElseThrow().state());
            assertTrue(Files.exists(firstFile));
            assertTrue(Files.exists(secondFile));
            DdlLogRecord marker = repository.ddlLog().records()
                    .stream()
                    .filter(record -> record.operation()
                            == DdlLogOperation.DROP_TABLE_BATCH)
                    .findFirst().orElseThrow();
            assertEquals(DdlLogPhase.DICTIONARY_COMMITTED,
                    marker.phase());
            assertEquals(List.of(
                            first.id(), second.id()),
                    marker.batchManifest().orElseThrow()
                            .tables().stream()
                            .map(DdlBatchTableEntry::tableId)
                            .toList());

            new DictionaryDdlRecoveryService(
                    control, repository, cache,
                    storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            assertFalse(Files.exists(firstFile));
            assertFalse(Files.exists(secondFile));
            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(
                            first.id()).orElseThrow().state());
            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(
                            second.id()).orElseThrow().state());
            assertEquals(DdlLogPhase.COMMITTED,
                    repository.ddlLog().find(
                            cn.zhangyis.db.dd.domain.DdlId.of(
                            marker.marker().ddlOperationId()))
                            .orElseThrow().phase());
        } finally {
            storage.close();
        }
    }

    /**
     * DROP SCHEMA 在全部文件删除后、最终 DD 事务前崩溃时，schema 仍 ACTIVE、表仍全 pending；
     * 恢复必须以 ENGINE_DONE manifest 在一个事务中同时发布 schema/table tombstone。
     */
    @Test
    void recoversSchemaCascadeAfterEngineDoneCrash() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve(
                "schema-cascade-tables");
        try (FileInternalCatalogStore catalog =
                     FileInternalCatalogStore.openOrCreate(
                             directory.resolve(
                                     "schema-cascade-mysql.ibd"));
             DictionaryControlStore control =
                     DictionaryControlStore.openOrCreate(
                             directory.resolve(
                                     "schema-cascade-mysql.dd.ctrl"),
                             SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository =
                    new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache =
                    new DictionaryObjectCache(16);
            MetadataLockManager locks =
                    new MetadataLockManager(8, 128);
            DictionaryDdlService creator =
                    new DictionaryDdlService(
                            control, repository, cache, locks,
                            storage.tableDdlStorageService(), tables);
            SchemaDefinition schema = creator.createSchema(
                    MdlOwnerId.of(72), ObjectName.of("app"),
                    1, 1, Duration.ofSeconds(5));
            TableDefinition table = creator.createTable(
                    MdlOwnerId.of(72), command(),
                    Duration.ofSeconds(5));
            Path file = table.storageBinding()
                    .orElseThrow().path();
            DictionaryDdlFaultInjector crashAtEngineDone =
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(
                                TableDefinition pending) {
                        }

                        @Override
                        public void afterBatchDropEngineDone(
                                DdlLogRecord engineDone) {
                            throw new DictionaryDdlException(
                                    "injected crash after schema cascade engine done");
                        }
                    };
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks,
                    tables, crashAtEngineDone);

            assertThrows(DictionaryDdlException.class,
                    () -> crashing.dropSchema(
                            MdlOwnerId.of(72),
                            ObjectName.of("app"), false,
                            Duration.ofSeconds(5)));

            assertEquals(SchemaState.ACTIVE,
                    repository.snapshot().schemas()
                            .get(schema.id()).state());
            assertEquals(TableState.DROP_PENDING,
                    repository.findTableForRecovery(
                            table.id()).orElseThrow().state());
            assertFalse(Files.exists(file));
            DdlLogRecord marker = repository.ddlLog().records()
                    .stream()
                    .filter(record -> record.operation()
                            == DdlLogOperation.DROP_SCHEMA_CASCADE)
                    .findFirst().orElseThrow();
            assertEquals(DdlLogPhase.ENGINE_DONE,
                    marker.phase());

            new DictionaryDdlRecoveryService(
                    control, repository, cache,
                    storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            assertEquals(SchemaState.DROPPED,
                    repository.snapshot().schemas()
                            .get(schema.id()).state());
            assertEquals(TableState.DROPPED,
                    repository.findTableForRecovery(
                            table.id()).orElseThrow().state());
            assertEquals(DdlLogPhase.COMMITTED,
                    repository.ddlLog().find(
                            cn.zhangyis.db.dd.domain.DdlId.of(
                                    marker.marker()
                                            .ddlOperationId()))
                            .orElseThrow().phase());
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

    /**
     * shadow/SDI/ENGINE_DONE durable 后崩溃但 DD 仍引用旧 binding 时，恢复必须删除 exact shadow，
     * 保留原表文件并把 REBUILD marker 终结为 ROLLED_BACK。
     */
    @Test
    void rollsBackAlterEngineDoneAgainstOldDictionaryBinding() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("alter-engine-done-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("alter-engine-done-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("alter-engine-done-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository =
                    new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(
                    control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(
                    MdlOwnerId.of(73), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(5));
            TableDefinition before = ddl.createTable(
                    MdlOwnerId.of(73), command(), Duration.ofSeconds(5));
            Path oldPath = before.storageBinding().orElseThrow().path();
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterAlterEngineDone(DdlLogRecord engineDone) {
                            throw new DictionaryDdlException(
                                    "injected crash after ALTER ENGINE_DONE");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> crashing.alterTable(
                    MdlOwnerId.of(74), structuralAlterCommand(),
                    Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            Path shadowPath = marker.auxiliaryPath().orElseThrow();
            assertEquals(DdlLogOperation.REBUILD_TABLE, marker.operation());
            assertEquals(DdlLogPhase.ENGINE_DONE, marker.phase());
            assertTrue(Files.exists(oldPath));
            assertTrue(Files.exists(shadowPath));
            assertEquals(before.storageBinding(),
                    repository.findTableForRecovery(before.id())
                            .orElseThrow().storageBinding());

            DictionaryObjectCache recoveryCache = new DictionaryObjectCache(16);
            new DictionaryDdlRecoveryService(
                    control, repository, recoveryCache,
                    storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            assertTrue(Files.exists(oldPath));
            assertFalse(Files.exists(shadowPath));
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(
                            marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
            try (var pin = recoveryCache.pinTable(
                    before.id(), Duration.ofSeconds(1), () -> Optional.of(before))) {
                assertEquals(before, pin.value());
            }
        } finally {
            storage.close();
        }
    }

    /**
     * committed DD 与 DICTIONARY_COMMITTED 已引用 shadow 后崩溃时，恢复必须保留新空间、发布 cache、
     * 补 COMMITTED 并把不再被字典引用的旧空间作为受控 orphan 删除。
     */
    @Test
    void finishesAlterAfterDictionaryBindingSwap() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("alter-dd-committed-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("alter-dd-committed-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("alter-dd-committed-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository =
                    new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(
                    control, repository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(
                    MdlOwnerId.of(75), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(5));
            TableDefinition before = ddl.createTable(
                    MdlOwnerId.of(75), command(), Duration.ofSeconds(5));
            Path oldPath = before.storageBinding().orElseThrow().path();
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterAlterDictionaryCommitted(
                                TableDefinition active) {
                            throw new DictionaryDdlException(
                                    "injected crash after ALTER DD commit");
                        }
                    });

            assertThrows(DictionaryDdlException.class, () -> crashing.alterTable(
                    MdlOwnerId.of(76), structuralAlterCommand(),
                    Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            TableDefinition committed =
                    repository.findTableForRecovery(before.id()).orElseThrow();
            Path shadowPath = committed.storageBinding().orElseThrow().path();
            assertEquals(DdlLogPhase.DICTIONARY_COMMITTED, marker.phase());
            assertTrue(Files.exists(oldPath));
            assertTrue(Files.exists(shadowPath));

            new DictionaryDdlRecoveryService(
                    control, repository, cache,
                    storage.tableDdlStorageService(), tables)
                    .recover(Duration.ofSeconds(5));

            assertFalse(Files.exists(oldPath));
            assertTrue(Files.exists(shadowPath));
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog()
                    .find(cn.zhangyis.db.dd.domain.DdlId.of(
                            marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
            try (var pin = cache.pinTable(
                    committed.id(), Duration.ofSeconds(1),
                    () -> Optional.of(committed))) {
                assertEquals(committed, pin.value());
            }
        } finally {
            storage.close();
        }
    }

    /** target SDI已写但control仍OPEN时崩溃，恢复必须让committed source DD获胜并覆盖SDI。 */
    @Test
    void rollsBackInstantMetadataAlterFromOpenTargetSdi() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("instant-open-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("instant-open-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("instant-open-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(77), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(5));
            TableDefinition source = ddl.createTable(
                    MdlOwnerId.of(77), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterInplaceAlterTargetSdi(DdlLogRecord prepared) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(78), metadataAlterCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            assertEquals(DdlLogOperation.ALTER_TABLE_INPLACE, marker.operation());
            assertEquals(DdlControlState.OPEN, marker.controlState());
            assertEquals(source, repository.findTableForRecovery(source.id()).orElseThrow());

            new DictionaryDdlRecoveryService(control, repository, new DictionaryObjectCache(16),
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));

            TableDefinition recovered = repository.findTableForRecovery(source.id()).orElseThrow();
            assertEquals(source, recovered);
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
            assertEquals(source, new cn.zhangyis.db.dd.sdi.SerializedDictionaryInfoService(
                    storage.tableDdlStorageService()).read(
                    source.storageBinding().orElseThrow()).orElseThrow());
        } finally {
            storage.close();
        }
    }

    /** target SDI窗口收到durable取消时，live coordinator必须恢复source SDI并保留取消证据到ROLLED_BACK。 */
    @Test
    void cancelsInstantMetadataAlterBeforeForwardFence() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("instant-cancel-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("instant-cancel-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("instant-cancel-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService base = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            base.createSchema(MdlOwnerId.of(83), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(5));
            TableDefinition source = base.createTable(
                    MdlOwnerId.of(83), command(), Duration.ofSeconds(5));
            OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(8);
            OnlineDdlControlService onlineControl =
                    new OnlineDdlControlService(repository.ddlLog(), registry);
            DictionaryDdlFaultInjector cancelAtTargetSdi = new DictionaryDdlFaultInjector() {
                @Override
                public void afterDropPendingPublished(TableDefinition pending) {
                }

                @Override
                public void afterInplaceAlterTargetSdi(DdlLogRecord prepared) {
                    OnlineDdlCancelResult result = onlineControl.requestCancel(
                            cn.zhangyis.db.dd.domain.DdlId.of(
                                    prepared.marker().ddlOperationId()),
                            OnlineDdlCancelRequest.admin(
                                    DdlCancellationReason.USER_REQUEST, 83L),
                            Duration.ofSeconds(1));
                    assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE, result.outcome());
                }
            };
            DictionaryDdlService cancellable = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                    TablePurgeBarrier.NONE, cancelAtTargetSdi,
                    DictionaryCleanSnapshotPublisher.noOp(), null, registry);

            assertThrows(OnlineDdlCancellationException.class, () -> cancellable.alterTable(
                    MdlOwnerId.of(84), metadataAlterCommand(), Duration.ofSeconds(5)));

            TableDefinition recoveredSource =
                    repository.findTableForRecovery(source.id()).orElseThrow();
            assertEquals(source, recoveredSource);
            DdlLogRecord terminal = repository.ddlLog().records().stream()
                    .filter(record -> record.operation() == DdlLogOperation.ALTER_TABLE_INPLACE)
                    .findFirst().orElseThrow();
            assertEquals(DdlLogPhase.ROLLED_BACK, terminal.phase());
            assertEquals(DdlControlState.CANCEL_REQUESTED, terminal.controlState());
            assertEquals(DdlCancellationReason.USER_REQUEST,
                    terminal.cancellation().orElseThrow().reasonCode());
            assertEquals(OnlineDdlTerminalResult.ROLLED_BACK,
                    registry.find(cn.zhangyis.db.dd.domain.DdlId.of(
                            terminal.marker().ddlOperationId())).orElseThrow().terminalResult());
        } finally {
            storage.close();
        }
    }

    /** FORWARD_ONLY已durable而DD仍是source时，恢复必须从exact target SDI发布目标comment并补terminal。 */
    @Test
    void forwardsInstantMetadataAlterFromTargetSdi() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("instant-forward-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("instant-forward-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("instant-forward-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(79), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(5));
            TableDefinition source = ddl.createTable(
                    MdlOwnerId.of(79), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterInplaceAlterForwardFenced(DdlLogRecord forwardFenced) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(80), metadataAlterCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            assertEquals(DdlControlState.FORWARD_ONLY, marker.controlState());
            assertEquals(source, repository.findTableForRecovery(source.id()).orElseThrow());

            DictionaryObjectCache recoveryCache = new DictionaryObjectCache(16);
            new DictionaryDdlRecoveryService(control, repository, recoveryCache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));

            TableDefinition recovered = repository.findTableForRecovery(source.id()).orElseThrow();
            assertEquals("instant", recovered.options().comment());
            assertEquals(marker.marker().dictionaryVersion(), recovered.version().value());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
            try (var pin = recoveryCache.pinTable(recovered.id(), Duration.ofSeconds(1),
                    () -> Optional.of(recovered))) {
                assertEquals(recovered, pin.value());
            }
        } finally {
            storage.close();
        }
    }

    /** target DD与DICTIONARY_COMMITTED完成后崩溃，恢复只补cache/COMMITTED，不得回写source。 */
    @Test
    void finishesInstantMetadataAlterAfterDictionaryCommit() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("instant-committed-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("instant-committed-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("instant-committed-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService ddl = new DictionaryDdlService(
                    control, repository, cache, locks, storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(81), ObjectName.of("app"),
                    1, 2, Duration.ofSeconds(5));
            TableDefinition source = ddl.createTable(
                    MdlOwnerId.of(81), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = ddlWithFault(
                    storage, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterInplaceAlterDictionaryCommitted(TableDefinition active) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(82), metadataAlterCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            TableDefinition committed = repository.findTableForRecovery(source.id()).orElseThrow();
            assertEquals("instant", committed.options().comment());
            assertEquals(DdlLogPhase.DICTIONARY_COMMITTED, marker.phase());

            new DictionaryDdlRecoveryService(control, repository, cache,
                    storage.tableDdlStorageService(), tables).recover(Duration.ofSeconds(5));

            assertEquals("instant", repository.findTableForRecovery(source.id())
                    .orElseThrow().options().comment());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
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
                    0L, DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED,
                    SpaceId.of(1024), mismatched, Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.empty(), Optional.empty(), Optional.of(new DdlSchemaDigest(
                    DdlDigestAlgorithm.SHA_256, DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1,
                    new byte[32])), DdlControlState.OPEN, Optional.empty(), Optional.empty()));

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

    /** recovery完成DD状态分类后必须先校验source digest；不匹配时不能回收descriptor或把marker伪装成回滚完成。 */
    @Test
    void preservesCreateIndexEvidenceWhenSourceSchemaDigestMismatches() {
        StorageEngine storage = new StorageEngine(config());
        storage.open();
        Path tables = directory.resolve("digest-mismatch-tables");
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("digest-mismatch-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("digest-mismatch-mysql.dd.ctrl"), SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            DictionaryDdlService ddl = new DictionaryDdlService(
                    control, repository, cache, new MetadataLockManager(8, 128),
                    storage.tableDdlStorageService(), tables);
            ddl.createSchema(MdlOwnerId.of(801), ObjectName.of("app"), 1, 1,
                    Duration.ofSeconds(5));
            TableDefinition table = ddl.createTable(
                    MdlOwnerId.of(802), updateCommand(), Duration.ofSeconds(5));
            var binding = table.storageBinding().orElseThrow();
            DdlSchemaDigest wrong = new DdlSchemaDigest(
                    DdlDigestAlgorithm.SHA_256, DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1,
                    new byte[32]);
            repository.ddlLog().prepare(new DdlLogRecord(
                    new cn.zhangyis.db.storage.api.ddl.DdlUndoMarker(
                            99, table.version().value() + 1, table.id().value()),
                    999, DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                    binding.spaceId(), binding.path(), Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(wrong), Optional.empty(), Optional.of(wrong),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty()));

            assertThrows(DictionaryRecoveryException.class,
                    () -> new DictionaryDdlRecoveryService(
                            control, repository, cache, storage.tableDdlStorageService(), tables)
                            .recover(Duration.ofSeconds(5)));
            assertEquals(DdlLogPhase.PREPARED,
                    repository.ddlLog().find(cn.zhangyis.db.dd.domain.DdlId.of(99))
                            .orElseThrow().phase());
            assertTrue(storage.tableDdlStorageService()
                    .readSecondaryIndexBuild(binding).isEmpty());
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

    /**
     * ALTER shadow 已完成后，catalog append 可能已经 durable 但响应丢失。此时当前进程必须阻断旧 cache，
     * 不能继续把 DML 送往旧 binding；重新读取 committed catalog 则应看到唯一的新 aggregate。
     */
    @Test
    void blocksOldAlterBindingWhenCatalogCommitOutcomeIsUncertain() {
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
            ddl.createSchema(MdlOwnerId.of(40), ObjectName.of("app"), 1, 1, Duration.ofSeconds(5));
            TableDefinition before = ddl.createTable(
                    MdlOwnerId.of(40), command(), Duration.ofSeconds(5));

            PersistentDictionaryRepository uncertainRepository =
                    new PersistentDictionaryRepository(durableThenThrow(catalog));
            DictionaryDdlService uncertainDdl = new DictionaryDdlService(
                    control, uncertainRepository, cache, locks,
                    storage.tableDdlStorageService(), tables);
            DataDictionaryService liveDictionary =
                    new DataDictionaryService(uncertainRepository, cache, locks);

            // 1、故障发生在新 aggregate 已写入 catalog、repository 尚未发布内存 snapshot 的窗口。
            assertThrows(InternalCatalogPersistenceException.class,
                    () -> uncertainDdl.alterTable(
                            MdlOwnerId.of(40), structuralAlterCommand(), Duration.ofSeconds(5)));

            // 2、旧 cache 必须被 publication barrier 拒绝，不能继续访问仍存在但已不再是持久真相的旧空间。
            assertThrows(DictionaryObjectNotFoundException.class,
                    () -> liveDictionary.openTable(
                            MdlOwnerId.of(41), QualifiedTableName.of("app", "orders"),
                            TableAccessIntent.READ, Duration.ofSeconds(2)));

            // 3、从 committed batches 重建证明新 binding/列已经 durable，后续启动可据此完成 marker 前滚。
            PersistentDictionaryRepository durableView = new PersistentDictionaryRepository(catalog);
            TableDefinition durable = durableView.findTableForRecovery(before.id()).orElseThrow();
            assertEquals(2, durable.columns().size());
            assertFalse(durable.storageBinding().equals(before.storageBinding()));
        } finally {
            storage.close();
        }
    }

    /**
     * Shadow 已写 target SDI/READY、但 OPEN 尚未变为 FORWARD_ONLY 时模拟进程消失；新 storage 只打开
     * committed source，DDL recovery 必须离线删除 exact shadow、保留 source 并删除 terminal journal。
     */
    @Test
    void rollsBackGeneralShadowAlterFromReadyOpenAfterRestart() {
        Path tables = directory.resolve("general-shadow-open-tables");
        EngineConfig base = config();
        StorageEngine live = new StorageEngine(base);
        live.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("general-shadow-open-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("general-shadow-open-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService bootstrap = new DictionaryDdlService(
                    control, repository, cache, locks, live.tableDdlStorageService(), tables);
            bootstrap.createSchema(MdlOwnerId.of(710), ObjectName.of("app"),
                    1, 1, Duration.ofSeconds(5));
            TableDefinition source = bootstrap.createTable(
                    MdlOwnerId.of(710), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = generalDdlWithFault(
                    live, base, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterGeneralAlterReady(DdlLogRecord prepared) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(711), structuralAlterCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            Path shadow = readGeneralManifest(base, marker)
                    .shadowTarget().orElseThrow().path();
            assertEquals(DdlControlState.OPEN, marker.controlState());
            assertTrue(Files.exists(shadow));
            live.close();

            StorageEngine recovered = new StorageEngine(base.withRecoveryTablespaces(List.of(
                    new EngineTablespaceConfig(
                            source.storageBinding().orElseThrow().spaceId(),
                            source.storageBinding().orElseThrow().path()))));
            recovered.open();
            try {
                recoverGeneralAlter(recovered, base, control, repository,
                        new DictionaryObjectCache(16), tables);
            } finally {
                recovered.close();
            }

            assertEquals(source, repository.findTableForRecovery(source.id()).orElseThrow());
            assertFalse(Files.exists(shadow));
            assertFalse(Files.exists(marker.auxiliaryPath().orElseThrow()));
            assertEquals(DdlLogPhase.ROLLED_BACK, repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
        } finally {
            live.close();
        }
    }

    /**
     * FORWARD_ONLY、RECONCILED 与 ENGINE_DONE 已持久而 DD 仍是 source 时，重启必须挂载 shadow SDI、
     * 发布唯一 target aggregate、等待旧版本退休并删除旧空间。
     */
    @Test
    void forwardsGeneralShadowAlterFromEngineDoneSourceAfterRestart() {
        Path tables = directory.resolve("general-shadow-forward-tables");
        EngineConfig base = config();
        StorageEngine live = new StorageEngine(base);
        live.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("general-shadow-forward-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("general-shadow-forward-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService bootstrap = new DictionaryDdlService(
                    control, repository, cache, locks, live.tableDdlStorageService(), tables);
            bootstrap.createSchema(MdlOwnerId.of(720), ObjectName.of("app"),
                    1, 1, Duration.ofSeconds(5));
            TableDefinition source = bootstrap.createTable(
                    MdlOwnerId.of(720), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = generalDdlWithFault(
                    live, base, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterGeneralAlterEngineDone(DdlLogRecord engineDone) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(721), structuralAlterCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            Path oldPath = source.storageBinding().orElseThrow().path();
            assertEquals(DdlLogPhase.ENGINE_DONE, marker.phase());
            assertEquals(DdlControlState.FORWARD_ONLY, marker.controlState());
            live.close();

            StorageEngine recovered = new StorageEngine(base.withRecoveryTablespaces(List.of(
                    new EngineTablespaceConfig(
                            source.storageBinding().orElseThrow().spaceId(), oldPath))));
            recovered.open();
            try {
                recoverGeneralAlter(recovered, base, control, repository,
                        new DictionaryObjectCache(16), tables);
            } finally {
                recovered.close();
            }

            TableDefinition target = repository.findTableForRecovery(source.id()).orElseThrow();
            assertEquals(2, target.columns().size());
            assertEquals(marker.marker().dictionaryVersion(), target.version().value());
            assertFalse(target.storageBinding().equals(source.storageBinding()));
            assertFalse(Files.exists(oldPath));
            assertTrue(Files.exists(target.storageBinding().orElseThrow().path()));
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
        } finally {
            live.close();
        }
    }

    /**
     * target DD 与 DICTIONARY_COMMITTED 已落盘后，重启 discovery 只打开 shadow；恢复必须依据持久 fence
     * 离线删除未打开的旧 source，而不能反向恢复旧 aggregate。
     */
    @Test
    void retiresGeneralShadowSourceFromTargetDictionaryAfterRestart() {
        Path tables = directory.resolve("general-shadow-target-tables");
        EngineConfig base = config();
        StorageEngine live = new StorageEngine(base);
        live.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("general-shadow-target-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("general-shadow-target-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService bootstrap = new DictionaryDdlService(
                    control, repository, cache, locks, live.tableDdlStorageService(), tables);
            bootstrap.createSchema(MdlOwnerId.of(730), ObjectName.of("app"),
                    1, 1, Duration.ofSeconds(5));
            TableDefinition source = bootstrap.createTable(
                    MdlOwnerId.of(730), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = generalDdlWithFault(
                    live, base, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterGeneralAlterDictionaryCommitted(TableDefinition active) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(731), structuralAlterCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            TableDefinition target = repository.findTableForRecovery(source.id()).orElseThrow();
            Path oldPath = source.storageBinding().orElseThrow().path();
            assertEquals(DdlLogPhase.DICTIONARY_COMMITTED, marker.phase());
            live.close();

            StorageEngine recovered = new StorageEngine(base.withRecoveryTablespaces(List.of(
                    new EngineTablespaceConfig(
                            target.storageBinding().orElseThrow().spaceId(),
                            target.storageBinding().orElseThrow().path()))));
            recovered.open();
            try {
                recoverGeneralAlter(recovered, base, control, repository,
                        new DictionaryObjectCache(16), tables);
            } finally {
                recovered.close();
            }

            assertEquals(target, repository.findTableForRecovery(source.id()).orElseThrow());
            assertFalse(Files.exists(oldPath));
            assertTrue(Files.exists(target.storageBinding().orElseThrow().path()));
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
        } finally {
            live.close();
        }
    }

    /**
     * 多 ADD descriptor 与 RECONCILED 已持久、DD 仍是 source 时，恢复应从同空间 target SDI 一次发布
     * 两个新索引，并清除 descriptor chain 而保留新 root/segment。
     */
    @Test
    void forwardsGeneralInplaceIndexesFromEngineDoneSourceAfterRestart() {
        Path tables = directory.resolve("general-inplace-forward-tables");
        EngineConfig base = config();
        StorageEngine live = new StorageEngine(base);
        live.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("general-inplace-forward-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("general-inplace-forward-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService bootstrap = new DictionaryDdlService(
                    control, repository, cache, locks, live.tableDdlStorageService(), tables);
            bootstrap.createSchema(MdlOwnerId.of(740), ObjectName.of("app"),
                    1, 1, Duration.ofSeconds(5));
            TableDefinition source = bootstrap.createTable(
                    MdlOwnerId.of(740), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = generalDdlWithFault(
                    live, base, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterGeneralAlterEngineDone(DdlLogRecord engineDone) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(741), generalAddIndexesCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            live.close();

            StorageEngine recovered = new StorageEngine(base.withRecoveryTablespaces(List.of(
                    new EngineTablespaceConfig(
                            source.storageBinding().orElseThrow().spaceId(),
                            source.storageBinding().orElseThrow().path()))));
            recovered.open();
            try {
                recoverGeneralAlter(recovered, base, control, repository,
                        new DictionaryObjectCache(16), tables);
            } finally {
                recovered.close();
            }

            TableDefinition target = repository.findTableForRecovery(source.id()).orElseThrow();
            assertEquals(List.of("PRIMARY", "idx_id_a", "idx_id_b"),
                    target.indexes().stream().map(index -> index.name().displayName()).toList());
            assertEquals(source.storageBinding().orElseThrow().spaceId(),
                    target.storageBinding().orElseThrow().spaceId());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
            assertFalse(Files.exists(marker.auxiliaryPath().orElseThrow()));
        } finally {
            live.close();
        }
    }

    /**
     * mixed DROP+ADD 的 target DD 已提交时，恢复必须按持久 fence 退休旧索引并保留新增 binding；
     * descriptor cleanup 不得误删已发布的 ADD root。
     */
    @Test
    void retiresGeneralInplaceDropFromTargetDictionaryAfterRestart() {
        Path tables = directory.resolve("general-inplace-target-tables");
        EngineConfig base = config();
        StorageEngine live = new StorageEngine(base);
        live.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("general-inplace-target-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("general-inplace-target-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService bootstrap = new DictionaryDdlService(
                    control, repository, cache, locks, live.tableDdlStorageService(), tables);
            bootstrap.createSchema(MdlOwnerId.of(750), ObjectName.of("app"),
                    1, 1, Duration.ofSeconds(5));
            bootstrap.createTable(MdlOwnerId.of(750), updateCommand(), Duration.ofSeconds(5));
            TableDefinition source = bootstrap.createSecondaryIndex(
                    MdlOwnerId.of(750), secondaryIndexCommand(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = generalDdlWithFault(
                    live, base, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterGeneralAlterDictionaryCommitted(TableDefinition active) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(751), generalMixedIndexCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            TableDefinition target = repository.findTableForRecovery(source.id()).orElseThrow();
            assertEquals(List.of("PRIMARY", "idx_id"),
                    target.indexes().stream().map(index -> index.name().displayName()).toList());
            live.close();

            StorageEngine recovered = new StorageEngine(base.withRecoveryTablespaces(List.of(
                    new EngineTablespaceConfig(
                            target.storageBinding().orElseThrow().spaceId(),
                            target.storageBinding().orElseThrow().path()))));
            recovered.open();
            try {
                recoverGeneralAlter(recovered, base, control, repository,
                        new DictionaryObjectCache(16), tables);
            } finally {
                recovered.close();
            }

            assertEquals(target, repository.findTableForRecovery(source.id()).orElseThrow());
            assertEquals(DdlLogPhase.COMMITTED, repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow().phase());
            assertFalse(Files.exists(marker.auxiliaryPath().orElseThrow()));
        } finally {
            live.close();
        }
    }

    /**
     * FORWARD_ONLY 先于 RECONCILED 是故意保留的 crash window；恢复不得把“方向已封闭”误当作“物理结果完整”，
     * 必须 fail-closed 并原样保留 journal、descriptor 与 source DD 供诊断或后续修复。
     */
    @Test
    void failsClosedWhenGeneralAlterForwardFenceLacksReconciledForce() {
        Path tables = directory.resolve("general-forward-gap-tables");
        EngineConfig base = config();
        StorageEngine live = new StorageEngine(base);
        live.open();
        try (FileInternalCatalogStore catalog = FileInternalCatalogStore.openOrCreate(
                directory.resolve("general-forward-gap-mysql.ibd"));
             DictionaryControlStore control = DictionaryControlStore.openOrCreate(
                     directory.resolve("general-forward-gap-mysql.dd.ctrl"),
                     SpaceId.of(1), 1024)) {
            PersistentDictionaryRepository repository = new PersistentDictionaryRepository(catalog);
            DictionaryObjectCache cache = new DictionaryObjectCache(16);
            MetadataLockManager locks = new MetadataLockManager(8, 128);
            DictionaryDdlService bootstrap = new DictionaryDdlService(
                    control, repository, cache, locks, live.tableDdlStorageService(), tables);
            bootstrap.createSchema(MdlOwnerId.of(760), ObjectName.of("app"),
                    1, 1, Duration.ofSeconds(5));
            TableDefinition source = bootstrap.createTable(
                    MdlOwnerId.of(760), command(), Duration.ofSeconds(5));
            DictionaryDdlService crashing = generalDdlWithFault(
                    live, base, control, repository, cache, locks, tables,
                    new DictionaryDdlFaultInjector() {
                        @Override
                        public void afterDropPendingPublished(TableDefinition pending) {
                        }

                        @Override
                        public void afterGeneralAlterForwardFenced(DdlLogRecord forwardFenced) {
                            throw new SimulatedProcessCrashError();
                        }
                    });

            assertThrows(SimulatedProcessCrashError.class, () -> crashing.alterTable(
                    MdlOwnerId.of(761), generalAddIndexesCommand(), Duration.ofSeconds(5)));
            DdlLogRecord marker = repository.ddlLog().unresolved().getFirst();
            live.close();

            StorageEngine recovered = new StorageEngine(base.withRecoveryTablespaces(List.of(
                    new EngineTablespaceConfig(
                            source.storageBinding().orElseThrow().spaceId(),
                            source.storageBinding().orElseThrow().path()))));
            recovered.open();
            try {
                assertThrows(DictionaryRecoveryException.class, () -> recoverGeneralAlter(
                        recovered, base, control, repository,
                        new DictionaryObjectCache(16), tables));
                assertTrue(recovered.tableDdlStorageService()
                        .readOnlineAlterDescriptorSet(
                                source.storageBinding().orElseThrow()).isPresent());
            } finally {
                recovered.close();
            }

            DdlLogRecord retained = repository.ddlLog().find(
                    cn.zhangyis.db.dd.domain.DdlId.of(marker.marker().ddlOperationId()))
                    .orElseThrow();
            assertEquals(DdlLogPhase.PREPARED, retained.phase());
            assertEquals(DdlControlState.FORWARD_ONLY, retained.controlState());
            assertTrue(Files.exists(marker.auxiliaryPath().orElseThrow()));
            assertEquals(source, repository.findTableForRecovery(source.id()).orElseThrow());
        } finally {
            live.close();
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

    /** 构造启用完整通用ALTER能力且共享真实storage gate/history的故障注入coordinator。 */
    private static DictionaryDdlService generalDdlWithFault(
            StorageEngine storage, EngineConfig config,
            DictionaryControlStore control,
            PersistentDictionaryRepository repository,
            DictionaryObjectCache cache, MetadataLockManager locks,
            Path tables, DictionaryDdlFaultInjector faultInjector) {
        OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(32);
        return new DictionaryDdlService(
                control, repository, cache, locks, storage.tableDdlStorageService(), tables,
                storage.tablePurgeBarrier(), faultInjector,
                DictionaryCleanSnapshotPublisher.noOp(), null, registry, null,
                new OnlineAlterRuntime(
                        storage.onlineDdlTableGate(), config.onlineDdlConfig(),
                        new OnlineAlterChangeLogFiles(
                                config.onlineDdlDirectory(), config.onlineDdlConfig()),
                        storage.typeCodecRegistry(), storage.readViewRetentionBarrier()),
                new DefaultOnlineAlterRetirementBarrier(
                        storage.indexRetirementHistoryBarrier(),
                        storage.tablePurgeBarrier(), cache));
    }

    /** 在已完成storage恢复的测试实例内运行完整通用ALTER DDL recovery。 */
    private static void recoverGeneralAlter(
            StorageEngine storage, EngineConfig config,
            DictionaryControlStore control,
            PersistentDictionaryRepository repository,
            DictionaryObjectCache cache, Path tables) {
        new DictionaryDdlRecoveryService(
                control, repository, cache, storage.tableDdlStorageService(), tables,
                storage.tablePurgeBarrier(), null,
                new OnlineDdlOperationRegistry(32), null,
                new OnlineAlterRecoveryRuntime(new OnlineAlterChangeLogFiles(
                        config.onlineDdlDirectory(), config.onlineDdlConfig())),
                new DefaultOnlineAlterRetirementBarrier(
                        storage.indexRetirementHistoryBarrier(),
                        storage.tablePurgeBarrier(), cache))
                .recover(Duration.ofSeconds(5));
    }

    /** 只读打开operation-owned journal并在返回manifest前关闭FileChannel。 */
    private static OnlineAlterManifest readGeneralManifest(
            EngineConfig config, DdlLogRecord marker) {
        OnlineDdlCaptureId captureId = OnlineDdlCaptureId.of(
                marker.marker().ddlOperationId());
        try (var journal = new OnlineAlterChangeLogFiles(
                config.onlineDdlDirectory(), config.onlineDdlConfig()).open(
                captureId, marker.auxiliaryPath().orElseThrow())) {
            return new OnlineAlterManifestCodec().decode(journal.header().manifest());
        }
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

    /** 构造会强制建立 shadow 的最小 ADD COLUMN 命令。 */
    private static AlterTableCommand structuralAlterCommand() {
        return new AlterTableCommand(
                QualifiedTableName.of("app", "orders"),
                List.of(new AlterTableAction.AddColumn(
                        ObjectName.of("status"),
                        ColumnTypeDefinition.integer(false, false),
                        ColumnDefaultDefinition.constant("9"),
                        Optional.of(new StorageDefaultValue.IntegerValue(
                                BigInteger.valueOf(9))),
                        new AlterTableAction.Position(
                                AlterTableAction.PositionKind.LAST,
                                Optional.empty()))));
    }

    /** 两个ADD强制进入通用INPLACE_INDEX，不走单索引兼容委托。 */
    private static AlterTableCommand generalAddIndexesCommand() {
        return new AlterTableCommand(
                QualifiedTableName.of("app", "orders"),
                List.of(
                        new AlterTableAction.AddIndex(new CreateIndexSpec(
                                ObjectName.of("idx_id_a"), false, false,
                                List.of(new CreateIndexKeyPartSpec(
                                        ObjectName.of("id"), IndexOrder.ASC, 0)))),
                        new AlterTableAction.AddIndex(new CreateIndexSpec(
                                ObjectName.of("idx_id_b"), false, false,
                                List.of(new CreateIndexKeyPartSpec(
                                        ObjectName.of("id"), IndexOrder.DESC, 0))))));
    }

    /** 一个DROP与一个ADD共享manifest/generation并同时覆盖retirement资源验证。 */
    private static AlterTableCommand generalMixedIndexCommand() {
        return new AlterTableCommand(
                QualifiedTableName.of("app", "orders"),
                List.of(
                        new AlterTableAction.DropIndex(ObjectName.of("idx_value")),
                        new AlterTableAction.AddIndex(new CreateIndexSpec(
                                ObjectName.of("idx_id"), false, false,
                                List.of(new CreateIndexKeyPartSpec(
                                        ObjectName.of("id"), IndexOrder.ASC, 0))))));
    }

    /** 构造不改变row layout、可由target SDI完整恢复的metadata-only命令。 */
    private static AlterTableCommand metadataAlterCommand() {
        return new AlterTableCommand(QualifiedTableName.of("app", "orders"),
                List.of(new AlterTableAction.Comment("instant")));
    }

    private static DropSecondaryIndexCommand dropIndexCommand() {
        return new DropSecondaryIndexCommand(
                QualifiedTableName.of("app", "orders"), ObjectName.of("idx_value"));
    }

    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    /** 仅用于让测试越过生产 RuntimeException 补偿分支，模拟 JVM 在 durable 边界直接消失。 */
    private static final class SimulatedProcessCrashError extends Error {
    }
}
