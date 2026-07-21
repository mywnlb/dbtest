package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.BTreeIndexMetadataFactory;
import cn.zhangyis.db.storage.api.ddl.StorageColumnDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageColumnType;
import cn.zhangyis.db.storage.api.ddl.StorageColumnTypeId;
import cn.zhangyis.db.storage.api.ddl.StorageIndexDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageIndexKeyPart;
import cn.zhangyis.db.storage.api.ddl.StorageIndexOrder;
import cn.zhangyis.db.storage.api.ddl.StorageTableDefinition;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.api.dml.DmlCommitCommand;
import cn.zhangyis.db.storage.api.dml.TableInsertCommand;
import cn.zhangyis.db.storage.api.dml.TableDeleteCommand;
import cn.zhangyis.db.storage.api.dml.TableUpdateCommand;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.EngineTablespaceConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.recovery.RecoveryStageName;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.redo.DurabilityPolicy;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 表级 UPDATE/DELETE history 驱动 version-safe secondary purge 的生产组合测试。 */
class SecondaryPurgeCoordinatorTest {

    private static final long TABLE_ID = 71;
    private static final long PRIMARY_ID = 701;
    private static final long EMAIL_ID = 702;
    private static final SpaceId DATA_SPACE = SpaceId.of(2071);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @TempDir
    Path directory;

    /** recovery rollback 必须完整解码 undo 并仅推进系统 logical head；同一隔离目标的 live rollback 必须提前拒绝。 */
    @Test
    void recoveryRollbackSkipsUnavailableTargetButLiveRollbackRejectsIt() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "unavailable-rollback.ibd");
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            Transaction transaction = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    transaction, table, row(1, "isolated@example.test"),
                    Optional.empty(), TIMEOUT));
            UndoLogBinding binding = UndoTestContexts.newestBinding(
                    transaction.undoContext());
            resolver.install(new UndoTargetMetadata(
                    table, Optional.empty(), UndoTargetDisposition.RECOVERY_UNAVAILABLE));

            assertThrows(UndoTargetUnavailableException.class,
                    () -> engine.rollbackService().rollback(transaction));
            assertEquals(TransactionState.ACTIVE, transaction.state(),
                    "live preflight failure must precede transaction state transition");

            RollbackSummary recovered = engine.rollbackService().rollbackRecovered(
                    List.of(new RecoveredUndoLogIdentity(
                            binding.kind(), binding.slotId(), binding.firstPageId())),
                    transaction.transactionId());

            assertEquals(0, recovered.undoRecordsApplied());
            assertEquals(1, recovered.recoveryUnavailableRecordsSkipped());
            assertTrue(clustered(engine, table, 1).isPresent(),
                    "recovery isolation must not touch the unavailable user tablespace");
            engine.lockManager().releaseAll(transaction.transactionId());
        } finally {
            engine.close();
        }
    }

    /** committed history 指向隔离对象时，purge 只推进已完整校验的 undo head，不生成 B+Tree/LOB 物理工作。 */
    @Test
    void purgeAdvancesUnavailableHistoryWithoutTouchingUserIndexes() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "unavailable-purge.ibd");
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            LogicalRecord before = row(1, "before-isolation@example.test");
            LogicalRecord after = row(1, "after-isolation@example.test");
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    insert, table, before, Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction update = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(
                    update, table, primaryKey(1), after, TIMEOUT));
            commit(engine, update);
            resolver.install(new UndoTargetMetadata(
                    table, Optional.empty(), UndoTargetDisposition.RECOVERY_UNAVAILABLE));

            PurgeSummary summary = engine.purgeCoordinator().runBatch(1);

            assertEquals(1, summary.purgedLogs());
            assertEquals(1, summary.recoveryUnavailableRecordsSkipped());
            assertEquals(0, summary.removedSecondaryEntries());
            assertEquals(0, summary.removedClusteredRecords());
            assertEquals(0, engine.tablePurgeBarrier().referenceCount(TABLE_ID));
            assertTrue(secondaryIncludingDeleted(
                    engine, table.requireSecondary(EMAIL_ID), before).isPresent(),
                    "isolation purge must leave obsolete user-index bytes untouched");
        } finally {
            engine.close();
        }
    }

    /** UPDATE A→B 提交后，purge 应证明当前/较新版本不再需要 A，再物理删除 marked A entry并保留当前 B 行。 */
    @Test
    @DisplayName("purge removes obsolete secondary key from committed update")
    void removesObsoleteSecondaryKeyFromCommittedUpdate() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "secondary-purge-update.ibd");
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord before = row(1, "before@example.test");
            LogicalRecord after = row(1, "after@example.test");
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(insert, table, before,
                    Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction update = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(update, table, primaryKey(1), after, TIMEOUT));
            commit(engine, update);
            assertEquals(1, engine.tablePurgeBarrier().referenceCount(TABLE_ID),
                    "committed UPDATE publishes one table-level history reference");

            PurgeSummary summary = engine.purgeCoordinator().runBatch(1);

            assertEquals(1, summary.purgedLogs());
            assertEquals(1, summary.removedSecondaryEntries());
            assertEquals(0, summary.removedClusteredRecords());
            assertTrue(secondaryIncludingDeleted(engine, email, before).isEmpty());
            assertTrue(secondaryIncludingDeleted(engine, email, after).isPresent());
            assertEquals(after.columnValues(), clustered(engine, table, 1).orElseThrow().record().columnValues());
            assertEquals(0, engine.tablePurgeBarrier().referenceCount(TABLE_ID),
                    "purge finalization releases the table-level history reference");
        } finally {
            engine.close();
        }
    }

    /** A→B→A 时第一条 update history 的旧 A identity 被当前 live A 复用，必须 RETAIN；第二条随后只回收旧 B。 */
    @Test
    @DisplayName("purge retains secondary identity required by a newer version")
    void retainsIdentityRequiredByNewerVersion() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "secondary-purge-aba.ibd");
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord versionA = row(1, "a@example.test");
            LogicalRecord versionB = row(1, "b@example.test");
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(insert, table, versionA,
                    Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction toB = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(toB, table, primaryKey(1), versionB, TIMEOUT));
            commit(engine, toB);
            Transaction backToA = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(
                    backToA, table, primaryKey(1), versionA, TIMEOUT));
            commit(engine, backToA);

            PurgeSummary first = engine.purgeCoordinator().runBatch(1);
            assertEquals(1, first.purgedLogs());
            assertEquals(0, first.removedSecondaryEntries());
            assertTrue(secondaryIncludingDeleted(engine, email, versionA).isPresent());
            assertTrue(!secondaryIncludingDeleted(engine, email, versionA).orElseThrow().record().deleted());

            PurgeSummary second = engine.purgeCoordinator().runBatch(1);
            assertEquals(1, second.purgedLogs());
            assertEquals(1, second.removedSecondaryEntries());
            assertTrue(secondaryIncludingDeleted(engine, email, versionB).isEmpty());
            assertTrue(secondaryIncludingDeleted(engine, email, versionA).isPresent());
        } finally {
            engine.close();
        }
    }

    /** committed DELETE 的 secondary entry 必须先物理删除，随后 clustered owner/pointer 精确删除同一行。 */
    @Test
    @DisplayName("delete purge removes secondary before clustered row")
    void deletePurgeRemovesSecondaryAndClusteredRow() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "secondary-purge-delete.ibd");
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord row = row(1, "delete@example.test");
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(insert, table, row,
                    Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction delete = transaction(engine);
            engine.tableDmlService().delete(new TableDeleteCommand(delete, table, primaryKey(1), TIMEOUT));
            commit(engine, delete);

            PurgeSummary summary = engine.purgeCoordinator().runBatch(1);

            assertEquals(1, summary.purgedLogs());
            assertEquals(1, summary.removedSecondaryEntries());
            assertEquals(1, summary.removedClusteredRecords());
            assertTrue(secondaryIncludingDeleted(engine, email, row).isEmpty());
            assertTrue(clustered(engine, table, 1).isEmpty());
        } finally {
            engine.close();
        }
    }

    /** purge 对共享 row guard 只做零等待；busy batch 不删除 entry、不 finalization，也不移动 history head。 */
    @Test
    @DisplayName("purge defers history while row guard is busy")
    void defersHistoryWhileRowGuardIsBusy() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "secondary-purge-busy.ibd");
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord before = row(1, "busy-before@example.test");
            LogicalRecord after = row(1, "busy-after@example.test");
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(insert, table, before,
                    Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction update = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(update, table, primaryKey(1), after, TIMEOUT));
            commit(engine, update);

            try (PurgeDmlRowGuard ignored = engine.purgeCoordinator().rowGuardsForTest()
                    .acquireForDml(TABLE_ID, primaryKey(1), TIMEOUT)) {
                PurgeSummary deferred = engine.purgeCoordinator().runBatch(1);
                assertEquals(0, deferred.purgedLogs());
                assertEquals(1, deferred.deferredLogs());
                assertTrue(secondaryIncludingDeleted(engine, email, before).isPresent());
            }

            PurgeSummary retried = engine.purgeCoordinator().runBatch(1);
            assertEquals(1, retried.purgedLogs());
            assertEquals(1, retried.removedSecondaryEntries());
            assertTrue(secondaryIncludingDeleted(engine, email, before).isEmpty());
        } finally {
            engine.close();
        }
    }

    /**
     * 当前聚簇版本链无法到达 history target 时，purge 无法证明旧 secondary identity 已安全失效，必须 fail-closed。
     * 测试只破坏聚簇隐藏指针，不修改 secondary/history，从而钉住“保留物理 entry 与 barrier”的失败语义。
     */
    @Test
    @DisplayName("purge keeps history when clustered chain cannot reach target undo")
    void keepsHistoryWhenVersionChainCannotReachTarget() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "secondary-purge-detached-chain.ibd");
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord before = row(1, "detached-before@example.test");
            LogicalRecord after = row(1, "detached-after@example.test");
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    insert, table, before, Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction update = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(
                    update, table, primaryKey(1), after, TIMEOUT));
            commit(engine, update);

            // 模拟聚簇页隐藏列损坏/错误恢复：当前行仍是 B，但 DB_ROLL_PTR 被截断，无法走到 A→B 的 target undo。
            LogicalRecord current = clustered(engine, table, 1).orElseThrow().record();
            HiddenColumns owner = current.hiddenColumns();
            LogicalRecord detached = new LogicalRecord(current.schemaVersion(), current.columnValues(),
                    current.deleted(), current.recordType(),
                    new HiddenColumns(owner.dbTrxId(), RollPointer.NULL));
            MiniTransaction corrupt = engine.miniTransactionManager().begin(
                    engine.miniTransactionManager().budgetFor(
                            RedoBudgetPurpose.CLUSTERED_UPDATE, BTreeRedoBudgetEstimator.pointRewrite()));
            assertTrue(engine.btreeService().replaceClustered(corrupt, table.clusteredIndex(),
                    primaryKey(1), detached, owner.dbTrxId(), owner.dbRollPtr()).replaced());
            engine.miniTransactionManager().commit(corrupt);

            assertThrows(SecondaryPurgeVersionChainException.class,
                    () -> engine.purgeCoordinator().runBatch(1));
            assertTrue(secondaryIncludingDeleted(engine, email, before).isPresent(),
                    "fail-closed purge must retain the obsolete candidate when chain proof is unavailable");
            assertEquals(1, engine.tablePurgeBarrier().referenceCount(TABLE_ID),
                    "history and DROP barrier remain authoritative after proof failure");
        } finally {
            engine.close();
        }
    }

    /** secondary removal MTR 提交后崩溃时 history 不能前进；重试把 ABSENT 视为已完成并继续 finalization。 */
    @Test
    @DisplayName("secondary purge task crash retries idempotently before history finalization")
    void retriesAfterSecondaryTaskCommitBeforeHistoryFinalization() {
        MutableTargetResolver resolver = new MutableTargetResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableIndexMetadata table = createTable(engine, "secondary-purge-task-crash.ibd");
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            SecondaryIndexMetadata email = table.requireSecondary(EMAIL_ID);
            LogicalRecord before = row(1, "crash-before@example.test");
            LogicalRecord after = row(1, "crash-after@example.test");
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    insert, table, before, Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction update = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(
                    update, table, primaryKey(1), after, TIMEOUT));
            commit(engine, update);
            engine.purgeCoordinator().installFaultInjectorForTest((phase, indexId) -> {
                if (phase == PurgeProgressPhase.AFTER_SECONDARY_COMMIT && indexId == EMAIL_ID) {
                    throw new DatabaseRuntimeException("synthetic crash after secondary purge commit");
                }
            });

            assertThrows(DatabaseRuntimeException.class, () -> engine.purgeCoordinator().runBatch(1));
            assertTrue(secondaryIncludingDeleted(engine, email, before).isEmpty());
            assertTrue(secondaryIncludingDeleted(engine, email, after).isPresent());
            assertEquals(1, engine.tablePurgeBarrier().referenceCount(TABLE_ID),
                    "history owner remains authoritative until finalization commits");

            engine.purgeCoordinator().installFaultInjectorForTest(PurgeProgressFaultInjector.NO_OP);
            PurgeSummary retry = engine.purgeCoordinator().runBatch(1);
            assertEquals(1, retry.purgedLogs());
            assertEquals(0, retry.removedSecondaryEntries(), "ABSENT task is an idempotent completion proof");
            assertEquals(0, engine.tablePurgeBarrier().referenceCount(TABLE_ID));
        } finally {
            engine.close();
        }
    }

    /**
     * existing-open 的 RESUME_PURGE 必须在后台 worker 禁用时真实消费恢复出的 UPDATE history；open 返回前旧 secondary
     * identity 已物理删除、当前 key 仍存活，且 history owner 已完成 finalization。
     */
    @Test
    @DisplayName("recovery resume purge removes obsolete secondary before traffic opens")
    void recoveryResumePurgeRemovesSecondaryBeforeTrafficOpens() {
        String fileName = "secondary-purge-recovery.ibd";
        Path dataPath = directory.resolve(fileName);
        EngineConfig recoveryConfig = config().withRecoveryTablespaces(List.of(
                new EngineTablespaceConfig(DATA_SPACE, dataPath)));
        MutableTargetResolver resolver = new MutableTargetResolver();
        LogicalRecord before = row(1, "recovery-before@example.test");
        LogicalRecord after = row(1, "recovery-after@example.test");
        TableIndexMetadata table;

        StorageEngine first = new StorageEngine(recoveryConfig);
        first.configureIndexMetadataResolver(resolver);
        first.open();
        try {
            table = createTable(first, fileName);
            resolver.install(new UndoTargetMetadata(table, Optional.empty()));
            Transaction insert = transaction(first);
            first.tableDmlService().insert(new TableInsertCommand(
                    insert, table, before, Optional.empty(), TIMEOUT));
            commit(first, insert);
            Transaction update = transaction(first);
            first.tableDmlService().update(new TableUpdateCommand(
                    update, table, primaryKey(1), after, TIMEOUT));
            commit(first, update);

            assertTrue(secondaryIncludingDeleted(first, table.requireSecondary(EMAIL_ID), before).isPresent(),
                    "committed UPDATE keeps the old delete-marked entry until purge");
            first.checkpoint();
        } finally {
            first.close();
        }

        StorageEngine recovered = new StorageEngine(recoveryConfig);
        recovered.configureIndexMetadataResolver(resolver);
        recovered.open();
        try {
            List<RecoveryStageName> stages = recovered.lastRecoveryReport().orElseThrow().completedStages();
            assertTrue(stages.indexOf(RecoveryStageName.RESUME_PURGE)
                            < stages.indexOf(RecoveryStageName.OPEN_TRAFFIC));
            assertTrue(secondaryIncludingDeleted(
                    recovered, table.requireSecondary(EMAIL_ID), before).isEmpty());
            assertTrue(secondaryIncludingDeleted(
                    recovered, table.requireSecondary(EMAIL_ID), after).isPresent());
            assertEquals(0, recovered.tablePurgeBarrier().referenceCount(TABLE_ID),
                    "recovery rebuild plus RESUME_PURGE must drain the reconstructed table reference");
            assertEquals(0, recovered.purgeCoordinator().runBatch(1).purgedLogs(),
                    "recovery stage must finalize history instead of leaving work for a disabled background worker");
        } finally {
            recovered.close();
        }
    }

    /** 创建聚簇主键和 logical unique email secondary。 */
    private TableIndexMetadata createTable(StorageEngine engine, String fileName) {
        StorageTableDefinition definition = new StorageTableDefinition(TABLE_ID, DATA_SPACE,
                directory.resolve(fileName), 1, PageNo.of(128),
                List.of(new StorageColumnDefinition(1, "id", 0,
                                StorageColumnType.bigint(false, false)),
                        new StorageColumnDefinition(2, "email", 1,
                                new StorageColumnType(StorageColumnTypeId.VARCHAR, false,
                                        160, 0, false, 1, 2, List.of()))),
                List.of(new StorageIndexDefinition(PRIMARY_ID, "PRIMARY", true, true,
                                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                        new StorageIndexDefinition(EMAIL_ID, "uq_email", true, false,
                                List.of(new StorageIndexKeyPart(2, StorageIndexOrder.ASC, 0)))));
        TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition);
        return new BTreeIndexMetadataFactory().createTable(definition, binding);
    }

    private static Transaction transaction(StorageEngine engine) {
        return engine.transactionManager().begin(TransactionOptions.defaults());
    }

    private static void commit(StorageEngine engine, Transaction transaction) {
        engine.tableDmlService().commit(new DmlCommitCommand(transaction,
                DurabilityPolicy.FLUSH_ON_COMMIT, TIMEOUT));
    }

    private static LogicalRecord row(long id, String email) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(email)), false, RecordType.CONVENTIONAL);
    }

    private static SearchKey primaryKey(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static Optional<BTreeLookupResult> clustered(StorageEngine engine,
                                                          TableIndexMetadata table, long id) {
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> result = engine.btreeService().lookupIncludingDeleted(
                    read, table.clusteredIndex(), primaryKey(id));
            engine.miniTransactionManager().commit(read);
            return result;
        } catch (RuntimeException error) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw error;
        }
    }

    private static Optional<BTreeLookupResult> secondaryIncludingDeleted(StorageEngine engine,
                                                                          SecondaryIndexMetadata secondary,
                                                                          LogicalRecord row) {
        LogicalRecord entry = secondary.layout().toEntry(row, false);
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> result = engine.btreeService().lookupIncludingDeleted(
                    read, secondary.index(), secondary.layout().physicalKey(entry));
            engine.miniTransactionManager().commit(read);
            return result;
        } catch (RuntimeException error) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw error;
        }
    }

    /** open 前注入、DDL 后安装 exact-version 表级目标。 */
    private static final class MutableTargetResolver
            implements IndexMetadataResolver, UndoTargetMetadataResolver {
        private volatile UndoTargetMetadata target;

        private void install(UndoTargetMetadata target) {
            this.target = target;
        }

        @Override
        public BTreeIndex resolve(long tableId, long indexId) {
            return requireTarget(tableId).tableIndexes().requireIndex(indexId);
        }

        @Override
        public UndoTargetMetadata resolveTarget(long tableId, long indexId) {
            UndoTargetMetadata resolved = requireTarget(tableId);
            if (resolved.clusteredIndex().indexId() != indexId) {
                throw new DatabaseValidationException("unexpected clustered index id: " + indexId);
            }
            return resolved;
        }

        private UndoTargetMetadata requireTarget(long tableId) {
            UndoTargetMetadata resolved = target;
            if (resolved == null || resolved.tableIndexes().tableId() != tableId) {
                throw new DatabaseValidationException("unknown purge test table: " + tableId);
            }
            return resolved;
        }
    }

    /** 禁用全部后台 worker，使测试只由显式 purge batch 推进 history。 */
    private EngineConfig config() {
        return new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 256,
                SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(30),
                64L * 1024 * 1024, List.of(), false, 4, Duration.ofSeconds(1),
                256, Duration.ofSeconds(30));
    }
}
