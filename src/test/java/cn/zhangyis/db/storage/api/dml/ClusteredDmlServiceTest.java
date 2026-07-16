package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.recovery.RecoveryTrafficGate;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.redo.DurabilityPolicy;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.TransactionOptions;
import cn.zhangyis.db.storage.trx.TransactionSavepoint;
import cn.zhangyis.db.storage.trx.TransactionState;
import cn.zhangyis.db.storage.trx.TransactionStateException;
import cn.zhangyis.db.storage.trx.TransactionSystem;
import cn.zhangyis.db.storage.trx.UndoContext;
import cn.zhangyis.db.storage.trx.UndoTestContexts;
import cn.zhangyis.db.storage.trx.UndoTargetMetadata;
import cn.zhangyis.db.storage.trx.UndoTargetMetadataResolver;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.trx.lock.LockWaitTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 单聚簇索引 DML facade 的行为测试。测试优先覆盖 public storage API 的事务/undo/B+Tree 编排语义，
 * 不直接依赖 BufferFrame、RecordCursor 或 undo page 内部结构。
 */
class ClusteredDmlServiceTest {

    private static final SpaceId SPACE = SpaceId.of(10);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;
    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path dir;

    @Test
    @DisplayName("DML command objects reject null required fields")
    void commandObjectsRejectNullRequiredFields() {
        Duration timeout = Duration.ofSeconds(1);

        assertThrows(DatabaseValidationException.class,
                () -> new DmlCommitCommand(null, DurabilityPolicy.FLUSH_ON_COMMIT, timeout));
        assertThrows(DatabaseValidationException.class,
                () -> new DmlCommitCommand(transaction(), null, timeout));
        assertThrows(DatabaseValidationException.class,
                () -> new DmlCommitCommand(transaction(), DurabilityPolicy.FLUSH_ON_COMMIT, null));
        assertThrows(DatabaseValidationException.class,
                () -> new DmlRollbackCommand(null, clusteredIndex()));
        assertThrows(DatabaseValidationException.class,
                () -> new DmlRollbackCommand(transaction(), null));
    }

    @Test
    @DisplayName("INSERT through DML facade writes clustered row with transaction id and undo roll pointer")
    void insertThroughFacadeWritesClusteredRowWithUndoPointer() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-insert.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());

            DmlWriteResult result = engine.dmlService().insert(new ClusteredInsertCommand(txn, index,
                    search(1), row(1, "v1"), TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));

            BTreeLookupResult found = lookup(engine, index, 1).orElseThrow();
            HiddenColumns hidden = found.record().hiddenColumns();
            assertTrue(result.changed());
            assertEquals(1, result.affectedRows());
            assertEquals(txn.transactionId(), result.transactionId());
            assertEquals(txn.transactionId(), hidden.dbTrxId(), "DB_TRX_ID 来自 facade 分配的写事务 id");
            assertFalse(hidden.dbRollPtr().isNull(), "DB_ROLL_PTR 必须指向 planInsert/appendPlanned 产生的真实 undo record");
            assertEquals("v1", payloadOf(found));
        } finally {
            engine.close();
        }
    }

    /** 超过 inline 阈值的多个 LOB 必须在同一 INSERT MTR externalize，并把完整值留在可校验页链。 */
    @Test
    void insertExternalizesMultipleLobsThroughPreparedProtocol() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            LobIndexSetup setup = createLobClusteredIndex(engine, dir.resolve("dml-lob.ibd"));
            String text = "界".repeat(120);
            byte[] binary = new byte[300];
            java.util.Arrays.fill(binary, (byte) 0x5A);
            LogicalRecord record = new LogicalRecord(1, List.of(new ColumnValue.IntValue(1),
                    new ColumnValue.StringValue(text), new ColumnValue.BinaryValue(binary)),
                    false, RecordType.CONVENTIONAL);
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());

            engine.dmlService().insert(new ClusteredInsertCommand(txn, setup.index(), search(1), record,
                    TABLE_ID, Optional.of(setup.lobSegment()), Duration.ofSeconds(1)));

            BTreeLookupResult found = lookup(engine, setup.index(), 1).orElseThrow();
            assertTrue(found.record().columnValues().get(1) instanceof ColumnValue.ExternalValue);
            assertTrue(found.record().columnValues().get(2) instanceof ColumnValue.ExternalValue);
            MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
            ColumnValue textValue = engine.lobStorage().read(read, lobSchema().column(1).type(),
                    (ColumnValue.ExternalValue) found.record().columnValues().get(1));
            ColumnValue binaryValue = engine.lobStorage().read(read, lobSchema().column(2).type(),
                    (ColumnValue.ExternalValue) found.record().columnValues().get(2));
            engine.miniTransactionManager().commit(read);
            assertEquals(text, ((ColumnValue.StringValue) textValue).value());
            assertArrayEquals(binary, ((ColumnValue.BinaryValue) binaryValue).value());
        } finally {
            engine.close();
        }
    }

    /** 旧表没有 LOB binding 时必须在业务写 MTR 前拒绝，不能发布 row 或创建 undo context。 */
    @Test
    void externalLobWithoutBindingFailsBeforeUndoOrRowSideEffects() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            LobIndexSetup setup = createLobClusteredIndex(engine, dir.resolve("dml-lob-missing.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            LogicalRecord record = new LogicalRecord(1, List.of(new ColumnValue.IntValue(2),
                    new ColumnValue.StringValue("x".repeat(300)), new ColumnValue.BinaryValue(new byte[]{1})),
                    false, RecordType.CONVENTIONAL);

            assertThrows(DmlLobBindingException.class, () -> engine.dmlService().insert(
                    new ClusteredInsertCommand(txn, setup.index(), search(2), record,
                            TABLE_ID, Optional.empty(), Duration.ofSeconds(1))));

            assertNull(txn.undoContext());
            assertTrue(lookup(engine, setup.index(), 2).isEmpty());
        } finally {
            engine.close();
        }
    }

    /** full rollback 不接收 last-index fallback，并在 marker MTR 中释放 INSERT ownership 后推进 undo head。 */
    @Test
    void resolvedFullRollbackDeletesRowAndReclaimsExternalLob() {
        StorageEngine engine = new StorageEngine(config(dir));
        MutableUndoTargetResolver resolver = new MutableUndoTargetResolver();
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            LobIndexSetup setup = createLobClusteredIndex(engine, dir.resolve("dml-lob-rollback.ibd"));
            resolver.install(new UndoTargetMetadata(setup.index(), Optional.of(setup.lobSegment())));
            String text = "回滚".repeat(160);
            LogicalRecord record = new LogicalRecord(1, List.of(new ColumnValue.IntValue(3),
                    new ColumnValue.StringValue(text), new ColumnValue.BinaryValue(new byte[]{1})),
                    false, RecordType.CONVENTIONAL);
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(txn, setup.index(), search(3), record,
                    TABLE_ID, Optional.of(setup.lobSegment()), Duration.ofSeconds(1)));
            ColumnValue.ExternalValue external = (ColumnValue.ExternalValue) lookup(engine, setup.index(), 3)
                    .orElseThrow().record().columnValues().get(1);

            DmlRollbackResult result = engine.dmlService().rollback(new ResolvedDmlRollbackCommand(txn));

            assertEquals(1, result.rollbackSummary().undoRecordsApplied());
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertTrue(lookup(engine, setup.index(), 3).isEmpty());
            MiniTransaction readFreed = engine.miniTransactionManager().beginReadOnly();
            assertThrows(DatabaseRuntimeException.class,
                    () -> engine.lobStorage().read(readFreed, lobSchema().column(1).type(), external));
            engine.miniTransactionManager().rollbackUncommitted(readFreed);
        } finally {
            engine.close();
        }
    }

    /** statement rollback 与 full/recovery 共用 ownership free+head marker，完成后事务仍保持 ACTIVE。 */
    @Test
    void statementRollbackReclaimsExternalLobAndKeepsTransactionActive() {
        StorageEngine engine = new StorageEngine(config(dir));
        MutableUndoTargetResolver resolver = new MutableUndoTargetResolver();
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            LobIndexSetup setup = createLobClusteredIndex(engine, dir.resolve("dml-lob-statement.ibd"));
            resolver.install(new UndoTargetMetadata(setup.index(), Optional.of(setup.lobSegment())));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            LogicalRecord record = new LogicalRecord(1, List.of(new ColumnValue.IntValue(4),
                    new ColumnValue.StringValue("语句".repeat(160)), new ColumnValue.BinaryValue(new byte[]{1})),
                    false, RecordType.CONVENTIONAL);

            try (DmlStatementGuard statement = engine.dmlService().beginStatement(txn, setup.index())) {
                engine.dmlService().insert(new ClusteredInsertCommand(txn, setup.index(), search(4), record,
                        TABLE_ID, Optional.of(setup.lobSegment()), Duration.ofSeconds(1)));
                assertEquals(1, statement.rollback().undoRecordsApplied());
            }

            assertEquals(TransactionState.ACTIVE, txn.state());
            assertTrue(lookup(engine, setup.index(), 4).isEmpty());
            assertTrue(txn.undoContext().head(UndoLogKind.INSERT).isEmpty());
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("UPDATE through DML facade writes update undo and replaces clustered row")
    void updateThroughFacadeReplacesClusteredRowWithNewHiddenColumns() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-update.ibd"));
            insertAndCommit(engine, index, 1, "v1");
            Transaction updater = engine.transactionManager().begin(TransactionOptions.defaults());

            DmlWriteResult result = engine.dmlService().update(new ClusteredUpdateCommand(updater, index,
                    search(1), row(1, "v2"), TABLE_ID, Duration.ofSeconds(1)));

            BTreeLookupResult found = lookup(engine, index, 1).orElseThrow();
            HiddenColumns hidden = found.record().hiddenColumns();
            assertTrue(result.changed());
            assertEquals(1, result.affectedRows());
            assertEquals(updater.transactionId(), result.transactionId());
            assertEquals("v2", payloadOf(found));
            assertEquals(updater.transactionId(), hidden.dbTrxId(), "UPDATE 后 DB_TRX_ID 指向更新事务");
            assertFalse(hidden.dbRollPtr().isNull(), "UPDATE 后 DB_ROLL_PTR 指向 UPDATE undo record");
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("DELETE through DML facade writes delete-mark undo and hides clustered row")
    void deleteThroughFacadeMarksClusteredRowDeleted() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-delete.ibd"));
            insertAndCommit(engine, index, 1, "v1");
            Transaction deleter = engine.transactionManager().begin(TransactionOptions.defaults());

            DmlWriteResult result = engine.dmlService().delete(new ClusteredDeleteCommand(deleter, index,
                    search(1), TABLE_ID, Duration.ofSeconds(1)));

            assertTrue(result.changed());
            assertEquals(1, result.affectedRows());
            assertTrue(lookup(engine, index, 1).isEmpty(), "普通 lookup 过滤 delete-marked 当前版本");
            BTreeLookupResult raw = lookupIncludingDeleted(engine, index, 1).orElseThrow();
            HiddenColumns hidden = raw.record().hiddenColumns();
            assertTrue(raw.record().deleted(), "delete-marked 当前版本仍供 MVCC/rollback/purge 定位");
            assertEquals(deleter.transactionId(), hidden.dbTrxId(), "DELETE 后 DB_TRX_ID 指向删除事务");
            assertFalse(hidden.dbRollPtr().isNull(), "DELETE 后 DB_ROLL_PTR 指向 DELETE_MARK undo record");
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("COMMIT through DML facade waits durability and releases transaction row locks")
    void commitThroughFacadeWaitsDurabilityAndReleasesLocks() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-commit.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(1), row(1, "v1"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));
            assertTrue(hasGrantedLock(engine, txn), "INSERT unique check keeps its transaction lock until commit");

            DmlCommitResult result = engine.dmlService().commit(new DmlCommitCommand(txn,
                    DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofSeconds(2)));

            assertEquals(TransactionState.COMMITTED, txn.state());
            assertFalse(txn.transactionNo().isNone(), "read-write commit assigns a transaction no");
            assertTrue(result.durable(), "FLUSH_ON_COMMIT must report durable after redo fsync wait");
            assertEquals(txn.transactionNo(), result.transactionNo());
            assertTrue(result.releasedLockCount() > 0, "commit releases row locks owned by the transaction");
            assertFalse(hasGrantedLock(engine, txn), "commit cleanup removes row-lock observability state");
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("COMMIT durability timeout leaves transaction committed but releases row locks")
    void commitDurabilityTimeoutReleasesLocks() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-commit-timeout.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(1), row(1, "v1"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));
            assertTrue(hasGrantedLock(engine, txn), "durability timeout 前事务锁仍由写事务持有");

            RedoLogManager neverDurable = new RedoLogManager();
            neverDurable.append(List.of(new PageInitRecord(PageId.of(SPACE, PageNo.of(50)), PageType.INDEX)));
            ClusteredDmlService timeoutService = serviceWithRedo(engine, neverDurable);

            assertThrows(DmlOperationException.class, () -> timeoutService.commit(new DmlCommitCommand(txn,
                    DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofNanos(1))));

            assertEquals(TransactionState.COMMITTED, txn.state(),
                    "durability 等待失败发生在事务状态 commit 之后，不能再自动 rollback");
            assertFalse(hasGrantedLock(engine, txn), "commit-uncertain failure must still release row locks");
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("COMMIT onCommit failure leaves transaction active and keeps row locks")
    void commitOnCommitFailureKeepsTransactionRecoverableAndLocksHeld() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-on-commit-failure.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(1), row(1, "v1"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));
            assertTrue(hasGrantedLock(engine, txn), "写事务在 commit 前持有 INSERT/current-read 锁");

            replaceUndoContext(txn, UndoTestContexts.restored(RollbackSegmentId.of(0), UndoLogKind.INSERT,
                    UndoSlotId.of(0), PageId.of(SPACE, PageNo.of(9999)), UndoNo.NONE,
                    UndoLogicalHead.EMPTY));

            assertThrows(DatabaseRuntimeException.class, () -> engine.dmlService().commit(new DmlCommitCommand(txn,
                    DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofSeconds(1))));

            assertEquals(TransactionState.ACTIVE, txn.state(),
                    "undo commit marker 未持久化时，事务不能进入 COMMITTED 或移出 active 语义");
            assertTrue(hasGrantedLock(engine, txn),
                    "未提交事务的 row locks 不能因 onCommit 失败被释放，否则其它事务会看到未提交版本");
            MiniTransaction probe = engine.miniTransactionManager().beginReadOnly();
            engine.miniTransactionManager().rollbackUncommitted(probe);
            engine.lockManager().releaseAll(txn.transactionId());
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("ROLLBACK through DML facade applies mixed undo chain and releases row locks")
    void rollbackThroughFacadeAppliesMixedUndoAndReleasesLocks() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-rollback.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(1), row(1, "v1"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));
            engine.dmlService().update(new ClusteredUpdateCommand(txn, index, search(1), row(1, "v2"),
                    TABLE_ID, Duration.ofSeconds(1)));
            engine.dmlService().delete(new ClusteredDeleteCommand(txn, index, search(1),
                    TABLE_ID, Duration.ofSeconds(1)));
            assertTrue(hasGrantedLock(engine, txn), "事务回滚前持有 current-read/insert-intention 锁");

            DmlRollbackResult result = engine.dmlService().rollback(new DmlRollbackCommand(txn, index));

            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertEquals(3, result.rollbackSummary().undoRecordsApplied(),
                    "rollback must consume delete, update, and insert undo records");
            assertTrue(result.releasedLockCount() > 0, "rollback releases row locks owned by the transaction");
            assertFalse(hasGrantedLock(engine, txn), "rollback cleanup removes granted locks");
            assertTrue(lookupIncludingDeleted(engine, index, 1).isEmpty(),
                    "insert->update->delete in one transaction rolls back to non-existence");
        } finally {
            engine.close();
        }
    }

    /** full rollback 尚未进入终态时失败必须保留事务锁，避免其它事务观察或覆盖半回滚数据。 */
    @Test
    @DisplayName("A failed full rollback keeps row locks until a successful retry")
    void failedFullRollbackKeepsLocksUntilSuccessfulRetry() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-rollback-lock-retain.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(
                    txn, index, search(1), row(1, "v1"), TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));
            assertTrue(hasGrantedLock(engine, txn));

            IndexKeyDef wrongKey = new IndexKeyDef(INDEX_ID + 1, index.keyDef().parts());
            BTreeIndex wrongIndex = new BTreeIndex(INDEX_ID + 1, index.rootPageId(), index.rootLevel(),
                    wrongKey, index.schema(), index.unique(), index.leafSegment(), index.nonLeafSegment());

            assertThrows(DatabaseRuntimeException.class,
                    () -> engine.dmlService().rollback(new DmlRollbackCommand(txn, wrongIndex)));
            assertEquals(TransactionState.ACTIVE, txn.state(),
                    "chain preflight fails before entering ROLLING_BACK");
            assertTrue(hasGrantedLock(engine, txn),
                    "failed rollback must retain row locks while transaction is not terminal");

            DmlRollbackResult retried = engine.dmlService().rollback(new DmlRollbackCommand(txn, index));
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertEquals(1, retried.rollbackSummary().undoRecordsApplied());
            assertFalse(hasGrantedLock(engine, txn));
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("Statement guard rolls back only DML writes after an existing undo boundary")
    void statementGuardRollsBackOnlyWritesAfterExistingUndoBoundary() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-statement-existing.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(1), row(1, "v1"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));

            DmlStatementGuard guard = engine.dmlService().beginStatement(txn, index);
            engine.dmlService().update(new ClusteredUpdateCommand(txn, index, search(1), row(1, "v2"),
                    TABLE_ID, Duration.ofSeconds(1)));
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(2), row(2, "new"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));

            var summary = guard.rollback();

            assertEquals(2, summary.undoRecordsApplied(), "guard rollback must undo only writes after the boundary");
            assertEquals(TransactionState.ACTIVE, txn.state(), "statement rollback must keep transaction active");
            assertEquals("v1", payloadOf(lookup(engine, index, 1).orElseThrow()));
            assertTrue(lookupIncludingDeleted(engine, index, 2).isEmpty(),
                    "insert created inside the failed statement must be physically removed");
            assertEquals(UndoNo.of(1), txn.undoContext().head(UndoLogKind.INSERT).undoNo(),
                    "guard rollback moves the logical chain head back to the saved boundary");

            DmlRollbackResult full = engine.dmlService().rollback(new DmlRollbackCommand(txn, index));
            assertEquals(1, full.rollbackSummary().undoRecordsApplied(),
                    "full rollback after statement rollback must consume only the pre-guard insert");
            assertTrue(lookupIncludingDeleted(engine, index, 1).isEmpty());
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("Statement guard can roll back the first write to an empty undo boundary")
    void statementGuardRollsBackFirstWriteToEmptyUndoBoundary() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-statement-empty.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());

            DmlStatementGuard guard = engine.dmlService().beginStatement(txn, index);
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(1), row(1, "rolled-back"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));

            var summary = guard.rollback();

            assertEquals(1, summary.undoRecordsApplied(), "empty-boundary rollback must undo the first write");
            assertEquals(TransactionState.ACTIVE, txn.state());
            assertTrue(lookupIncludingDeleted(engine, index, 1).isEmpty());
            assertNotNull(txn.undoContext(), "v1 keeps the allocated undo context and slot for later writes");
            assertEquals(UndoNo.of(1), txn.undoContext().lastUndoNo(),
                    "append high-water mark must not be reused after statement rollback");
            assertEquals(UndoNo.NONE, txn.undoContext().head(UndoLogKind.INSERT).undoNo(),
                    "current logical undo chain is empty after rolling back to the empty boundary");
            assertTrue(txn.undoContext().head(UndoLogKind.INSERT).rollPointer().isNull());

            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(2), row(2, "after"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));
            assertEquals(UndoNo.of(2), txn.undoContext().lastUndoNo(),
                    "new writes after empty-boundary rollback continue with a fresh undo number");

            DmlRollbackResult full = engine.dmlService().rollback(new DmlRollbackCommand(txn, index));
            assertEquals(1, full.rollbackSummary().undoRecordsApplied(),
                    "full rollback must see only the post-rollback insert in the current logical chain");
            assertTrue(lookupIncludingDeleted(engine, index, 2).isEmpty());
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("Closing a successful statement guard keeps writes for later transaction rollback")
    void statementGuardCloseKeepsSuccessfulWritesForFullRollback() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-statement-success.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(1), row(1, "v1"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));

            try (DmlStatementGuard ignored = engine.dmlService().beginStatement(txn, index)) {
                engine.dmlService().update(new ClusteredUpdateCommand(txn, index, search(1), row(1, "v2"),
                        TABLE_ID, Duration.ofSeconds(1)));
            }

            assertEquals("v2", payloadOf(lookup(engine, index, 1).orElseThrow()),
                    "normal close is a success path and must not perform statement rollback");
            DmlRollbackResult full = engine.dmlService().rollback(new DmlRollbackCommand(txn, index));
            assertEquals(2, full.rollbackSummary().undoRecordsApplied(),
                    "transaction rollback still consumes the successful statement update plus the original insert");
            assertTrue(lookupIncludingDeleted(engine, index, 1).isEmpty());
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("Closing an empty statement guard still requires an active transaction")
    void emptyStatementGuardCloseRequiresActiveTransaction() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-statement-close-state.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            DmlStatementGuard guard = engine.dmlService().beginStatement(txn, index);
            engine.transactionManager().commit(txn);

            assertThrows(TransactionStateException.class, guard::close,
                    "both empty and savepoint-backed guards must reject close after the transaction ends");
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("A statement rollback failure is terminal for the guard")
    void statementGuardRollbackFailureIsTerminal() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-statement-failed-state.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            DmlStatementGuard guard = engine.dmlService().beginStatement(txn, index);
            engine.transactionManager().commit(txn);

            assertThrows(TransactionStateException.class, guard::rollback,
                    "the first rollback exposes the concrete transaction-state failure");
            assertThrows(DmlOperationException.class, guard::rollback,
                    "an outcome-uncertain rollback must not be retried through the same guard");
        } finally {
            engine.close();
        }
    }

    /**
     * ACTIVE 事务的 statement rollback 若在边界校验阶段失败，Guard 必须把事务标成 rollback-only；否则调用方
     * 仍可走 facade commit，把 outcome-uncertain 的部分语句结果持久化。
     */
    @Test
    @DisplayName("An active statement rollback failure dooms the transaction until full rollback")
    void activeStatementRollbackFailureMarksTransactionRollbackOnly() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-statement-doomed.ibd"));
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(1), row(1, "v1"),
                    TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));
            TransactionSavepoint real = engine.rollbackService().createSavepoint(txn);
            TransactionSavepoint detached = new TransactionSavepoint(
                    txn, real.insertHead(), real.updateHead(), real.sequence() + 1_000);
            DmlStatementGuard guard = DmlStatementGuard.savepointBoundary(
                    engine.rollbackService(), txn, index, detached);

            assertThrows(DatabaseValidationException.class, guard::rollback);
            assertTrue(txn.rollbackOnly(), "failed statement rollback must revoke commit eligibility");
            assertThrows(TransactionStateException.class, () -> engine.dmlService().commit(
                    new DmlCommitCommand(txn, DurabilityPolicy.FLUSH_ON_COMMIT, Duration.ofSeconds(1))));
            assertEquals(TransactionState.ACTIVE, txn.state(), "doomed transaction remains active for full rollback");

            DmlRollbackResult result = engine.dmlService().rollback(new DmlRollbackCommand(txn, index));
            assertEquals(1, result.rollbackSummary().undoRecordsApplied());
            assertEquals(TransactionState.ROLLED_BACK, txn.state());
            assertTrue(lookupIncludingDeleted(engine, index, 1).isEmpty());
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("INSERT duplicate through DML facade throws duplicate key and keeps original row")
    void duplicateInsertThroughFacadeThrowsAndKeepsOriginalRow() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-duplicate.ibd"));
            insertAndCommit(engine, index, 1, "v1");
            Transaction duplicate = engine.transactionManager().begin(TransactionOptions.defaults());

            assertThrows(DmlDuplicateKeyException.class, () -> engine.dmlService().insert(
                    new ClusteredInsertCommand(duplicate, index, search(1), row(1, "v2"),
                            TABLE_ID, Optional.empty(), Duration.ofSeconds(1))));

            assertEquals("v1", payloadOf(lookup(engine, index, 1).orElseThrow()));
            engine.lockManager().releaseAll(duplicate.transactionId());
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("UPDATE and DELETE miss through DML facade do not allocate undo context")
    void missingRowsDoNotAllocateUndoContext() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-miss.ibd"));
            Transaction updater = engine.transactionManager().begin(TransactionOptions.defaults());
            DmlWriteResult update = engine.dmlService().update(new ClusteredUpdateCommand(updater, index,
                    search(404), row(404, "missing"), TABLE_ID, Duration.ofMillis(100)));
            assertFalse(update.changed());
            assertEquals(0, update.affectedRows());
            assertNull(updater.undoContext(), "miss update must not write orphan undo");

            Transaction deleter = engine.transactionManager().begin(TransactionOptions.defaults());
            DmlWriteResult delete = engine.dmlService().delete(new ClusteredDeleteCommand(deleter, index,
                    search(404), TABLE_ID, Duration.ofMillis(100)));
            assertFalse(delete.changed());
            assertEquals(0, delete.affectedRows());
            assertNull(deleter.undoContext(), "miss delete must not write orphan undo");
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("DML facade rejects ordinary traffic while recovery gate is not open")
    void closedRecoveryGateRejectsDmlBeforeStateMutation() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            ClusteredDmlService closed = closedGateService(engine);
            Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());

            assertThrows(DmlOperationException.class, () -> closed.commit(new DmlCommitCommand(txn,
                    DurabilityPolicy.BACKGROUND_FLUSH, Duration.ofMillis(100))));

            assertEquals(TransactionState.ACTIVE, txn.state(), "gate rejection must happen before commit state change");
        } finally {
            engine.close();
        }
    }

    @Test
    @DisplayName("Lock timeout from current-read propagates through DML facade without latch leak")
    void lockTimeoutPropagatesAndSubsequentUpdateCanProceedAfterOwnerRollback() {
        StorageEngine engine = new StorageEngine(config(dir));
        engine.open();
        try {
            BTreeIndex index = createClusteredIndex(engine, dir.resolve("dml-lock-timeout.ibd"));
            insertAndCommit(engine, index, 1, "aa");
            Transaction holder = engine.transactionManager().begin(TransactionOptions.defaults());
            engine.dmlService().update(new ClusteredUpdateCommand(holder, index, search(1), row(1, "bb"),
                    TABLE_ID, Duration.ofSeconds(1)));

            Transaction waiter = engine.transactionManager().begin(TransactionOptions.defaults());
            assertThrows(LockWaitTimeoutException.class, () -> engine.dmlService().update(
                    new ClusteredUpdateCommand(waiter, index, search(1), row(1, "cc"),
                            TABLE_ID, Duration.ofMillis(20))));

            engine.dmlService().rollback(new DmlRollbackCommand(holder, index));
            Transaction after = engine.transactionManager().begin(TransactionOptions.defaults());
            DmlWriteResult retried = engine.dmlService().update(new ClusteredUpdateCommand(after, index,
                    search(1), row(1, "dd"), TABLE_ID, Duration.ofSeconds(1)));
            assertEquals(1, retried.affectedRows(), "after lock owner rollback, current-read can relock and update");
            engine.dmlService().rollback(new DmlRollbackCommand(after, index));
        } finally {
            engine.close();
        }
    }

    private static Transaction transaction() {
        return new TransactionManager(new TransactionSystem()).begin(TransactionOptions.defaults());
    }

    private static EngineConfig config(Path dir) {
        return new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    private static BTreeIndex createClusteredIndex(StorageEngine engine, Path dataPath) {
        DiskSpaceManager disk = engine.diskSpaceManager();
        MiniTransaction boot = engine.miniTransactionManager().begin(
                engine.miniTransactionManager().budgetFor(RedoBudgetPurpose.ENGINE_BOOT));
        disk.createTablespace(boot, SPACE, dataPath, PageNo.of(64));
        SegmentRef leaf = disk.createSegment(boot, SPACE, SegmentPurpose.INDEX_LEAF);
        SegmentRef nonLeaf = disk.createSegment(boot, SPACE, SegmentPurpose.INDEX_NON_LEAF);
        PageId root = disk.allocatePage(boot, leaf);
        engine.indexPageAccess().createIndexPage(boot, root, INDEX_ID, 0);
        engine.miniTransactionManager().commit(boot);
        return new BTreeIndex(INDEX_ID, root, 0, idKey(), clusteredSchema(), true, leaf, nonLeaf);
    }

    /** 创建同表空间 leaf/non-leaf/LOB 三 segment 的聚簇索引 fixture。 */
    private static LobIndexSetup createLobClusteredIndex(StorageEngine engine, Path dataPath) {
        DiskSpaceManager disk = engine.diskSpaceManager();
        MiniTransaction boot = engine.miniTransactionManager().begin(
                engine.miniTransactionManager().budgetFor(RedoBudgetPurpose.ENGINE_BOOT));
        disk.createTablespace(boot, SPACE, dataPath, PageNo.of(64));
        SegmentRef leaf = disk.createSegment(boot, SPACE, SegmentPurpose.INDEX_LEAF);
        SegmentRef nonLeaf = disk.createSegment(boot, SPACE, SegmentPurpose.INDEX_NON_LEAF);
        SegmentRef lob = disk.createSegment(boot, SPACE, SegmentPurpose.LOB);
        PageId root = disk.allocatePage(boot, leaf);
        engine.indexPageAccess().createIndexPage(boot, root, INDEX_ID, 0);
        engine.miniTransactionManager().commit(boot);
        return new LobIndexSetup(new BTreeIndex(INDEX_ID, root, 0, idKey(), lobSchema(), true,
                leaf, nonLeaf), lob);
    }

    private static Optional<BTreeLookupResult> lookup(StorageEngine engine, BTreeIndex index, long id) {
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> found = engine.btreeService().lookup(read, index, search(id));
            engine.miniTransactionManager().commit(read);
            return found;
        } catch (RuntimeException e) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw e;
        }
    }

    private static Optional<BTreeLookupResult> lookupIncludingDeleted(StorageEngine engine, BTreeIndex index,
                                                                      long id) {
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> found = engine.btreeService()
                    .lookupIncludingDeleted(read, index, search(id));
            engine.miniTransactionManager().commit(read);
            return found;
        } catch (RuntimeException e) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw e;
        }
    }

    private static void insertAndCommit(StorageEngine engine, BTreeIndex index, long id, String payload) {
        Transaction txn = engine.transactionManager().begin(TransactionOptions.defaults());
        engine.dmlService().insert(new ClusteredInsertCommand(txn, index, search(id), row(id, payload),
                TABLE_ID, Optional.empty(), Duration.ofSeconds(1)));
        engine.dmlService().commit(new DmlCommitCommand(txn, DurabilityPolicy.FLUSH_ON_COMMIT,
                Duration.ofSeconds(2)));
    }

    private static boolean hasGrantedLock(StorageEngine engine, Transaction txn) {
        return engine.lockManager().snapshot().grantedLocks().stream()
                .anyMatch(lock -> lock.owner().equals(txn.transactionId()));
    }

    private static void replaceUndoContext(Transaction txn, UndoContext context) {
        try {
            Field field = Transaction.class.getDeclaredField("undoContext");
            field.setAccessible(true);
            field.set(txn, context);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("failed to install test undo context", e);
        }
    }

    private static ClusteredDmlService closedGateService(StorageEngine engine) {
        return serviceWithGate(engine, new RecoveryTrafficGate());
    }

    private static ClusteredDmlService serviceWithRedo(StorageEngine engine, RedoLogManager redo) {
        RecoveryTrafficGate gate = new RecoveryTrafficGate();
        gate.openForUserTraffic();
        return new ClusteredDmlService(engine.transactionManager(), engine.undoLogManager(),
                engine.miniTransactionManager(), engine.btreeService(), engine.btreeCurrentReadService(),
                engine.rollbackService(), engine.lockManager(), redo, gate, engine.lobStorage());
    }

    private static ClusteredDmlService serviceWithGate(StorageEngine engine, RecoveryTrafficGate gate) {
        return new ClusteredDmlService(engine.transactionManager(), engine.undoLogManager(),
                engine.miniTransactionManager(), engine.btreeService(), engine.btreeCurrentReadService(),
                engine.rollbackService(), engine.lockManager(),
                engine.miniTransactionManager().redoLogManager(), gate, engine.lobStorage());
    }

    private static BTreeIndex clusteredIndex() {
        return new BTreeIndex(INDEX_ID, PageId.of(SPACE, PageNo.of(3)), 0,
                idKey(), clusteredSchema(), true);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(200, true), 1)), true);
    }

    private static TableSchema lobSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "text_value", ColumnType.text(false), 1),
                new ColumnDef(new ColumnId(2), "blob_value", ColumnType.blob(false), 2)), true);
    }

    /** LOB DML fixture 的聚簇索引与权威 table LOB segment。 */
    private record LobIndexSetup(BTreeIndex index, SegmentRef lobSegment) {
    }

    /** open 前注入、建表后安装精确 target 的测试 resolver；生产 Dictionary adapter 不需要这层可变接缝。 */
    private static final class MutableUndoTargetResolver
            implements IndexMetadataResolver, UndoTargetMetadataResolver {
        private UndoTargetMetadata target;

        private void install(UndoTargetMetadata target) {
            this.target = target;
        }

        @Override
        public BTreeIndex resolve(long tableId, long indexId) {
            return requireTarget(tableId, indexId).clusteredIndex();
        }

        @Override
        public UndoTargetMetadata resolveTarget(long tableId, long indexId) {
            return requireTarget(tableId, indexId);
        }

        private UndoTargetMetadata requireTarget(long tableId, long indexId) {
            if (target == null || tableId != TABLE_ID || target.clusteredIndex().indexId() != indexId) {
                throw new DatabaseValidationException("unknown test undo target");
            }
            return target;
        }
    }

    private static SearchKey search(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String payload) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(payload)), false, RecordType.CONVENTIONAL);
    }

    private static String payloadOf(BTreeLookupResult result) {
        return ((ColumnValue.StringValue) result.record().columnValues().get(1)).value();
    }
}
