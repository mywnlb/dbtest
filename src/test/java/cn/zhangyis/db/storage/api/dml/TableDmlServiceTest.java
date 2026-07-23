package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
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
import cn.zhangyis.db.storage.api.trx.CommitPreparedTransactionCommand;
import cn.zhangyis.db.storage.api.trx.PrepareTransactionCommand;
import cn.zhangyis.db.storage.api.trx.ResolvedRollbackPreparedTransactionCommand;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.EngineTablespaceConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.redo.DurabilityPolicy;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionOptions;
import cn.zhangyis.db.storage.trx.TransactionState;
import cn.zhangyis.db.storage.trx.UndoTargetMetadata;
import cn.zhangyis.db.storage.trx.UndoTargetMetadataResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 表级 DML 的生产组合测试。测试从 {@link StorageEngine#tableDmlService()} 进入，并检查聚簇与两棵二级树的
 * 最终物理状态，避免只验证 undo tail 或测试专用 B+Tree 原语而遗漏真实写入编排。
 */
class TableDmlServiceTest {

    /** 测试表稳定身份；undo、row guard 和 DD 风格 metadata 均使用同一值。 */
    private static final long TABLE_ID = 41L;
    /** 聚簇主键索引。 */
    private static final long PRIMARY_ID = 101L;
    /** 大小写不敏感 unique email 二级索引。 */
    private static final long EMAIL_ID = 102L;
    /** 普通 tenant 二级索引。 */
    private static final long TENANT_ID = 103L;
    /** 独立 GENERAL tablespace。 */
    private static final SpaceId DATA_SPACE = SpaceId.of(2041);
    /** 与生产默认一致的 16 KiB 页。 */
    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    /** DML、锁等待和提交共用的有界测试超时。 */
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    /** JUnit 为每个测试创建的独立目录，承载 redo、undo 和 GENERAL tablespace，结束后自动清理。 */
    @TempDir
    Path directory;

    /**
     * INSERT、改变两个二级 key 的 UPDATE、DELETE 必须同步维护全部索引；旧 entry 仅 delete-mark，
     * 新 entry 保持 live，DELETE 后新 entry 也被标记，物理删除留给 purge。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建包含聚簇、唯一 email 与普通 tenant 二级索引的真实物理表，确保测试经过生产组合根。</li>
     *     <li>提交 INSERT 并读取三棵树，确认完整行和两个紧凑 entry 同时发布。</li>
     *     <li>提交 key-changing UPDATE，确认两个旧 entry 被标记、两个新 entry 为 live。</li>
     *     <li>提交 DELETE，确认聚簇行及当前 secondary entry 均为 delete-marked，未提前执行 purge。</li>
     * </ol>
     */
    @Test
    @DisplayName("table DML maintains clustered and all secondary indexes")
    void maintainsAllIndexesAcrossInsertUpdateAndDelete() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            // 1. 使用 DDL facade 创建真实多索引 tablespace，并从同一 binding 构造 exact-version metadata。
            TableSetup table = createTable(engine, "table-dml.ibd");
            LogicalRecord before = row(1, "Alice@example.test", 7);
            LogicalRecord after = row(1, "Bob@example.test", 9);

            // 2. INSERT 的聚簇首 MTR 与两个 secondary MTR 全部成功后，三棵树都必须可读。
            Transaction insert = transaction(engine);
            DmlWriteResult inserted = engine.tableDmlService().insert(new TableInsertCommand(insert,
                    table.metadata(), before, Optional.empty(), TIMEOUT));
            commit(engine, insert);

            assertEquals(1, inserted.affectedRows());
            assertTrue(clustered(engine, table, 1).isPresent());
            assertLive(engine, table.email(), before);
            assertLive(engine, table.tenant(), before);

            // 3. UPDATE 固定 new-before-old 发布顺序；最终状态应为旧 entry marked、新 entry live。
            Transaction update = transaction(engine);
            DmlWriteResult updated = engine.tableDmlService().update(new TableUpdateCommand(update,
                    table.metadata(), primaryKey(1), after, TIMEOUT));
            commit(engine, update);

            assertEquals(1, updated.affectedRows());
            assertMarked(engine, table.email(), before);
            assertMarked(engine, table.tenant(), before);
            assertLive(engine, table.email(), after);
            assertLive(engine, table.tenant(), after);

            // 4. DELETE 只发布逻辑删除；聚簇和当前两个 secondary entry 均保留物理记录等待 purge。
            Transaction delete = transaction(engine);
            DmlWriteResult deleted = engine.tableDmlService().delete(new TableDeleteCommand(delete,
                    table.metadata(), primaryKey(1), TIMEOUT));
            commit(engine, delete);

            assertEquals(1, deleted.affectedRows());
            assertTrue(clusteredIncludingDeleted(engine, table, 1).orElseThrow().record().deleted());
            assertMarked(engine, table.email(), after);
            assertMarked(engine, table.tenant(), after);
        } finally {
            engine.close();
        }
    }

    /**
     * 两个事务以不同主键写入 collation 等价的 logical unique key 时，第二个必须等待第一个事务终态；锁释放后
     * including-deleted prefix 复核发现其它主键并返回 duplicate，不能双双越过检查。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    @DisplayName("logical unique key lock serializes concurrent inserts")
    void logicalUniqueKeyLockSerializesConcurrentInserts() throws Exception {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-concurrent.ibd");
            Transaction first = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(first, table.metadata(),
                    row(1, "Case@Example.Test", 7), Optional.empty(), TIMEOUT));
            Transaction second = transaction(engine);

            try (var executor = Executors.newSingleThreadExecutor()) {
                var waiting = executor.submit(() -> engine.tableDmlService().insert(new TableInsertCommand(
                        second, table.metadata(), row(2, "case@example.test", 8), Optional.empty(), TIMEOUT)));
                assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS),
                        "second insert must wait on the normalized logical unique-key lock");

                commit(engine, first);
                ExecutionException duplicate = assertThrows(ExecutionException.class,
                        () -> waiting.get(2, TimeUnit.SECONDS));
                assertTrue(duplicate.getCause() instanceof DmlDuplicateKeyException);
            }

            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(second));
            assertEquals(row(1, "Case@Example.Test", 7).columnValues(),
                    clustered(engine, table, 1).orElseThrow().record().columnValues());
            assertTrue(clustered(engine, table, 2).isEmpty());
        } finally {
            engine.close();
        }
    }

    /** NULL logical unique key 不取得 logical-key X 锁，只按“NULL + 聚簇主键后缀”检查完整物理 identity。 */
    @Test
    @DisplayName("nullable unique secondary allows multiple null physical identities")
    void nullableUniqueSecondaryAllowsMultipleNulls() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-null.ibd");
            LogicalRecord firstRow = rowWithEmail(1, ColumnValue.NullValue.INSTANCE, 7);
            LogicalRecord secondRow = rowWithEmail(2, ColumnValue.NullValue.INSTANCE, 8);
            Transaction first = transaction(engine);
            Transaction second = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    first, table.metadata(), firstRow, Optional.empty(), TIMEOUT));
            engine.tableDmlService().insert(new TableInsertCommand(
                    second, table.metadata(), secondRow, Optional.empty(), TIMEOUT));
            commit(engine, first);
            commit(engine, second);

            assertLive(engine, table.email(), firstRow);
            assertLive(engine, table.email(), secondRow);
        } finally {
            engine.close();
        }
    }

    /**
     * 已提交 DELETE 遗留的其它主键 marked entry 不再占用 logical unique key；新 INSERT 必须创建自己的物理
     * identity，而不是等待 purge 或复活旧主键 entry。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>提交主键 1 的 INSERT 与 DELETE，使聚簇行和 email entry 保持 delete-marked 物理状态。</li>
     *     <li>主键 2 插入相同 email；logical-key X 已证明删除事务终结，prefix scan 跳过其它主键 marked history。</li>
     *     <li>提交第二事务，确认旧 identity 仍 marked、新 identity live，两个物理候选可共存但仅一个当前有效。</li>
     * </ol>
     */
    @Test
    @DisplayName("committed delete-marked unique entry can be reused before purge")
    void committedDeleteMarkedUniqueEntryCanBeReusedBeforePurge() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            // 1. DELETE 只发布标记；在 purge 前，该 physical identity 仍是 persistent unique-prefix 候选。
            TableSetup table = createTable(engine, "table-unique-marked-conflict.ibd");
            LogicalRecord deletedRow = row(1, "retained@example.test", 7);
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    insert, table.metadata(), deletedRow, Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction delete = transaction(engine);
            engine.tableDmlService().delete(new TableDeleteCommand(
                    delete, table.metadata(), primaryKey(1), TIMEOUT));
            commit(engine, delete);
            assertMarked(engine, table.email(), deletedRow);

            // 2. 其它主键 marked 候选只是历史导航入口；新主键必须得到独立 ABSENT 物理前态。
            LogicalRecord replacement = row(2, "retained@example.test", 8);
            Transaction replacementInsert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    replacementInsert, table.metadata(), replacement, Optional.empty(), TIMEOUT));
            commit(engine, replacementInsert);

            // 3. purge 前允许同一 logical prefix 有多个 marked/live physical suffix，但绝不允许两个 live identity。
            assertMarked(engine, table.email(), deletedRow);
            assertLive(engine, table.email(), replacement);
            assertTrue(clusteredIncludingDeleted(engine, table, 1).orElseThrow().record().deleted());
            assertEquals(replacement.columnValues(),
                    clustered(engine, table, 2).orElseThrow().record().columnValues());
        } finally {
            engine.close();
        }
    }

    /**
     * 删除事务尚未终结时，新的同键 INSERT 必须等待 logical-key X；删除提交后等待者重扫当前 prefix，跳过 marked
     * history 并成功发布，不能在锁授予前读取中间状态。
     *
     * @throws Exception executor future 或有界等待报告异常时抛出，由测试框架保留原始 cause
     */
    @Test
    @DisplayName("unique insert waits for delete commit before reusing marked key")
    void uniqueInsertWaitsForDeleteCommitBeforeReusingMarkedKey() throws Exception {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-delete-commit-wait.ibd");
            LogicalRecord deletedRow = row(1, "commit-wait@example.test", 7);
            Transaction initial = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    initial, table.metadata(), deletedRow, Optional.empty(), TIMEOUT));
            commit(engine, initial);

            Transaction deleting = transaction(engine);
            engine.tableDmlService().delete(new TableDeleteCommand(
                    deleting, table.metadata(), primaryKey(1), TIMEOUT));
            LogicalRecord replacement = row(2, "commit-wait@example.test", 8);
            Transaction waitingTxn = transaction(engine);

            try (var executor = Executors.newSingleThreadExecutor()) {
                var waiting = executor.submit(() -> engine.tableDmlService().insert(new TableInsertCommand(
                        waitingTxn, table.metadata(), replacement, Optional.empty(), TIMEOUT)));
                assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS),
                        "waiter must not inspect the uncommitted delete-marked entry");

                commit(engine, deleting);
                assertEquals(1, waiting.get(2, TimeUnit.SECONDS).affectedRows());
            }
            commit(engine, waitingTxn);

            assertMarked(engine, table.email(), deletedRow);
            assertLive(engine, table.email(), replacement);
        } finally {
            engine.close();
        }
    }

    /**
     * 删除事务回滚时必须先通过 undo revive 原 entry，再释放 logical-key X；等待 INSERT 醒来后重扫看到 live 候选并
     * 报 duplicate，不能依据等待前观察到的 marked 位错误复用键值。
     *
     * @throws Exception executor future 或有界等待报告异常时抛出，由测试框架保留原始 cause
     */
    @Test
    @DisplayName("unique insert sees live conflict after delete rollback")
    void uniqueInsertSeesLiveConflictAfterDeleteRollback() throws Exception {
        MutableTableResolver resolver = new MutableTableResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-delete-rollback-wait.ibd");
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            LogicalRecord retained = row(1, "rollback-wait@example.test", 7);
            Transaction initial = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    initial, table.metadata(), retained, Optional.empty(), TIMEOUT));
            commit(engine, initial);

            Transaction deleting = transaction(engine);
            engine.tableDmlService().delete(new TableDeleteCommand(
                    deleting, table.metadata(), primaryKey(1), TIMEOUT));
            LogicalRecord rejected = row(2, "rollback-wait@example.test", 8);
            Transaction waitingTxn = transaction(engine);

            try (var executor = Executors.newSingleThreadExecutor()) {
                var waiting = executor.submit(() -> engine.tableDmlService().insert(new TableInsertCommand(
                        waitingTxn, table.metadata(), rejected, Optional.empty(), TIMEOUT)));
                assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS),
                        "waiter must remain behind the deleting transaction logical-key lock");

                engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(deleting));
                ExecutionException duplicate = assertThrows(ExecutionException.class,
                        () -> waiting.get(2, TimeUnit.SECONDS));
                assertTrue(duplicate.getCause() instanceof DmlDuplicateKeyException);
            }
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(waitingTxn));

            assertLive(engine, table.email(), retained);
            assertTrue(clustered(engine, table, 2).isEmpty());
            assertAbsent(engine, table.email(), rejected);
        } finally {
            engine.close();
        }
    }

    /**
     * PREPARED 删除事务必须继续持有 logical-key X；只有 phase-two commit 的终态 redo durable 并释放锁后，等待
     * INSERT 才能重扫 marked 当前状态并成功，phase one 不能提前暴露可复用键值。
     *
     * @throws Exception executor future 或有界等待报告异常时抛出，由测试框架保留原始 cause
     */
    @Test
    @DisplayName("prepared delete commit releases unique key only after phase two")
    void preparedDeleteCommitReleasesUniqueKeyOnlyAfterPhaseTwo() throws Exception {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-prepared-commit.ibd");
            LogicalRecord deletedRow = row(1, "prepared-commit@example.test", 7);
            Transaction initial = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    initial, table.metadata(), deletedRow, Optional.empty(), TIMEOUT));
            commit(engine, initial);

            Transaction preparedDelete = transaction(engine);
            engine.tableDmlService().delete(new TableDeleteCommand(
                    preparedDelete, table.metadata(), primaryKey(1), TIMEOUT));
            engine.preparedTransactionService().prepare(
                    new PrepareTransactionCommand(preparedDelete, TIMEOUT));
            LogicalRecord replacement = row(2, "prepared-commit@example.test", 8);
            Transaction waitingTxn = transaction(engine);

            try (var executor = Executors.newSingleThreadExecutor()) {
                var waiting = executor.submit(() -> engine.tableDmlService().insert(new TableInsertCommand(
                        waitingTxn, table.metadata(), replacement, Optional.empty(), TIMEOUT)));
                assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS),
                        "phase-one PREPARED must retain the logical unique-key lock");

                engine.preparedTransactionService().commitPrepared(
                        new CommitPreparedTransactionCommand(preparedDelete, TIMEOUT));
                assertEquals(1, waiting.get(2, TimeUnit.SECONDS).affectedRows());
            }
            commit(engine, waitingTxn);
            assertMarked(engine, table.email(), deletedRow);
            assertLive(engine, table.email(), replacement);
        } finally {
            engine.close();
        }
    }

    /**
     * PREPARED 删除回滚必须先恢复聚簇与全部 secondary entry，再持久化 phase-two rollback 并释放 logical-key X；
     * 等待 INSERT 醒来后只能看到恢复后的 live 冲突。
     *
     * @throws Exception executor future 或有界等待报告异常时抛出，由测试框架保留原始 cause
     */
    @Test
    @DisplayName("prepared delete rollback restores live unique conflict before unlock")
    void preparedDeleteRollbackRestoresLiveUniqueConflictBeforeUnlock() throws Exception {
        MutableTableResolver resolver = new MutableTableResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-prepared-rollback.ibd");
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            LogicalRecord retained = row(1, "prepared-rollback@example.test", 7);
            Transaction initial = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    initial, table.metadata(), retained, Optional.empty(), TIMEOUT));
            commit(engine, initial);

            Transaction preparedDelete = transaction(engine);
            engine.tableDmlService().delete(new TableDeleteCommand(
                    preparedDelete, table.metadata(), primaryKey(1), TIMEOUT));
            engine.preparedTransactionService().prepare(
                    new PrepareTransactionCommand(preparedDelete, TIMEOUT));
            LogicalRecord rejected = row(2, "prepared-rollback@example.test", 8);
            Transaction waitingTxn = transaction(engine);

            try (var executor = Executors.newSingleThreadExecutor()) {
                var waiting = executor.submit(() -> engine.tableDmlService().insert(new TableInsertCommand(
                        waitingTxn, table.metadata(), rejected, Optional.empty(), TIMEOUT)));
                assertThrows(TimeoutException.class, () -> waiting.get(100, TimeUnit.MILLISECONDS),
                        "phase-one PREPARED must retain the logical unique-key lock");

                engine.preparedTransactionService().rollbackPrepared(
                        new ResolvedRollbackPreparedTransactionCommand(preparedDelete, TIMEOUT));
                ExecutionException duplicate = assertThrows(ExecutionException.class,
                        () -> waiting.get(2, TimeUnit.SECONDS));
                assertTrue(duplicate.getCause() instanceof DmlDuplicateKeyException);
            }
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(waitingTxn));
            assertLive(engine, table.email(), retained);
            assertTrue(clustered(engine, table, 2).isEmpty());
        } finally {
            engine.close();
        }
    }

    /** 同一事务删除旧主键后可重入 logical-key X，并用新主键发布同一 unique key。 */
    @Test
    @DisplayName("same transaction reuses unique key with a new primary key")
    void sameTransactionReusesUniqueKeyWithNewPrimaryKey() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-same-transaction-reuse.ibd");
            LogicalRecord oldRow = row(1, "same-transaction@example.test", 7);
            Transaction initial = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    initial, table.metadata(), oldRow, Optional.empty(), TIMEOUT));
            commit(engine, initial);

            Transaction replacing = transaction(engine);
            engine.tableDmlService().delete(new TableDeleteCommand(
                    replacing, table.metadata(), primaryKey(1), TIMEOUT));
            LogicalRecord newRow = row(2, "same-transaction@example.test", 8);
            engine.tableDmlService().insert(new TableInsertCommand(
                    replacing, table.metadata(), newRow, Optional.empty(), TIMEOUT));
            commit(engine, replacing);

            assertMarked(engine, table.email(), oldRow);
            assertLive(engine, table.email(), newRow);
        } finally {
            engine.close();
        }
    }

    /**
     * logical prefix 中可以积累多个已提交 marked history；检查必须越过前两个候选发现后续 live 冲突，并允许全 marked
     * prefix 创建新的物理 suffix，防止旧的 limit=2 产生双 live unique key。
     */
    @Test
    @DisplayName("unique check scans beyond multiple marked histories")
    void uniqueCheckScansBeyondMultipleMarkedHistories() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-many-marked.ibd");
            String email = "many-marked@example.test";
            for (int id = 1; id <= 3; id++) {
                LogicalRecord historical = row(id, email, id);
                Transaction insert = transaction(engine);
                engine.tableDmlService().insert(new TableInsertCommand(
                        insert, table.metadata(), historical, Optional.empty(), TIMEOUT));
                commit(engine, insert);
                Transaction delete = transaction(engine);
                engine.tableDmlService().delete(new TableDeleteCommand(
                        delete, table.metadata(), primaryKey(id), TIMEOUT));
                commit(engine, delete);
                assertMarked(engine, table.email(), historical);
            }

            LogicalRecord live = row(4, email, 4);
            Transaction accepted = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    accepted, table.metadata(), live, Optional.empty(), TIMEOUT));
            commit(engine, accepted);

            Transaction rejected = transaction(engine);
            assertThrows(DmlDuplicateKeyException.class, () -> engine.tableDmlService().insert(
                    new TableInsertCommand(rejected, table.metadata(), row(5, email, 5), Optional.empty(), TIMEOUT)));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(rejected));
            assertLive(engine, table.email(), live);
        } finally {
            engine.close();
        }
    }

    /**
     * 候选扫描达到测试上限加一且尚未发现 live entry 时，服务必须抛容量异常；已读取的 marked 前缀不能被误当作
     * 完整唯一性证明，异常后事务没有聚簇或二级页写。
     */
    @Test
    @DisplayName("unique check fails closed when marked candidate capacity is exceeded")
    void uniqueCheckFailsClosedWhenMarkedCandidateCapacityIsExceeded() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-unique-capacity.ibd");
            String email = "capacity@example.test";
            for (int id = 1; id <= 3; id++) {
                LogicalRecord historical = row(id, email, id);
                Transaction insert = transaction(engine);
                engine.tableDmlService().insert(new TableInsertCommand(
                        insert, table.metadata(), historical, Optional.empty(), TIMEOUT));
                commit(engine, insert);
                Transaction delete = transaction(engine);
                engine.tableDmlService().delete(new TableDeleteCommand(
                        delete, table.metadata(), primaryKey(id), TIMEOUT));
                commit(engine, delete);
            }

            SecondaryUniqueCheckService bounded = new SecondaryUniqueCheckService(
                    engine.miniTransactionManager(), engine.btreeService(), engine.lockManager(),
                    engine.typeCodecRegistry(), 2);
            Transaction probe = transaction(engine);
            var owner = engine.transactionManager().assignWriteId(probe);
            LogicalRecord target = row(4, email, 4);

            assertThrows(SecondaryUniqueCheckCapacityException.class, () -> bounded.check(
                    table.email(), target,
                    new BTreeCurrentReadRequest(owner, probe.isolationLevel(), TIMEOUT, 8)));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(probe));

            assertTrue(clusteredIncludingDeleted(engine, table, 4).isEmpty());
            assertAbsent(engine, table.email(), target);
        } finally {
            engine.close();
        }
    }

    /**
     * 同一主键执行 A→B→A 时，第二次 UPDATE 应复活第一次更新留下的 A marked identity，再标记 B；这样既不制造
     * 重复 physical key，也保留 undo 所需的准确发布前态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>提交 email=A 的初始行，再提交 A→B，使 A entry marked、B entry live。</li>
     *     <li>提交 B→A；unique-prefix 检查识别同一完整主键后缀的 marked A，并把发布动作冻结为 revive。</li>
     *     <li>验证 A 恢复 live、B 变为 marked，聚簇行内容与回转后的完整行一致。</li>
     * </ol>
     */
    @Test
    @DisplayName("same primary key revives its delete-marked unique entry")
    void samePrimaryKeyRevivesDeleteMarkedUniqueEntry() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            // 1. 首次变键留下可由同一 physical identity 复活的 A entry。
            TableSetup table = createTable(engine, "table-unique-revive.ibd");
            LogicalRecord rowA = row(1, "a@example.test", 7);
            LogicalRecord rowB = row(1, "b@example.test", 8);
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    insert, table.metadata(), rowA, Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction toB = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(
                    toB, table.metadata(), primaryKey(1), rowB, TIMEOUT));
            commit(engine, toB);
            assertMarked(engine, table.email(), rowA);
            assertLive(engine, table.email(), rowB);

            // 2. 回转到 A 时，UPDATE 允许消费 DELETE_MARKED 前态并执行 header revive，而不是重复 insert。
            Transaction backToA = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(
                    backToA, table.metadata(), primaryKey(1), rowA, TIMEOUT));
            commit(engine, backToA);

            // 3. 最终仅 A 对当前版本可见，B 仍保留 marked 物理记录等待其 history 到达 purge horizon。
            assertLive(engine, table.email(), rowA);
            assertMarked(engine, table.email(), rowB);
            assertEquals(rowA.columnValues(), clustered(engine, table, 1).orElseThrow().record().columnValues());
        } finally {
            engine.close();
        }
    }

    /**
     * 初次 unique 检查识别到同一物理 identity marked 后，purge 可以在 UPDATE 尚未取得 row guard 时删除该 entry。
     * UPDATE 取得 guard 后必须把前态从 DELETE_MARKED 复核为 ABSENT；rollback 随后应删除新插入的 A，而不是尝试
     * 对已被 purge 的历史 identity 恢复 delete mark。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>提交 A→B，留下 marked A、live B 和可 purge 的 UPDATE history。</li>
     *     <li>在 B→A 完成初次 unique scan 后暂停；前台线程尚未取得 row guard，也没有写聚簇/secondary 页。</li>
     *     <li>真实 PurgeCoordinator 删除 marked A 并推进 history，再恢复 UPDATE，让 row guard 内 exact recheck 选择 ABSENT。</li>
     *     <li>回滚 B→A，确认新插入 A 被物理删除、B 恢复 live，证明 undo before-state 使用了复核结果。</li>
     * </ol>
     *
     * @throws Exception executor、latch 或 purge 有界等待失败时抛出，由测试框架保留原始 cause
     */
    @Test
    @DisplayName("update rechecks marked publish state after concurrent purge")
    void updateRechecksMarkedPublishStateAfterConcurrentPurge() throws Exception {
        MutableTableResolver resolver = new MutableTableResolver();
        StorageEngine engine = new StorageEngine(foregroundOnlyConfig());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            // 1、只改变 unique email，确保 history 中恰有一个可物理删除的 secondary old-key task。
            TableSetup table = createTable(engine, "table-unique-purge-window.ibd");
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            LogicalRecord rowA = row(1, "purge-window-a@example.test", 7);
            LogicalRecord rowB = row(1, "purge-window-b@example.test", 7);
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(
                    insert, table.metadata(), rowA, Optional.empty(), TIMEOUT));
            commit(engine, insert);
            Transaction toB = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(
                    toB, table.metadata(), primaryKey(1), rowB, TIMEOUT));
            commit(engine, toB);
            assertMarked(engine, table.email(), rowA);

            // 2、测试 hook 只阻塞无 page/row-guard 资源的稳定边界；事务 record/logical locks 仍按生产语义持有。
            CountDownLatch uniqueChecked = new CountDownLatch(1);
            CountDownLatch allowRowGuard = new CountDownLatch(1);
            engine.tableDmlService().installFaultInjectorForTest((phase, operation, indexId) -> {
                if (phase == TableDmlProgressPhase.AFTER_UNIQUE_CHECK_BEFORE_ROW_GUARD
                        && operation == TableDmlSecondaryOperation.INSERT_OR_REVIVE
                        && indexId == EMAIL_ID) {
                    uniqueChecked.countDown();
                    try {
                        if (!allowRowGuard.await(2, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to resume row-guard acquisition");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("unique-check test hook was interrupted", interrupted);
                    }
                }
            });

            Transaction backToA = transaction(engine);
            try (var executor = Executors.newSingleThreadExecutor()) {
                var updating = executor.submit(() -> engine.tableDmlService().update(new TableUpdateCommand(
                        backToA, table.metadata(), primaryKey(1), rowA, TIMEOUT)));
                try {
                    assertTrue(uniqueChecked.await(2, TimeUnit.SECONDS),
                            "B→A update must pause after seeing the marked A identity");

                    // 3、purge 不进入事务逻辑锁目录；UPDATE 尚未取得 row guard，因此旧 A 可以在该窗口被删除。
                    var summary = engine.purgeCoordinator().runBatch(1);
                    assertEquals(1, summary.purgedLogs());
                    assertEquals(1, summary.removedSecondaryEntries());
                    assertAbsent(engine, table.email(), rowA);
                } finally {
                    allowRowGuard.countDown();
                }
                assertEquals(1, updating.get(2, TimeUnit.SECONDS).affectedRows());
            } finally {
                allowRowGuard.countDown();
                engine.tableDmlService().installFaultInjectorForTest(TableDmlProgressFaultInjector.NO_OP);
            }

            // 4、复核后写入 undo 的 before-state 必须是 ABSENT；rollback 删除新 A 并恢复 B。
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(backToA));
            assertAbsent(engine, table.email(), rowA);
            assertLive(engine, table.email(), rowB);
            assertEquals(rowB.columnValues(),
                    clustered(engine, table, 1).orElseThrow().record().columnValues());
        } finally {
            engine.close();
        }
    }

    /** 第一棵 secondary MTR 已提交后故障，statement rollback 必须把已发布 entry 与聚簇 anchor 一并撤销。 */
    @Test
    @DisplayName("statement rollback converges after a secondary commit")
    void statementRollbackConvergesAfterSecondaryCommit() {
        assertStatementRollbackAtSecondaryBoundary(TableDmlProgressPhase.AFTER_COMMIT, EMAIL_ID);
    }

    /** 第一棵 secondary 已提交、第二棵尚未开始时故障，rollback 对未发布 mutation 也必须按 ABSENT 幂等收敛。 */
    @Test
    @DisplayName("statement rollback converges before the next secondary MTR")
    void statementRollbackConvergesBeforeNextSecondaryMtr() {
        assertStatementRollbackAtSecondaryBoundary(TableDmlProgressPhase.BEFORE_MTR, TENANT_ID);
    }

    /** 执行一个 secondary 边界故障场景并验证三棵树、事务状态与 undo marker 同时收敛。 */
    private void assertStatementRollbackAtSecondaryBoundary(TableDmlProgressPhase phase, long indexId) {
        MutableTableResolver resolver = new MutableTableResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-secondary-fault-" + phase + ".ibd");
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            LogicalRecord row = row(1, "fault@example.test", 7);
            Transaction transaction = transaction(engine);
            engine.tableDmlService().installFaultInjectorForTest((currentPhase, operation, currentIndexId) -> {
                if (currentPhase == phase && currentIndexId == indexId) {
                    throw new DmlOperationException("synthetic secondary boundary failure");
                }
            });

            try (DmlStatementGuard statement = engine.dmlService().beginStatement(
                    transaction, table.metadata().clusteredIndex())) {
                assertThrows(DmlOperationException.class, () -> engine.tableDmlService().insert(
                        new TableInsertCommand(transaction, table.metadata(), row, Optional.empty(), TIMEOUT)));
                assertEquals(1, statement.rollback().undoRecordsApplied());
            } finally {
                engine.tableDmlService().installFaultInjectorForTest(TableDmlProgressFaultInjector.NO_OP);
            }

            assertEquals(TransactionState.ACTIVE, transaction.state());
            assertTrue(clusteredIncludingDeleted(engine, table, 1).isEmpty());
            assertAbsent(engine, table.email(), row);
            assertAbsent(engine, table.tenant(), row);
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(transaction));
        } finally {
            engine.close();
        }
    }

    /**
     * secondary insert 触发 split 时 redo admission 必须使用刷新后的结构预算。旧实现固定 point-rewrite 预算，
     * 在首个 split 分支会低估并中断；测试持续插入直到 root level 上升，并断言全部 entry 可读。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建真实多索引表，初始 secondary root 为 level 0。</li>
     *     <li>逐事务插入足量长 email entry，迫使 email secondary 执行 leaf split 与 root grow。</li>
     *     <li>直接读取 root 页头确认结构层级上升，再逐项按完整物理 key 验证 entry 未丢失。</li>
     * </ol>
     */
    @Test
    @DisplayName("secondary split uses structural redo budget")
    void secondarySplitUsesStructuralRedoBudget() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        Throwable primaryFailure = null;
        try {
            // 1. 物理 DDL 提供稳定 root/segment；后续结构变化只读取页头权威 level。
            TableSetup table = createTable(engine, "table-dml-split.ibd");
            int rows = 240;
            // 2. 每行独立提交，确保下一语句必须处理前一次 split 后的 root level，而非复用单个 MTR 快照。
            for (int id = 1; id <= rows; id++) {
                LogicalRecord row = row(id, "account-" + id + "-" + "x".repeat(72), id % 11);
                Transaction transaction = transaction(engine);
                engine.tableDmlService().insert(new TableInsertCommand(transaction,
                        table.metadata(), row, Optional.empty(), TIMEOUT));
                commit(engine, transaction);
            }

            // 3. 页头必须证明测试走过 split；随后完整读取所有 entry，防止仅以“未抛异常”误判成功。
            assertTrue(refreshRootLevel(engine, table.email()) > 0,
                    "测试必须真实覆盖 secondary root split，而不是只跑 leaf happy path");
            for (int id = 1; id <= rows; id++) {
                assertLive(engine, table.email(),
                        row(id, "account-" + id + "-" + "x".repeat(72), id % 11));
            }
        } catch (RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                engine.close();
            } catch (RuntimeException closeFailure) {
                if (primaryFailure == null) {
                    throw closeFailure;
                }
                primaryFailure.addSuppressed(closeFailure);
            }
        }
    }

    /**
     * 未提交 INSERT 的完整回滚必须先移除全部二级 entry，再删除聚簇行；最终三棵树都不存在该 physical identity。
     */
    @Test
    @DisplayName("table INSERT rollback removes clustered and all secondary entries")
    void rollsBackInsertAcrossAllIndexes() {
        MutableTableResolver resolver = new MutableTableResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-insert-rollback.ibd");
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            LogicalRecord insertedRow = row(1, "insert@example.test", 7);

            Transaction transaction = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(transaction,
                    table.metadata(), insertedRow, Optional.empty(), TIMEOUT));
            DmlRollbackResult rolledBack = engine.tableDmlService().rollback(
                    new ResolvedDmlRollbackCommand(transaction));

            assertEquals(1, rolledBack.rollbackSummary().undoRecordsApplied());
            assertTrue(clusteredIncludingDeleted(engine, table, 1).isEmpty());
            assertAbsent(engine, table.email(), insertedRow);
            assertAbsent(engine, table.tenant(), insertedRow);
        } finally {
            engine.close();
        }
    }

    /**
     * 未提交 key-changing UPDATE 的完整回滚必须 revive 两个旧 entry、删除两个新 entry，并恢复聚簇旧 image。
     */
    @Test
    @DisplayName("table UPDATE rollback restores old clustered row and secondary keys")
    void rollsBackUpdateAcrossAllIndexes() {
        MutableTableResolver resolver = new MutableTableResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-update-rollback.ibd");
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            LogicalRecord before = row(1, "before@example.test", 7);
            LogicalRecord after = row(1, "after@example.test", 9);
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(insert,
                    table.metadata(), before, Optional.empty(), TIMEOUT));
            commit(engine, insert);

            Transaction update = transaction(engine);
            engine.tableDmlService().update(new TableUpdateCommand(update,
                    table.metadata(), primaryKey(1), after, TIMEOUT));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(update));

            assertEquals(before.columnValues(), clustered(engine, table, 1).orElseThrow().record().columnValues());
            assertLive(engine, table.email(), before);
            assertLive(engine, table.tenant(), before);
            assertAbsent(engine, table.email(), after);
            assertAbsent(engine, table.tenant(), after);
        } finally {
            engine.close();
        }
    }

    /**
     * 未提交 DELETE 的完整回滚必须同时取消聚簇和全部二级 delete mark，不得等待 purge 重建当前索引入口。
     */
    @Test
    @DisplayName("table DELETE rollback revives clustered and all secondary entries")
    void rollsBackDeleteAcrossAllIndexes() {
        MutableTableResolver resolver = new MutableTableResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-delete-rollback.ibd");
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            LogicalRecord row = row(1, "delete@example.test", 7);
            Transaction insert = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(insert,
                    table.metadata(), row, Optional.empty(), TIMEOUT));
            commit(engine, insert);

            Transaction delete = transaction(engine);
            engine.tableDmlService().delete(new TableDeleteCommand(delete,
                    table.metadata(), primaryKey(1), TIMEOUT));
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(delete));

            assertFalse(clustered(engine, table, 1).orElseThrow().record().deleted());
            assertLive(engine, table.email(), row);
            assertLive(engine, table.tenant(), row);
        } finally {
            engine.close();
        }
    }

    /**
     * statement/savepoint rollback 必须复用表级 inverse，只撤销边界后的多索引写并保持事务 ACTIVE；边界前已发布的
     * 聚簇与二级 entry 仍属于当前事务，不能被 full-rollback 语义误删。
     */
    @Test
    @DisplayName("statement rollback removes only later multi-index writes")
    void statementRollbackRestoresAllIndexesToSavepoint() {
        MutableTableResolver resolver = new MutableTableResolver();
        StorageEngine engine = new StorageEngine(config());
        engine.configureIndexMetadataResolver(resolver);
        engine.open();
        try {
            TableSetup table = createTable(engine, "table-statement-rollback.ibd");
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            LogicalRecord beforeBoundary = row(1, "before-boundary@example.test", 7);
            LogicalRecord afterBoundary = row(2, "after-boundary@example.test", 9);
            Transaction transaction = transaction(engine);
            engine.tableDmlService().insert(new TableInsertCommand(transaction,
                    table.metadata(), beforeBoundary, Optional.empty(), TIMEOUT));

            try (DmlStatementGuard statement = engine.dmlService().beginStatement(
                    transaction, table.metadata().clusteredIndex())) {
                engine.tableDmlService().insert(new TableInsertCommand(transaction,
                        table.metadata(), afterBoundary, Optional.empty(), TIMEOUT));
                assertEquals(1, statement.rollback().undoRecordsApplied());
            }

            assertEquals(TransactionState.ACTIVE, transaction.state());
            assertTrue(clustered(engine, table, 1).isPresent());
            assertLive(engine, table.email(), beforeBoundary);
            assertLive(engine, table.tenant(), beforeBoundary);
            assertTrue(clusteredIncludingDeleted(engine, table, 2).isEmpty());
            assertAbsent(engine, table.email(), afterBoundary);
            assertAbsent(engine, table.tenant(), afterBoundary);
            engine.tableDmlService().rollback(new ResolvedDmlRollbackCommand(transaction));
        } finally {
            engine.close();
        }
    }

    /**
     * existing-open 的 UNDO_ROLLBACK 阶段必须通过 exact-version target resolver 重放同一套多索引 inverse；测试故意
     * 在 active UPDATE 后正常刷盘关闭，以稳定构造“数据与 active undo 都 durable、事务未终结”的恢复输入。
     */
    @Test
    @DisplayName("recovery rollback restores clustered and all secondary indexes")
    void recoveryRollbackRestoresAllIndexes() {
        String fileName = "table-recovery-rollback.ibd";
        EngineConfig recoveryConfig = config().withRecoveryTablespaces(List.of(
                new EngineTablespaceConfig(DATA_SPACE, directory.resolve(fileName))));
        MutableTableResolver resolver = new MutableTableResolver();
        TableSetup table;
        LogicalRecord before = row(1, "recovery-before@example.test", 7);
        LogicalRecord after = row(1, "recovery-after@example.test", 9);

        StorageEngine first = new StorageEngine(recoveryConfig);
        first.configureIndexMetadataResolver(resolver);
        first.open();
        try {
            table = createTable(first, fileName);
            resolver.install(new UndoTargetMetadata(table.metadata(), Optional.empty()));
            Transaction insert = transaction(first);
            first.tableDmlService().insert(new TableInsertCommand(insert,
                    table.metadata(), before, Optional.empty(), TIMEOUT));
            commit(first, insert);

            Transaction activeUpdate = transaction(first);
            first.tableDmlService().update(new TableUpdateCommand(activeUpdate,
                    table.metadata(), primaryKey(1), after, TIMEOUT));
        } finally {
            first.close();
        }

        StorageEngine recovered = new StorageEngine(recoveryConfig);
        recovered.configureIndexMetadataResolver(resolver);
        recovered.open();
        try {
            assertEquals(before.columnValues(),
                    clustered(recovered, table, 1).orElseThrow().record().columnValues());
            assertLive(recovered, table.email(), before);
            assertLive(recovered, table.tenant(), before);
            assertAbsent(recovered, table.email(), after);
            assertAbsent(recovered, table.tenant(), after);
        } finally {
            recovered.close();
        }
    }

    /**
     * 创建含一聚簇、一个 logical unique 和一个普通 secondary 的真实物理表。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>构造稳定 storage DDL DTO，声明连续列 ordinal、索引 id 和 UTF8 大小写不敏感 email key。</li>
     *     <li>调用物理 DDL facade 创建 tablespace、segment 与三个 root，返回 durable binding。</li>
     *     <li>从同一 definition/binding 映射表级运行期 metadata，禁止测试手工拼接错版本 descriptor。</li>
     * </ol>
     *
     * @param engine   已打开且拥有本测试独立目录的存储引擎。
     * @param fileName 当前测试的数据表空间文件名，必须位于 JUnit 临时目录内。
     * @return 同一 schema version 的聚簇、email secondary 与 tenant secondary 元数据聚合。
     * @throws cn.zhangyis.db.common.exception.DatabaseRuntimeException 物理 DDL、flush 或 metadata 映射失败时抛出。
     */
    private TableSetup createTable(StorageEngine engine, String fileName) {
        // 1. DTO 只携带稳定 identity/type，不泄露 record page 或 BufferFrame。
        StorageTableDefinition definition = new StorageTableDefinition(TABLE_ID, DATA_SPACE,
                directory.resolve(fileName), 1, PageNo.of(128),
                List.of(new StorageColumnDefinition(1, "id", 0,
                                StorageColumnType.bigint(false, false)),
                        new StorageColumnDefinition(2, "email", 1,
                                new StorageColumnType(StorageColumnTypeId.VARCHAR, true,
                                        160, 0, false, 1, 2, List.of())),
                        new StorageColumnDefinition(3, "tenant_id", 2,
                                StorageColumnType.bigint(false, false))),
                List.of(new StorageIndexDefinition(PRIMARY_ID, "PRIMARY", true, true,
                                List.of(new StorageIndexKeyPart(1, StorageIndexOrder.ASC, 0))),
                        new StorageIndexDefinition(EMAIL_ID, "uq_email", true, false,
                                List.of(new StorageIndexKeyPart(2, StorageIndexOrder.ASC, 0))),
                        new StorageIndexDefinition(TENANT_ID, "idx_tenant", false, false,
                                List.of(new StorageIndexKeyPart(3, StorageIndexOrder.ASC, 0)))));
        // 2. 真实 DDL 创建三组 segment/root，并在返回前完成 WAL-safe flush。
        TableStorageBinding binding = engine.tableDdlStorageService().createTable(definition);
        // 3. 工厂从同一不可变输入构造 compact layout；索引顺序由 TableIndexMetadata 校验。
        TableIndexMetadata metadata = new BTreeIndexMetadataFactory().createTable(definition, binding);
        return new TableSetup(metadata, metadata.secondaryIndexes().get(0), metadata.secondaryIndexes().get(1));
    }

    /**
     * 使用公开事务入口创建普通可写事务。
     *
     * @param engine 已打开的存储引擎，其 TransactionSystem 是本测试唯一事务 id/no 权威来源。
     * @return 尚未分配 write id、状态为 ACTIVE 的普通事务。
     */
    private static Transaction transaction(StorageEngine engine) {
        return engine.transactionManager().begin(TransactionOptions.defaults());
    }

    /**
     * 提交表级 DML 事务；提交 durable 后 unique/record locks 才释放。
     *
     * @param engine      提供表级 DML facade、redo durability 与锁目录的存储引擎。
     * @param transaction 已完成当前测试写入且仍为 ACTIVE 的事务。
     * @throws DmlOperationException undo finalization、事务状态发布或 redo durability 失败时抛出。
     */
    private static void commit(StorageEngine engine, Transaction transaction) {
        engine.tableDmlService().commit(new DmlCommitCommand(transaction,
                DurabilityPolicy.FLUSH_ON_COMMIT, TIMEOUT));
    }

    /**
     * 读取 live 聚簇记录；普通 lookup 会过滤 delete-marked 行。
     *
     * @param engine 已打开的存储引擎。
     * @param table  持有目标聚簇 descriptor 的 exact-version 表快照。
     * @param id     完整聚簇主键值。
     * @return live 行的完全物化结果；不存在或已 delete-marked 时为空。
     */
    private static Optional<BTreeLookupResult> clustered(StorageEngine engine, TableSetup table, long id) {
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> result = engine.btreeService().lookup(read,
                    table.metadata().clusteredIndex(), primaryKey(id));
            engine.miniTransactionManager().commit(read);
            return result;
        } catch (RuntimeException error) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw error;
        }
    }

    /**
     * 读取包含 delete-marked 的聚簇物理记录，用于验证 DELETE 只做逻辑标记。
     *
     * @param engine 已打开的存储引擎。
     * @param table  持有目标聚簇 descriptor 的 exact-version 表快照。
     * @param id     完整聚簇主键值。
     * @return 对应物理记录；物理不存在时为空。
     */
    private static Optional<BTreeLookupResult> clusteredIncludingDeleted(StorageEngine engine,
                                                                          TableSetup table, long id) {
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> result = engine.btreeService().lookupIncludingDeleted(read,
                    table.metadata().clusteredIndex(), primaryKey(id));
            engine.miniTransactionManager().commit(read);
            return result;
        } catch (RuntimeException error) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw error;
        }
    }

    /**
     * 精确读取一个 compact secondary physical entry，并保留 delete-marked 状态。
     *
     * @param engine    已打开的存储引擎。
     * @param secondary 目标 secondary descriptor/layout。
     * @param row       用于投影 logical key 与完整聚簇主键后缀的完整表行。
     * @return 完整物理 key 对应的紧凑 entry；不存在时为空。
     */
    private static Optional<BTreeLookupResult> secondaryIncludingDeleted(StorageEngine engine,
                                                                          SecondaryIndexMetadata secondary,
                                                                          LogicalRecord row) {
        LogicalRecord entry = secondary.layout().toEntry(row, false);
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            Optional<BTreeLookupResult> result = engine.btreeService().lookupIncludingDeleted(read,
                    secondary.index(), secondary.layout().physicalKey(entry));
            engine.miniTransactionManager().commit(read);
            return result;
        } catch (RuntimeException error) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw error;
        }
    }

    /**
     * 断言 secondary entry 存在且未被标记。
     *
     * @param engine    已打开的存储引擎。
     * @param secondary 目标 secondary metadata。
     * @param row       应映射到目标完整物理 entry 的表行。
     */
    private static void assertLive(StorageEngine engine, SecondaryIndexMetadata secondary, LogicalRecord row) {
        assertFalse(secondaryIncludingDeleted(engine, secondary, row).orElseThrow().record().deleted());
    }

    /**
     * 断言 secondary entry 存在且 delete-marked。
     *
     * @param engine    已打开的存储引擎。
     * @param secondary 目标 secondary metadata。
     * @param row       应映射到目标完整物理 entry 的表行。
     */
    private static void assertMarked(StorageEngine engine, SecondaryIndexMetadata secondary, LogicalRecord row) {
        assertTrue(secondaryIncludingDeleted(engine, secondary, row).orElseThrow().record().deleted());
    }

    /**
     * 断言完整 secondary physical identity 已不存在，普通 live 过滤不能替代该检查。
     *
     * @param engine    已打开的存储引擎。
     * @param secondary 目标二级 exact-version metadata。
     * @param row       用于投影完整 physical key 的表行。
     */
    private static void assertAbsent(StorageEngine engine, SecondaryIndexMetadata secondary, LogicalRecord row) {
        assertTrue(secondaryIncludingDeleted(engine, secondary, row).isEmpty());
    }

    /**
     * 读取 root 页头的权威结构 level；descriptor 中的创建时 level 不能作为断言来源。
     *
     * @param engine    已打开的存储引擎。
     * @param secondary 目标 secondary metadata，其 root page id 在树生命周期内稳定。
     * @return 当前 root page header 中的非负 level。
     */
    private static int refreshRootLevel(StorageEngine engine, SecondaryIndexMetadata secondary) {
        MiniTransaction read = engine.miniTransactionManager().beginReadOnly();
        try {
            int level = engine.indexPageAccess().openIndexPage(read,
                    secondary.index().rootPageId(), cn.zhangyis.db.storage.buf.PageLatchMode.SHARED)
                    .header().level();
            engine.miniTransactionManager().commit(read);
            return level;
        } catch (RuntimeException error) {
            engine.miniTransactionManager().rollbackUncommitted(read);
            throw error;
        }
    }

    /**
     * 构造完整表行；二级 layout 从该行投影紧凑 entry。
     *
     * @param id       聚簇主键。
     * @param email    nullable email 列的本测试非空值。
     * @param tenantId 普通 secondary key。
     * @return schema version 1、无隐藏列的 conventional 输入行。
     */
    private static LogicalRecord row(long id, String email, long tenantId) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(email), new ColumnValue.IntValue(tenantId)),
                false, RecordType.CONVENTIONAL);
    }

    /** 构造允许 NULL email 的完整行，专用于 unique-NULL 物理 identity 测试。 */
    private static LogicalRecord rowWithEmail(long id, ColumnValue email, long tenantId) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), email,
                new ColumnValue.IntValue(tenantId)), false, RecordType.CONVENTIONAL);
    }

    /**
     * 构造完整聚簇主键 search key。
     *
     * @param id BIGINT 主键值。
     * @return 单 part ASC 搜索键。
     */
    private static SearchKey primaryKey(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    /**
     * 测试固定的 exact-version metadata 聚合。
     *
     * @param metadata 表级聚簇/二级索引快照。
     * @param email    logical unique email secondary。
     * @param tenant   普通 tenant secondary。
     */
    private record TableSetup(TableIndexMetadata metadata, SecondaryIndexMetadata email,
                              SecondaryIndexMetadata tenant) {
    }

    /**
     * 测试用可安装 exact-version target resolver；实例在引擎 open 前注入，物理 DDL 完成后再发布同一表快照。
     */
    private static final class MutableTableResolver
            implements IndexMetadataResolver, UndoTargetMetadataResolver {

        /** 当前测试唯一表的权威 undo target；volatile 保证恢复/回滚线程看到完整安装结果。 */
        private volatile UndoTargetMetadata target;

        /**
         * 发布由真实 DDL binding 构造的表级目标。
         *
         * @param target 同时携带全部索引 metadata 与可选 LOB segment 的 exact-version target。
         */
        private void install(UndoTargetMetadata target) {
            this.target = target;
        }

        /** {@inheritDoc} */
        @Override
        public cn.zhangyis.db.storage.btree.BTreeIndex resolve(long tableId, long indexId) {
            UndoTargetMetadata resolved = requireTarget(tableId);
            return resolved.tableIndexes().requireIndex(indexId);
        }

        /** {@inheritDoc} */
        @Override
        public UndoTargetMetadata resolveTarget(long tableId, long indexId) {
            UndoTargetMetadata resolved = requireTarget(tableId);
            if (resolved.clusteredIndex().indexId() != indexId) {
                throw new AssertionError("unexpected undo clustered index id: " + indexId);
            }
            return resolved;
        }

        /**
         * 校验测试 resolver 已安装且 table identity 与 undo 固定前缀一致。
         *
         * @param tableId undo record 固定前缀中的稳定表 id。
         * @return 当前测试安装的 exact-version target。
         */
        private UndoTargetMetadata requireTarget(long tableId) {
            UndoTargetMetadata resolved = target;
            if (resolved == null || resolved.tableIndexes().tableId() != tableId) {
                throw new AssertionError("unexpected undo table id: " + tableId);
            }
            return resolved;
        }
    }

    /**
     * 构造隔离的引擎目录和足够覆盖多索引 split 的 redo/buffer 配置。
     *
     * @return 使用当前 JUnit 临时目录、16 KiB 页和有界恢复参数的引擎配置。
     */
    private EngineConfig config() {
        return new EngineConfig(directory, PAGE_SIZE, 256, SpaceId.of(5), PageNo.of(64),
                64, 100, Duration.ofSeconds(30), 128L * 1024 * 1024);
    }

    /**
     * 构造关闭后台 worker 的确定性配置，使测试可以显式控制 committed history 的唯一一次 purge 时点。
     *
     * @return 与 {@link #config()} 相同存储容量、但不启动 page-cleaner/purge driver 的引擎配置
     */
    private EngineConfig foregroundOnlyConfig() {
        return new EngineConfig(directory, PAGE_SIZE, 256, SpaceId.of(5), PageNo.of(64),
                64, 100, Duration.ofSeconds(30), 128L * 1024 * 1024, List.of(),
                false, 128, Duration.ofMillis(10), 64, Duration.ofSeconds(30));
    }
}
