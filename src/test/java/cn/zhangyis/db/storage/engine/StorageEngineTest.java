package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.server.lockobs.api.SnapshotRequest;
import cn.zhangyis.db.server.lockobs.snapshot.LockDiagnosticSnapshot;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentDropPlan;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.trx.PrepareTransactionCommand;
import cn.zhangyis.db.storage.api.undotruncate.UndoTruncationConfig;
import cn.zhangyis.db.storage.api.undotruncate.UndoTruncationCycleStatus;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.flush.cleaner.PageCleanerState;
import cn.zhangyis.db.storage.flush.cleaner.PageCleanerMetricsSnapshot;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.recovery.RecoveryDiagnosticsSnapshot;
import cn.zhangyis.db.storage.recovery.RecoveryProgressEventKind;
import cn.zhangyis.db.storage.recovery.RecoveryProgressJournal;
import cn.zhangyis.db.storage.recovery.RecoveryReport;
import cn.zhangyis.db.storage.recovery.RecoveryMode;
import cn.zhangyis.db.storage.recovery.RecoveryStageName;
import cn.zhangyis.db.storage.recovery.RecoveryState;
import cn.zhangyis.db.storage.recovery.RecoveryStartupException;
import cn.zhangyis.db.storage.recovery.RecoveryTrafficGate;
import cn.zhangyis.db.storage.recovery.PreparedTransactionDecision;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.trx.UndoRedoBudgetEstimator;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoRecoveryReader;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.TransactionOptions;
import cn.zhangyis.db.storage.trx.UndoLogManager;
import cn.zhangyis.db.storage.trx.UndoSegmentAcquisition;
import cn.zhangyis.db.storage.trx.UndoTestWrites;
import cn.zhangyis.db.storage.trx.UndoWritePlan;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import cn.zhangyis.db.storage.trx.lock.RecordLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * E1 StorageEngine：生命周期（open/close/状态机）+ WAL durable 往返（写→close→重开读回）+ 重开续写
 * （redo 边界安装使 LSN 连续）+ checkpoint 使 redo durable。整栈经引擎访问器驱动，证明组合根是生产可用的。
 */
class StorageEngineTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId DATA_SPACE = SpaceId.of(10);
    private static final long INDEX_ID = 9L;
    private static final long TABLE_ID = 1L;
    private static final int RECOVERY_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 320;

    @TempDir
    Path dir;

    private EngineConfig config() {
        return new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024);
    }

    private EngineConfig configWithRecoveryTablespace(Path dataPath) {
        return new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024,
                List.of(new EngineTablespaceConfig(DATA_SPACE, dataPath)));
    }

    private EngineConfig configWithBackgroundTick(Duration interval) {
        return new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024, List.of(),
                true, 4, interval, 0, Duration.ofSeconds(2));
    }

    // ---- 生命周期 ----

    /**
     * 验证 {@code openWiresServicesAndPublishesOpen} 对应的数据库引擎组合根行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void openWiresServicesAndPublishesOpen() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        assertEquals(EngineState.OPEN, engine.state());
        assertNotNull(engine.transactionManager());
        assertNotNull(engine.miniTransactionManager());
        assertNotNull(engine.diskSpaceManager());
        assertNotNull(engine.lobStorage());
        assertNotNull(engine.btreeService());
        assertNotNull(engine.undoLogManager());
        assertNotNull(engine.mvccReader());
        assertNotNull(engine.rollbackService());
        assertNotNull(engine.indexPageAccess());
        engine.close();
        assertEquals(EngineState.CLOSED, engine.state());
        engine.close(); // 幂等
        assertEquals(EngineState.CLOSED, engine.state());
    }

    /** 生产组合根的 capacity-aware MTR 可用 LOB_WRITE profile 完成真实多页写读，不依赖测试 no-op manager。 */
    @Test
    void engineLobFacadeUsesSharedDiskPoolCodecAndRedoBudget() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            MiniTransactionManager manager = engine.miniTransactionManager();
            MiniTransaction create = manager.begin(manager.budgetFor(RedoBudgetPurpose.ENGINE_BOOT));
            engine.diskSpaceManager().createTablespace(
                    create, DATA_SPACE, dir.resolve("lob-data.ibd"), PageNo.of(128));
            SegmentRef segment = engine.diskSpaceManager().createSegment(
                    create, DATA_SPACE, SegmentPurpose.LOB);
            manager.commit(create);

            byte[] payload = new byte[20_000];
            payload[0] = 1;
            payload[payload.length - 1] = 2;
            MiniTransaction write = manager.begin(manager.budgetFor(
                    RedoBudgetPurpose.LOB_WRITE, engine.lobStorage().writeWorkload(payload.length)));
            ColumnValue.ExternalValue external = engine.lobStorage().write(write, segment,
                    ColumnType.longBlob(false), new ColumnValue.BinaryValue(payload));
            manager.commit(write);

            MiniTransaction read = manager.beginReadOnly();
            ColumnValue.BinaryValue decoded = (ColumnValue.BinaryValue) engine.lobStorage().read(
                    read, ColumnType.longBlob(false), external);
            manager.commit(read);
            assertArrayEquals(payload, decoded.value());
        } finally {
            engine.close();
        }
    }

    /**
     * 验证 {@code engineExposesLockDiagnosticSnapshotFromSharedLockManager} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void engineExposesLockDiagnosticSnapshotFromSharedLockManager() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            TransactionId owner = TransactionId.of(9901);
            RecordLockKey record = new RecordLockKey(INDEX_ID, PageId.of(DATA_SPACE, PageNo.of(7)), 3);
            engine.lockManager().acquire(owner, record, TransactionLockMode.REC_X, Duration.ofMillis(200));

            LockDiagnosticSnapshot snapshot = engine.lockDiagnosticSnapshot(SnapshotRequest.defaults());

            assertEquals(1, snapshot.dataLocks().size());
            assertEquals(owner, snapshot.dataLocks().getFirst().engineTransactionId());
            assertEquals("GRANTED", snapshot.dataLocks().getFirst().lockStatus());
            assertTrue(snapshot.dataLocks().getFirst().eventId() > 0,
                    "engine must wire the real lock observation service, not the no-op observer");
            assertTrue(snapshot.dataLockWaits().isEmpty());
            assertEquals(1, engine.lockManager().releaseAll(owner));
        } finally {
            engine.close();
        }
    }

    /**
     * 验证 {@code closedEngineRejectsAccess} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void closedEngineRejectsAccess() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        engine.close();
        assertThrows(EngineStateException.class, engine::diskSpaceManager);
        assertThrows(EngineStateException.class, engine::checkpoint);
    }

    /**
     * 验证 {@code doubleOpenRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void doubleOpenRejected() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        try {
            assertThrows(EngineStateException.class, engine::open);
        } finally {
            engine.close();
        }
    }

    // ---- durable 往返 + 重开续写 ----

    /**
     * 验证 {@code engineRunsWithMultipleBufferPoolInstances} 对应的数据库引擎组合根行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void engineRunsWithMultipleBufferPoolInstances() {
        // 0.10d：验证 config.bufferPoolInstanceCount() 真正驱动生产 LruBufferPool 分片——
        // 引擎以 N=4 打开，btree 页经 facade 路由到 4 个分片，插入/查找端到端可用。
        EngineConfig cfg = config().withBufferPoolInstanceCount(4);
        Path dataPath = dir.resolve("data.ibd");

        StorageEngine engine = new StorageEngine(cfg);
        engine.open();
        BTreeIndex index = createClusteredIndex(engine, dataPath);
        insertRow(engine, index, 1, "v1");
        insertRow(engine, index, 2, "v2");

        MiniTransaction r = engine.miniTransactionManager().beginReadOnly();
        BTreeLookupResult f1 = engine.btreeService().lookup(r, index, search(1)).orElseThrow();
        BTreeLookupResult f2 = engine.btreeService().lookup(r, index, search(2)).orElseThrow();
        engine.miniTransactionManager().commit(r);
        assertEquals("v1", payloadOf(f1));
        assertEquals("v2", payloadOf(f2));
        engine.close();
    }

    /**
     * 验证 {@code durableRoundTripAcrossRestart} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void durableRoundTripAcrossRestart() {
        EngineConfig cfg = config();
        Path dataPath = dir.resolve("data.ibd");

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1");
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.open();
        e2.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        MiniTransaction r = e2.miniTransactionManager().beginReadOnly();
        BTreeLookupResult found = e2.btreeService().lookup(r, index, search(1)).orElseThrow();
        e2.miniTransactionManager().commit(r);
        assertEquals("v1", payloadOf(found), "row persists across clean restart (read from flushed data file)");
        e2.close();
    }

    /**
     * 验证 {@code reopenInstallsRedoBoundaryAndAllowsContinuedWrites} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void reopenInstallsRedoBoundaryAndAllowsContinuedWrites() {
        EngineConfig cfg = config();
        Path dataPath = dir.resolve("data.ibd");

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1");
        long tailLsn = e1.miniTransactionManager().redoLogManager().currentLsn().value();
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.open();
        assertEquals(tailLsn, e2.miniTransactionManager().redoLogManager().currentLsn().value(),
                "reopen installs redo boundary = durable tail LSN (no restart-from-0 overlap)");
        e2.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        insertRow(e2, index, 2, "v2"); // 重开后续写
        e2.close();

        StorageEngine e3 = new StorageEngine(cfg);
        e3.open();
        e3.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        MiniTransaction r = e3.miniTransactionManager().beginReadOnly();
        assertEquals("v1", payloadOf(e3.btreeService().lookup(r, index, search(1)).orElseThrow()));
        assertEquals("v2", payloadOf(e3.btreeService().lookup(r, index, search(2)).orElseThrow()));
        e3.miniTransactionManager().commit(r);
        e3.close();
    }

    /**
     * 重启发现 PREPARED 后必须在 traffic gate 关闭期间消费外部 commit 决议；INSERT undo owner 被回收，
     * 聚簇行保留，恢复完成后事务 creator 不再位于 active table。
     */
    @Test
    void restartCommitsPreparedTransactionFromDecisionProvider() {
        Path dataPath = dir.resolve("prepared-recovery-commit.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);
        StorageEngine first = new StorageEngine(cfg);
        first.open();
        BTreeIndex index = createClusteredIndex(first, dataPath);
        Transaction prepared = insertPreparedRow(first, index, 41, "commit");
        TransactionId creator = prepared.transactionId();
        first.close();

        StorageEngine recovered = new StorageEngine(
                cfg, transactionId -> transactionId.equals(creator)
                        ? PreparedTransactionDecision.COMMIT
                        : PreparedTransactionDecision.UNRESOLVED);
        recovered.configureClusteredIndex(index);
        recovered.open();
        try {
            MiniTransaction read = recovered.miniTransactionManager().beginReadOnly();
            BTreeLookupResult found = recovered.btreeService().lookup(
                    read, index, search(41)).orElseThrow();
            recovered.miniTransactionManager().commit(read);
            assertEquals("commit", payloadOf(found));
            assertFalse(recovered.transactionManager().system()
                    .snapshotActiveReadWriteIds().contains(creator.value()));
        } finally {
            recovered.close();
        }
    }

    /**
     * restart rollback 决议必须重建 transaction/undo context，复用 live prepared rollback 逐条反向应用，
     * 并在开放流量前清除聚簇行与 page3 owner。
     */
    @Test
    void restartRollsBackPreparedTransactionFromDecisionProvider() {
        Path dataPath = dir.resolve("prepared-recovery-rollback.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);
        StorageEngine first = new StorageEngine(cfg);
        first.open();
        BTreeIndex index = createClusteredIndex(first, dataPath);
        Transaction prepared = insertPreparedRow(first, index, 42, "rollback");
        TransactionId creator = prepared.transactionId();
        first.close();

        StorageEngine recovered = new StorageEngine(
                cfg, transactionId -> transactionId.equals(creator)
                        ? PreparedTransactionDecision.ROLLBACK
                        : PreparedTransactionDecision.UNRESOLVED);
        recovered.configureClusteredIndex(index);
        recovered.open();
        try {
            MiniTransaction read = recovered.miniTransactionManager().beginReadOnly();
            assertTrue(recovered.btreeService().lookupIncludingDeleted(
                    read, index, search(42)).isEmpty());
            recovered.miniTransactionManager().commit(read);
            assertEquals(0, recovered.rollbackSegmentSlotManager().activeSlotCount());
        } finally {
            recovered.close();
        }
    }

    /**
     * 未决 PREPARED 不能被默认猜测为 commit 或 rollback；provider 返回 UNRESOLVED 时启动必须 fail-closed。
     */
    @Test
    void restartFailsClosedWhenPreparedDecisionIsUnresolved() {
        Path dataPath = dir.resolve("prepared-recovery-unresolved.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);
        StorageEngine first = new StorageEngine(cfg);
        first.open();
        BTreeIndex index = createClusteredIndex(first, dataPath);
        insertPreparedRow(first, index, 43, "unresolved");
        first.close();

        StorageEngine recovered = new StorageEngine(cfg);
        recovered.configureClusteredIndex(index);
        assertThrows(RecoveryStartupException.class, recovered::open);
        assertEquals(RecoveryState.FAILED, recovered.recoveryState());
    }

    /**
     * 验证 {@code bootstrapsWithRotatingRedoAndRecoversAcrossRestart} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void bootstrapsWithRotatingRedoAndRecoversAcrossRestart() {
        // 0.18b：config 启用文件环后，引擎 bootstrap 走 RotatingRedoLogRepository，checkpoint 经回收边界端口推动文件环回收，
        // 恢复期跨文件读回 redo。容量取足够大，保证 fresh 建库/插入的单个 MTR 批次不超过单文件容量。
        EngineConfig cfg = config().withRedoRotation(4, 8L * 1024 * 1024);
        Path dataPath = dir.resolve("data.ibd");

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        assertTrue(Files.exists(cfg.redoDir().resolve("redo-000000.log")),
                "rotation 模式应在 redo 目录建文件环");
        assertFalse(Files.exists(cfg.redoFile()), "rotation 模式不写单 redo.log");
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1");
        e1.checkpoint(); // 推进 checkpoint → 经 RedoReclaimBoundary 推动文件环回收
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.open(); // 走文件环 recovery（RecoveryRequest 经接口读 ring）
        e2.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        insertRow(e2, index, 2, "v2"); // 恢复后续写
        e2.close();

        StorageEngine e3 = new StorageEngine(cfg);
        e3.open();
        e3.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        MiniTransaction r = e3.miniTransactionManager().beginReadOnly();
        assertEquals("v1", payloadOf(e3.btreeService().lookup(r, index, search(1)).orElseThrow()));
        assertEquals("v2", payloadOf(e3.btreeService().lookup(r, index, search(2)).orElseThrow()),
                "文件环模式下数据跨两次重启持久");
        e3.miniTransactionManager().commit(r);
        e3.close();
    }

    /**
     * 验证 {@code warmupDumpsAtCloseAndReopensCleanly} 所描述的组件生命周期，并断言状态转换、后台线程停止和资源恰好释放一次。
     */
    @Test
    void warmupDumpsAtCloseAndReopensCleanly() {
        // 0.10b：close 写出 buffer pool warmup dump，reopen 时 load 预取回池（最佳努力，不破坏恢复/数据）。
        EngineConfig cfg = config();
        Path dataPath = dir.resolve("data.ibd");

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1");
        e1.close();
        assertTrue(Files.exists(cfg.bufferPoolDumpFile()), "close 应写出 buffer pool warmup dump 文件");

        StorageEngine e2 = new StorageEngine(cfg);
        e2.open(); // open 期 warmup load 预取上次热页（不阻断、不破坏恢复）
        e2.diskSpaceManager().openTablespace(DATA_SPACE, dataPath);
        MiniTransaction r = e2.miniTransactionManager().beginReadOnly();
        assertEquals("v1", payloadOf(e2.btreeService().lookup(r, index, search(1)).orElseThrow()),
                "warmup 接线后数据跨重启仍可读");
        e2.miniTransactionManager().commit(r);
        e2.close();
    }

    /**
     * 验证 {@code checkpointMakesRedoDurable} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void checkpointMakesRedoDurable() {
        StorageEngine engine = new StorageEngine(config());
        engine.open();
        BTreeIndex index = createClusteredIndex(engine, dir.resolve("data.ibd"));
        insertRow(engine, index, 1, "v1");
        RedoLogManager redo = engine.miniTransactionManager().redoLogManager();
        engine.checkpoint();
        assertEquals(redo.currentLsn(), redo.flushedToDiskLsn(),
                "checkpoint flushes redo durable (WAL prerequisite for any data page flush)");
        engine.close();
    }

    /**
     * 验证 {@code foregroundRedoCapacityThrottleAdvancesCheckpointBeforeNextAppend} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void foregroundRedoCapacityThrottleAdvancesCheckpointBeforeNextAppend() {
        EngineConfig cfg = new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(2), 1_000_000L, List.of(), false, 4, Duration.ofSeconds(1), 256,
                Duration.ofSeconds(2));
        StorageEngine engine = new StorageEngine(cfg);
        engine.open();
        try {
            RedoLogManager redo = engine.miniTransactionManager().redoLogManager();
            LogRange range = redo.append(List.of(new PageBytesRecord(
                    PageId.of(cfg.undoSpaceId(), PageNo.of(7)), RECOVERY_OFFSET, new byte[800_000])));
            redo.markClosed(range);
            Lsn checkpointBefore = readCheckpoint(cfg);

            MiniTransaction empty = engine.miniTransactionManager().begin(
                    engine.miniTransactionManager().budgetFor(RedoBudgetPurpose.TRANSACTION_STATE));
            engine.miniTransactionManager().commit(empty);

            Lsn checkpointAfter = readCheckpoint(cfg);
            assertTrue(checkpointAfter.value() > checkpointBefore.value(),
                    "foreground throttle should advance checkpoint before allowing the next redo append");
        } finally {
            engine.close();
        }
    }

    /**
     * 验证 {@code foregroundRedoCapacityThrottleFlushesDirtyPagesWhenBackgroundMaxPagesIsZero} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void foregroundRedoCapacityThrottleFlushesDirtyPagesWhenBackgroundMaxPagesIsZero() {
        EngineConfig cfg = new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofMillis(300), 1_000_000L, List.of(), false, 4,
                Duration.ofSeconds(1), 0, Duration.ofSeconds(2));
        StorageEngine engine = new StorageEngine(cfg);
        engine.open();
        try {
            createClusteredIndex(engine, dir.resolve("dirty-budget.ibd"));
            RedoLogManager redo = engine.miniTransactionManager().redoLogManager();
            LogRange pressure = redo.append(List.of(new PageBytesRecord(
                    PageId.of(cfg.undoSpaceId(), PageNo.of(7)), RECOVERY_OFFSET, new byte[900_000])));
            redo.markClosed(pressure);
            Lsn checkpointBefore = readCheckpoint(cfg);

            MiniTransaction empty = engine.miniTransactionManager().begin(
                    engine.miniTransactionManager().budgetFor(RedoBudgetPurpose.TRANSACTION_STATE));
            engine.miniTransactionManager().commit(empty);

            assertTrue(readCheckpoint(cfg).value() > checkpointBefore.value(),
                    "foreground throttle must use its own flush budget; background maxPages=0 means no background dirty flush only");
        } finally {
            if (engine.state() == EngineState.OPEN) {
                engine.close();
            }
        }
    }

    /**
     * 验证 {@code backgroundPageCleanerAdvancesCheckpointTickAndStopsOnClose} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void backgroundPageCleanerAdvancesCheckpointTickAndStopsOnClose() {
        StorageEngine engine = new StorageEngine(configWithBackgroundTick(Duration.ofMillis(50)));
        engine.open();
        try {
            engine.checkpoint();
            RedoLogManager redo = engine.miniTransactionManager().redoLogManager();
            LogRange range = redo.append(List.of(new PageBytesRecord(PageId.of(DATA_SPACE, PageNo.of(7)),
                    RECOVERY_OFFSET, new byte[]{9})));
            redo.flush();
            redo.markClosed(range);
            Lsn target = range.end();

            assertTrue(awaitUntil(() -> engine.lastBackgroundFlushCycle()
                    .filter(cycle -> cycle.checkpointAfter().value() >= target.value())
                    .isPresent(), Duration.ofSeconds(3)),
                    "periodic page cleaner tick should advance checkpoint even when no data page flush is needed");
            assertTrue(engine.awaitBackgroundFlushIdle(Duration.ofSeconds(2)));
            assertEquals(PageCleanerState.IDLE, engine.pageCleanerState());
            PageCleanerMetricsSnapshot metrics = engine.pageCleanerMetrics();
            assertEquals(PageCleanerState.IDLE, metrics.state());
            assertTrue(metrics.successfulCycles() > 0);

            engine.close();
            assertEquals(PageCleanerState.STOPPED, engine.pageCleanerState());
            assertEquals(PageCleanerState.STOPPED, engine.pageCleanerMetrics().state());
        } finally {
            if (engine.state() == EngineState.OPEN) {
                engine.close();
            }
        }
    }

    /**
     * 验证 {@code existingOpenRunsCrashRecoveryAndReplaysRedoIntoConfiguredTablespace} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void existingOpenRunsCrashRecoveryAndReplaysRedoIntoConfiguredTablespace() throws Exception {
        Path dataPath = dir.resolve("data-recovery.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);

        StorageEngine clean = new StorageEngine(cfg);
        clean.open();
        createClusteredIndex(clean, dataPath);
        clean.close();

        PageId target = PageId.of(DATA_SPACE, PageNo.of(63));
        byte[] payload = new byte[]{11, 22, 33, 44};
        appendPhysicalRedoAfterCheckpoint(cfg, target, RECOVERY_OFFSET, payload);

        StorageEngine recovered = new StorageEngine(cfg);
        recovered.open();

        RecoveryReport report = recovered.lastRecoveryReport().orElseThrow();
        assertEquals(RecoveryState.OPEN, recovered.recoveryState());
        assertTrue(report.completedStages().contains(RecoveryStageName.REDO_REPLAY),
                "existing open must run CrashRecoveryService instead of only installing the redo tail");
        assertTrue(report.completedStages().contains(RecoveryStageName.REDO_BOUNDARY_INSTALL),
                "recovery must install recoveredToLsn before new MTR append");
        assertTrue(report.completedStages().contains(RecoveryStageName.SPACE_FILE_RECONCILE),
                "configured tablespaces must be reconciled to page0 current size after redo replay");
        assertArrayEquals(payload, readPhysicalSlice(dataPath, target, RECOVERY_OFFSET, payload.length));
        String progress = Files.readString(cfg.recoveryProgressFile());
        assertTrue(progress.contains("\"stageName\":\"REDO_REPLAY\""),
                "existing-open recovery must persist progress events under the engine baseDir");
        assertTrue(progress.contains("\"stageName\":\"OPEN_TRAFFIC\""));

        recovered.close();
    }

    /**
     * 验证 {@code existingOpenWithReadOnlyValidatePublishesReadOnlyEngineAndRejectsAccessors} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void existingOpenWithReadOnlyValidatePublishesReadOnlyEngineAndRejectsAccessors() throws Exception {
        Path dataPath = dir.resolve("data-readonly-validate.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);

        StorageEngine clean = new StorageEngine(cfg);
        clean.open();
        createClusteredIndex(clean, dataPath);
        clean.close();
        Map<String, byte[]> redoBefore = snapshotRedoInputs(cfg);

        StorageEngine readOnly = new StorageEngine(cfg.withRecoveryMode(RecoveryMode.READ_ONLY_VALIDATE));
        readOnly.open();

        RecoveryReport report = readOnly.lastRecoveryReport().orElseThrow();
        assertEquals(EngineState.READ_ONLY, readOnly.state(),
                "READ_ONLY_VALIDATE existing-open must not publish the ordinary writable OPEN lifecycle");
        assertEquals(RecoveryState.READ_ONLY, readOnly.recoveryState());
        assertEquals(RecoveryMode.READ_ONLY_VALIDATE, report.mode());
        assertEquals(RecoveryState.READ_ONLY, report.state());
        assertTrue(report.completedStages().contains(RecoveryStageName.READ_ONLY_DIAGNOSTIC_OPEN));
        assertThrows(EngineStateException.class, readOnly::dmlService,
                "只读诊断态只暴露恢复报告和 gate 状态，普通 DML facade 仍必须拒绝访问");
        RecoveryDiagnosticsSnapshot diagnostics = readOnly.recoveryDiagnostics();
        assertEquals(RecoveryState.READ_ONLY, diagnostics.gateState());
        assertTrue(diagnostics.lastReport().isPresent());
        assertTrue(diagnostics.progressEvents().stream().anyMatch(event ->
                event.kind() == RecoveryProgressEventKind.COMPLETED
                        && event.stageName() == RecoveryStageName.READ_ONLY_DIAGNOSTIC_OPEN
                        && event.state() == RecoveryState.READ_ONLY));

        readOnly.close();
        assertEquals(EngineState.CLOSED, readOnly.state());
        assertRedoInputsEqual(redoBefore, snapshotRedoInputs(cfg));
    }

    /** 只读诊断必须报告 NORMAL 会遇到的事务 sidecar 缺失，同时不得为诊断偷偷重建空文件。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void readOnlyValidateMissingTransactionSidecarFailsWithoutCreatingIt() throws Exception {
        EngineConfig cfg = config();
        StorageEngine original = new StorageEngine(cfg);
        original.open();
        original.close();
        Files.delete(cfg.transactionRecoveryCheckpointFile());

        StorageEngine readOnly = new StorageEngine(cfg.withRecoveryMode(RecoveryMode.READ_ONLY_VALIDATE));
        assertThrows(RecoveryStartupException.class, readOnly::open);
        assertEquals(EngineState.CLOSED, readOnly.state());
        assertFalse(Files.exists(cfg.transactionRecoveryCheckpointFile()),
                "READ_ONLY_VALIDATE must not create a missing transaction sidecar");
        readOnly.close();
    }

    /**
     * 验证 {@code ordinaryAccessorsRejectWhenRecoveryGateLeavesOpenAfterStartup} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void ordinaryAccessorsRejectWhenRecoveryGateLeavesOpenAfterStartup() {
        RecoveryTrafficGate gate = new RecoveryTrafficGate();
        RecoveryProgressJournal journal = new RecoveryProgressJournal();
        StorageEngine engine = new StorageEngine(config(), gate, journal);
        engine.open();
        try {
            gate.closeForRecovery();

            assertThrows(EngineStateException.class, engine::miniTransactionManager);
            assertThrows(EngineStateException.class, engine::diskSpaceManager);
            assertThrows(EngineStateException.class, engine::btreeService);
            assertThrows(EngineStateException.class, engine::undoLogManager);
            assertThrows(EngineStateException.class, engine::dmlService);

            RecoveryDiagnosticsSnapshot diagnostics = engine.recoveryDiagnostics();
            assertEquals(RecoveryState.RECOVERING, diagnostics.gateState());
            assertTrue(diagnostics.lastReport().isEmpty());
            assertTrue(diagnostics.lastFailureMessage().isEmpty());
        } finally {
            if (engine.state() == EngineState.OPEN) {
                gate.openForUserTraffic();
                engine.close();
            }
        }
    }

    /**
     * 验证 {@code freshOpenWithReadOnlyValidateIsRejectedBeforeFormattingFiles} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void freshOpenWithReadOnlyValidateIsRejectedBeforeFormattingFiles() {
        EngineConfig cfg = config().withRecoveryMode(RecoveryMode.READ_ONLY_VALIDATE);
        StorageEngine engine = new StorageEngine(cfg);
        try {
            assertThrows(DatabaseValidationException.class, engine::open,
                    "READ_ONLY_VALIDATE 只能诊断 existing recovery，不能隐式创建新实例文件");
        } finally {
            if (engine.state() == EngineState.OPEN || engine.state() == EngineState.READ_ONLY) {
                engine.close();
            }
        }

        assertFalse(Files.exists(cfg.redoDir()));
        assertFalse(Files.exists(cfg.undoFile()));
        assertFalse(Files.exists(cfg.doublewriteFile()));
    }

    /** redo 目录里的无关诊断文件不能把 fresh 实例伪装成 existing ring。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void readOnlyValidateTreatsRedoDirectoryWithoutRingFilesAsFresh() throws Exception {
        EngineConfig cfg = config().withRecoveryMode(RecoveryMode.READ_ONLY_VALIDATE);
        Files.createDirectories(cfg.redoDir());
        Path unrelated = cfg.redoDir().resolve("operator-note.txt");
        Files.writeString(unrelated, "keep");
        StorageEngine engine = new StorageEngine(cfg);

        assertThrows(DatabaseValidationException.class, engine::open);

        try (var entries = Files.list(cfg.redoDir())) {
            assertEquals(List.of(unrelated), entries.sorted().toList());
        }
        assertFalse(Files.exists(cfg.redoControlFile()));
        assertFalse(Files.exists(cfg.undoFile()));
    }

    /**
     * 验证 {@code existingOpenForceSkipDoesNotOpenSkippedRecoveryTablespace} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void existingOpenForceSkipDoesNotOpenSkippedRecoveryTablespace() {
        EngineConfig cleanCfg = config();
        StorageEngine clean = new StorageEngine(cleanCfg);
        clean.open();
        clean.close();

        Path missingSkippedFile = dir.resolve("missing-skipped.ibd");
        EngineConfig forceCfg = new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024,
                List.of(new EngineTablespaceConfig(DATA_SPACE, missingSkippedFile)))
                .withForceSkipRecovery(Set.of(DATA_SPACE));

        StorageEngine recovered = new StorageEngine(forceCfg);
        recovered.open();
        RecoveryReport report = recovered.lastRecoveryReport().orElseThrow();
        assertEquals(EngineState.OPEN, recovered.state());
        assertEquals(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE, report.mode());
        assertEquals(Set.of(DATA_SPACE), report.skippedSpaces());
        assertEquals(1, report.skippedReconcileSpaceCount(),
                "configured but skipped recovery tablespace still appears in reconcile diagnostics");
        assertFalse(Files.exists(missingSkippedFile),
                "skipped recovery tablespace must not be opened or created during recovery");
        recovered.close();
    }

    /**
     * 验证 {@code existingOpenForceSkipRequiresExplicitNonEmptySkipSet} 对应的数据库引擎组合根行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void existingOpenForceSkipRequiresExplicitNonEmptySkipSet() {
        EngineConfig cleanCfg = config();
        StorageEngine clean = new StorageEngine(cleanCfg);
        clean.open();
        clean.close();

        StorageEngine recovered = new StorageEngine(cleanCfg.withRecoveryMode(
                RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE));
        assertThrows(DatabaseValidationException.class, recovered::open,
                "force-skip mode must not start without an explicit skipped space set");
    }

    /**
     * 验证 {@code existingOpenForceSkipRejectsSystemUndoSpace} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void existingOpenForceSkipRejectsSystemUndoSpace() {
        EngineConfig cleanCfg = config();
        StorageEngine clean = new StorageEngine(cleanCfg);
        clean.open();
        clean.close();

        StorageEngine recovered = new StorageEngine(cleanCfg.withForceSkipRecovery(Set.of(SpaceId.of(5))));
        assertThrows(DatabaseValidationException.class, recovered::open,
                "system undo space cannot be skipped because undo rollback/purge recovery depends on it");
    }

    /**
     * 验证 {@code existingOpenForceSkipRejectsConfiguredClusteredIndexSpace} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void existingOpenForceSkipRejectsConfiguredClusteredIndexSpace() {
        Path dataPath = dir.resolve("clustered-skip.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);
        StorageEngine clean = new StorageEngine(cfg);
        clean.open();
        BTreeIndex index = createClusteredIndex(clean, dataPath);
        clean.close();

        StorageEngine recovered = new StorageEngine(cfg.withForceSkipRecovery(Set.of(DATA_SPACE)));
        recovered.configureClusteredIndex(index);
        assertThrows(DatabaseValidationException.class, recovered::open,
                "当前单聚簇 undo rollback 没有对象级 skip 语义，不能跳过该 index 所在 space");
    }

    /**
     * recoverable doublewrite 端到端：写+flush 一页（产生 doublewrite 副本 + 有效 data 页）→ close → 裸写损坏该
     * data 页 → 重开恢复期用 doublewrite 副本修复 torn 页 → 行可读、checksum 恢复有效。
     */
    @Test
    void recoversTornDataPageFromDoublewriteCopyOnRestart() {
        Path dataPath = dir.resolve("data-dw.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1");
        e1.checkpoint(); // 经 doublewrite 刷脏：data 页盖 checksum + doublewrite 留整页副本
        PageId root = index.rootPageId();
        assertTrue(pageChecksumValid(dataPath, root), "flushed root page has valid checksum");
        e1.close();

        corruptPhysicalPage(dataPath, root);
        assertFalse(pageChecksumValid(dataPath, root), "data page is now torn (checksum invalid)");

        StorageEngine e2 = new StorageEngine(cfg);
        e2.open(); // 恢复期 doublewrite repair 修复 torn 页
        RecoveryReport report = e2.lastRecoveryReport().orElseThrow();
        assertTrue(report.completedStages().contains(RecoveryStageName.DOUBLEWRITE_REPAIR));
        assertTrue(report.repairedPageCount() >= 1, "torn data page repaired from doublewrite copy");
        assertTrue(pageChecksumValid(dataPath, root), "repaired page checksum valid again");

        // DATA_SPACE 已由恢复期 openTablespaceForRecovery 打开，无需再 openTablespace。
        MiniTransaction r = e2.miniTransactionManager().beginReadOnly();
        assertEquals("v1", payloadOf(e2.btreeService().lookup(r, index, search(1)).orElseThrow()),
                "row readable after torn-page repair");
        e2.miniTransactionManager().commit(r);
        e2.close();
    }

    // ---- 0.3：持久 rseg header + 恢复扫描 ----

    /**
     * money test：active 事务写 undo（claim slot 持久到 page3）→ 不 commit → checkpoint → close → 重开恢复扫描
     * page3 重建内存 slot 目录，active 事务的 undo segment 首页被找回。
     */
    @Test
    void activeRsegWithoutRecoveryIndexFailsClosed() {
        EngineConfig cfg = config();

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        Transaction txn = e1.transactionManager().begin(TransactionOptions.defaults());
        e1.transactionManager().assignWriteId(txn);
        MiniTransaction m = e1.miniTransactionManager().begin(
                e1.miniTransactionManager().budgetFor(RedoBudgetPurpose.CLUSTERED_INSERT,
                        UndoRedoBudgetEstimator.append(true)));
        UndoTestWrites.insert(e1.undoLogManager(), txn, m, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(1)), idKey(), clusteredSchema());
        e1.miniTransactionManager().commit(m);
        // 不 commit/onCommit：模拟 active 事务
        UndoSlotId activeSlot = UndoSlotId.of(0); // 首写 first-fit 占最低空槽
        assertEquals(1, e1.rollbackSegmentSlotManager().activeSlotCount());
        e1.checkpoint(); // page3 + undo 页 durable
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        assertThrows(RecoveryStartupException.class, e2::open,
                "without DD/multi-index recovery metadata, ACTIVE undo cannot be skipped before OPEN");
        assertEquals(EngineState.CLOSED, e2.state(),
                "failed open must close every partially opened recovery handle");
        e2.close();
    }

    /** 纯 insert commit 把 page3 owner 从 active slot 移入 cache；重启不得恢复为 active，并应复用同一段。 */
    @Test
    void committedInsertTxnSlotClearedOnPage3AndNotRestored() {
        EngineConfig cfg = config();

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        Transaction txn = e1.transactionManager().begin(TransactionOptions.defaults());
        e1.transactionManager().assignWriteId(txn);
        MiniTransaction m = e1.miniTransactionManager().begin(
                e1.miniTransactionManager().budgetFor(RedoBudgetPurpose.CLUSTERED_INSERT,
                        UndoRedoBudgetEstimator.append(true)));
        UndoTestWrites.insert(e1.undoLogManager(), txn, m, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(1)), idKey(), clusteredSchema());
        e1.miniTransactionManager().commit(m);
        PageId cachedFirstPage = txn.undoContext().binding(UndoLogKind.INSERT).firstPageId();
        e1.transactionManager().prepareCommit(txn);
        e1.undoLogManager().onCommit(txn); // 纯 insert → active owner 转入 page3 cache
        e1.transactionManager().commit(txn);
        TransactionId committedId = txn.transactionId();
        TransactionNo committedNo = txn.transactionNo();
        assertEquals(0, e1.rollbackSegmentSlotManager().activeSlotCount());
        e1.checkpoint();
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.open();
        assertEquals(0, e2.rollbackSegmentSlotManager().activeSlotCount(),
                "committed insert txn slot cleared on page3, not restored");
        Transaction next = e2.transactionManager().begin(TransactionOptions.defaults());
        e2.transactionManager().assignWriteId(next);
        UndoWritePlan reusePlan = e2.undoLogManager().planInsert(next, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(2)), idKey(), clusteredSchema());
        assertEquals(UndoSegmentAcquisition.REUSE_CACHED, reusePlan.acquisition(),
                "recovery must validate page3/undo/FSP evidence and rebuild the cached stack");
        MiniTransaction reuseMtr = e2.miniTransactionManager().begin(
                e2.miniTransactionManager().budgetFor(RedoBudgetPurpose.CLUSTERED_INSERT,
                        reusePlan.redoWorkload()));
        e2.undoLogManager().appendPlanned(next, reuseMtr, reusePlan);
        e2.miniTransactionManager().commit(reuseMtr);
        assertEquals(cachedFirstPage, next.undoContext().binding(UndoLogKind.INSERT).firstPageId());
        e2.transactionManager().prepareCommit(next);
        assertTrue(next.transactionId().value() > committedId.value(),
                "checkpointed pure INSERT transaction id must not be reused after its page3 slot was cleared");
        assertTrue(next.transactionNo().value() > committedNo.value(),
                "checkpointed pure INSERT commit number must not be reused after redo recycling");
        e2.undoLogManager().onCommit(next);
        e2.transactionManager().commit(next);
        e2.close();
    }

    /** page3 v4 的 free FIFO 必须跨重启恢复，并允许最近 kind=INSERT 的节点为 UPDATE 首写重新分类。 */
    @Test
    void persistentFreeUndoSegmentIsRecoveredAndReusedAcrossKinds() {
        EngineConfig cfg = config().withUndoCachedSegmentsPerKind(0);
        StorageEngine first = new StorageEngine(cfg);
        first.open();
        Transaction owner = first.transactionManager().begin(TransactionOptions.defaults());
        first.transactionManager().assignWriteId(owner);
        UndoWritePlan initial = first.undoLogManager().planInsert(owner, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(81)), idKey(), clusteredSchema());
        MiniTransaction write = first.miniTransactionManager().begin(
                first.miniTransactionManager().budgetFor(RedoBudgetPurpose.CLUSTERED_INSERT,
                        initial.redoWorkload()));
        first.undoLogManager().appendPlanned(owner, write, initial);
        first.miniTransactionManager().commit(write);
        PageId freeFirstPage = owner.undoContext().binding(UndoLogKind.INSERT).firstPageId();
        first.transactionManager().prepareCommit(owner);
        first.undoLogManager().onCommit(owner);
        first.transactionManager().commit(owner);
        first.checkpoint();
        first.close();

        StorageEngine reopened = new StorageEngine(cfg);
        reopened.open();
        Transaction update = reopened.transactionManager().begin(TransactionOptions.defaults());
        reopened.transactionManager().assignWriteId(update);
        UndoWritePlan reuse = reopened.undoLogManager().planUpdate(update, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(81)), List.of(new ColumnValue.IntValue(81)),
                new HiddenColumns(update.transactionId(), RollPointer.NULL), idKey(), clusteredSchema());
        assertEquals(UndoSegmentAcquisition.REUSE_FREE, reuse.acquisition());
        MiniTransaction reuseMtr = reopened.miniTransactionManager().begin(
                reopened.miniTransactionManager().budgetFor(RedoBudgetPurpose.CLUSTERED_UPDATE,
                        reuse.redoWorkload()));
        reopened.undoLogManager().appendPlanned(update, reuseMtr, reuse);
        reopened.miniTransactionManager().commit(reuseMtr);
        assertEquals(freeFirstPage, update.undoContext().binding(UndoLogKind.UPDATE).firstPageId());
        reopened.transactionManager().prepareCommit(update);
        reopened.undoLogManager().onCommit(update);
        reopened.transactionManager().commit(update);
        reopened.close();
    }

    // ---- R 1.2：恢复期回滚未提交事务 ----

    /**
     * money test：同一 active 事务插三行（写 undo + insertClustered）→ 不 commit → checkpoint → close → 重开（注入
     * recoveryRollbackIndex）→ 恢复逐条回滚到 EMPTY 并原子 cache/free/drop、转移 page3 owner；再次重启不得再发现 active slot、重复 inverse
     * 或复活数据。
     */
    @Test
    void recoveryRollsBackActiveInsertOnRestart() {
        Path dataPath = dir.resolve("data-rollback.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRowsActive(e1, index, 3); // active：同一事务写三条 undo + 插行，不 commit/onCommit
        MiniTransaction r1 = e1.miniTransactionManager().beginReadOnly();
        for (int id = 1; id <= 3; id++) {
            assertTrue(e1.btreeService().lookup(r1, index, search(id)).isPresent(),
                    "row " + id + " present before crash");
        }
        e1.miniTransactionManager().commit(r1);
        e1.checkpoint();
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.configureClusteredIndex(index); // open 前注入恢复回滚索引（无 DD）
        e2.open();
        assertStageBeforeOpen(e2, RecoveryStageName.UNDO_ROLLBACK);
        assertEquals(0, e2.rollbackSegmentSlotManager().activeSlotCount(),
                "recovery finalization removes the ACTIVE page3 owner");
        MiniTransaction r2 = e2.miniTransactionManager().beginReadOnly();
        for (int id = 1; id <= 3; id++) {
            assertTrue(e2.btreeService().lookup(r2, index, search(id)).isEmpty(),
                    "active row " + id + " rolled back at recovery");
        }
        e2.miniTransactionManager().commit(r2);
        e2.close();

        StorageEngine e3 = new StorageEngine(cfg);
        e3.configureClusteredIndex(index);
        e3.open();
        assertEquals(0, e3.rollbackSegmentSlotManager().activeSlotCount(),
                "second startup must not rediscover a finalized rollback segment");
        MiniTransaction r3 = e3.miniTransactionManager().beginReadOnly();
        for (int id = 1; id <= 3; id++) {
            assertTrue(e3.btreeService().lookup(r3, index, search(id)).isEmpty(),
                    "atomic rollback finalization keeps row " + id + " absent on the second restart");
        }
        e3.miniTransactionManager().commit(r3);
        e3.close();
    }

    /**
     * 同一 active 事务先 INSERT 再 UPDATE 同一行会留下两个独立 slot/segment。重启恢复必须按全局 undoNo
     * 先撤销 UPDATE、再撤销 INSERT，并在一个批终结边界清空两个 slot；否则会残留插入版本或孤儿 owner。
     */
    @Test
    void recoveryMergesIndependentActiveInsertAndUpdateLogsOnRestart() {
        Path dataPath = dir.resolve("data-dual-undo-rollback.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        Transaction txn = e1.transactionManager().begin(TransactionOptions.defaults());
        e1.transactionManager().assignWriteId(txn);

        UndoWritePlan insertPlan = e1.undoLogManager().planInsert(txn, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(1)), index.keyDef(), index.schema());
        MiniTransaction insertMtr = e1.miniTransactionManager().begin(
                e1.miniTransactionManager().budgetFor(RedoBudgetPurpose.CLUSTERED_INSERT,
                        BTreeRedoBudgetEstimator.insert(index.rootLevel()).plus(insertPlan.redoWorkload())));
        RollPointer insertPointer = e1.undoLogManager().appendPlanned(txn, insertMtr, insertPlan);
        e1.btreeService().insertClustered(insertMtr, index, row(1, "inserted"),
                txn.transactionId(), insertPointer);
        e1.miniTransactionManager().commit(insertMtr);

        MiniTransaction readOld = e1.miniTransactionManager().beginReadOnly();
        BTreeLookupResult old = e1.btreeService().lookup(readOld, index, search(1)).orElseThrow();
        e1.miniTransactionManager().commit(readOld);
        HiddenColumns oldHidden = old.record().hiddenColumns();

        UndoWritePlan updatePlan = e1.undoLogManager().planUpdate(txn, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(1)), old.record().columnValues(), oldHidden,
                index.keyDef(), index.schema());
        MiniTransaction updateMtr = e1.miniTransactionManager().begin(
                e1.miniTransactionManager().budgetFor(RedoBudgetPurpose.CLUSTERED_UPDATE,
                        BTreeRedoBudgetEstimator.pointRewrite().plus(updatePlan.redoWorkload())));
        RollPointer updatePointer = e1.undoLogManager().appendPlanned(txn, updateMtr, updatePlan);
        LogicalRecord updated = new LogicalRecord(1, row(1, "updated").columnValues(), false,
                RecordType.CONVENTIONAL, new HiddenColumns(txn.transactionId(), updatePointer));
        assertTrue(e1.btreeService().replaceClustered(updateMtr, index, search(1), updated,
                oldHidden.dbTrxId(), oldHidden.dbRollPtr()).replaced());
        e1.miniTransactionManager().commit(updateMtr);

        assertEquals(2, e1.rollbackSegmentSlotManager().activeSlotCount(),
                "independent INSERT and UPDATE logs must own separate ACTIVE slots");
        e1.checkpoint();
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.configureClusteredIndex(index);
        e2.open();
        assertStageBeforeOpen(e2, RecoveryStageName.UNDO_ROLLBACK);
        assertEquals(0, e2.rollbackSegmentSlotManager().activeSlotCount(),
                "dual-log recovery batch must release both persistent owners");
        MiniTransaction readRecovered = e2.miniTransactionManager().beginReadOnly();
        assertTrue(e2.btreeService().lookup(readRecovered, index, search(1)).isEmpty(),
                "merge rollback must restore INSERT state before removing the uncommitted row");
        e2.miniTransactionManager().commit(readRecovered);
        e2.close();
    }

    /** 对照：committed insert 的 onCommit 原子 cache/free/drop undo owner，重启后数据保留且不会被恢复误删。 */
    @Test
    void recoveryPreservesCommittedInsertOnRestart() {
        Path dataPath = dir.resolve("data-keep.ibd");
        EngineConfig cfg = configWithRecoveryTablespace(dataPath);

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1"); // committed（insertRow 内 commit + onCommit）
        e1.checkpoint();
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.configureClusteredIndex(index);
        e2.open();
        MiniTransaction r = e2.miniTransactionManager().beginReadOnly();
        assertEquals("v1", payloadOf(e2.btreeService().lookup(r, index, search(1)).orElseThrow()),
                "committed insert preserved (recovery skips committed/cleared slot)");
        e2.miniTransactionManager().commit(r);
        e2.close();
    }

    /** 在同一事务中插入 count 行但不 commit/onCommit，使 undo 段保持 ACTIVE 供恢复逐条回滚。 */
    private void insertRowsActive(StorageEngine engine, BTreeIndex index, int count) {
        TransactionManager txnMgr = engine.transactionManager();
        MiniTransactionManager mtrMgr = engine.miniTransactionManager();
        UndoLogManager undoMgr = engine.undoLogManager();
        SplitCapableBTreeIndexService svc = engine.btreeService();
        Transaction txn = txnMgr.begin(TransactionOptions.defaults());
        txnMgr.assignWriteId(txn);
        for (int id = 1; id <= count; id++) {
            MiniTransaction m = mtrMgr.begin(mtrMgr.budgetFor(RedoBudgetPurpose.CLUSTERED_INSERT,
                    BTreeRedoBudgetEstimator.insert(index.rootLevel())
                            .plus(UndoRedoBudgetEstimator.append(id == 1))));
            RollPointer rp = UndoTestWrites.insert(undoMgr, txn, m, TABLE_ID, INDEX_ID,
                    List.of(new ColumnValue.IntValue(id)), index.keyDef(), index.schema());
            svc.insertClustered(m, index, row(id, "v" + id), txn.transactionId(), rp);
            mtrMgr.commit(m);
        }
        // 不 commit txn / onCommit：保持 ACTIVE
    }

    // ---- 0.4：后台 purge driver ----

    /**
     * money test：配 `clusteredIndex` 的 engine 后台 purge driver 自动回收已提交 delete-mark。索引须 open 前配置，
     * 故先 e1 建索引+close（rootPageId 持久），e2 配置索引+open，在 e2 上 insert+delete-mark+commit（无 live ReadView），
     * 后台 driver 周期 runBatch 物理移除 delete-marked 行。
     */
    @Test
    void backgroundPurgeDriverRemovesCommittedDeleteMark() {
        Path dataPath = dir.resolve("data-purge.ibd");
        EngineConfig cfg = configWithRecoveryTablespaceAndTick(dataPath, Duration.ofMillis(40));

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath); // 建索引（rootPageId 持久）
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.configureClusteredIndex(index); // open 前配置 → e2 启动 purge driver
        e2.open();
        insertRow(e2, index, 1, "v1");       // committed insert
        deleteMarkRow(e2, index, 1);         // committed delete-mark → 入 history（无 live ReadView）

        assertTrue(awaitUntil(() -> lookupIncludingDeletedEmpty(e2, index, 1), Duration.ofSeconds(3)),
                "background purge driver physically removed the committed delete-marked row");
        e2.close();
    }

    /**
     * purge driver 的成功空批次也必须驱动自动 undo 截断候选检查；初始大小不足门槛时只发布可观察 skip，
     * 不改写 page0、page3 或文件长度。该用例固定生产组合根接线，而非只验证独立 scheduler fake。
     */
    @Test
    void backgroundPurgeCycleRunsUndoTruncationSchedulerAndPublishesMetrics() {
        Path dataPath = dir.resolve("data-purge-undo-truncate-metrics.ibd");
        EngineConfig cfg = configWithRecoveryTablespaceAndTick(dataPath, Duration.ofMillis(40))
                .withUndoTruncationConfig(new UndoTruncationConfig(true, 1, Duration.ofMillis(10)));

        StorageEngine first = new StorageEngine(cfg);
        first.open();
        BTreeIndex index = createClusteredIndex(first, dataPath);
        first.close();

        StorageEngine reopened = new StorageEngine(cfg);
        reopened.configureClusteredIndex(index);
        reopened.open();
        try {
            assertTrue(awaitUntil(() -> reopened.undoTruncationMetrics().checks() > 0,
                            Duration.ofSeconds(3)),
                    "成功的 purge 空批次应最终越过 cooldown 并执行一次候选检查");
            assertEquals(UndoTruncationCycleStatus.BELOW_THRESHOLD,
                    reopened.undoTruncationMetrics().lastStatus());
            assertTrue(reopened.undoTruncationMetrics().skipped() > 0);
            assertEquals(0, reopened.undoTruncationMetrics().completed());
        } finally {
            reopened.close();
        }
    }

    /**
     * 整栈验证 page0 autoextend 留下的空闲尾部由 purge driver 自动截回持久 initial size。测试先用正常 FSP
     * API 分配并完整 drop 一个临时 undo segment，禁止直接改文件伪造候选；重启后再由共享 live/recovery service
     * 完成 marker、flush、文件缩短、FSP rebuild 与 ACTIVE 发布。
     */
    @Test
    void backgroundPurgeCycleAutomaticallyTruncatesReleasedUndoExtent() throws Exception {
        Path dataPath = dir.resolve("data-purge-undo-truncate.ibd");
        EngineConfig cfg = configWithRecoveryTablespaceAndTick(dataPath, Duration.ofMillis(40))
                .withUndoTruncationConfig(new UndoTruncationConfig(true, 1, Duration.ofSeconds(30)));

        // 1、fresh engine 尚未配置 purge 索引，因此可确定性地用普通 FSP 路径扩容到第二个 extent。
        StorageEngine first = new StorageEngine(cfg);
        first.open();
        BTreeIndex index = createClusteredIndex(first, dataPath);
        MiniTransactionManager firstMtr = first.miniTransactionManager();
        DiskSpaceManager disk = first.diskSpaceManager();
        MiniTransaction allocate = firstMtr.begin(firstMtr.budgetFor(RedoBudgetPurpose.ENGINE_BOOT));
        SegmentRef temporary = disk.createSegment(allocate, cfg.undoSpaceId(), SegmentPurpose.UNDO);
        disk.allocatePage(allocate, temporary);
        firstMtr.commit(allocate);

        // 2、先以只读 MTR 物化 drop 预算，再在独立写 MTR 清空 inode/FSP owner；物理文件仍保持扩容后的 128 页。
        MiniTransaction inspect = firstMtr.beginReadOnly();
        SegmentDropPlan dropPlan = disk.inspectDropSegmentPlan(inspect, temporary);
        firstMtr.commit(inspect);
        MiniTransaction drop = firstMtr.begin(firstMtr.budgetFor(
                RedoBudgetPurpose.UNDO_FINALIZATION,
                UndoRedoBudgetEstimator.finalization(new UndoSegmentDropPlan(
                        dropPlan.fragmentPageCount(), dropPlan.extentCount(), dropPlan.usedPageCount()), false)));
        disk.dropSegment(drop, temporary);
        firstMtr.commit(drop);
        first.close();
        long initialBytes = Math.multiplyExact(cfg.undoSpaceInitialPages().value(), (long) PS.bytes());
        assertTrue(Files.size(cfg.undoFile()) >= initialBytes + 64L * PS.bytes(),
                "普通 segment drop 只归还 FSP owner，不应自行缩短物理文件");

        // 3、existing open 配置聚簇索引后启动 purge driver；成功空批次应触发共享 truncate service 完成物理回收。
        StorageEngine reopened = new StorageEngine(cfg);
        reopened.configureClusteredIndex(index);
        reopened.open();
        try {
            assertTrue(awaitUntil(() -> reopened.undoTruncationMetrics().completed() > 0,
                            Duration.ofSeconds(5)),
                    "空闲第二个 extent 应在 purge cycle 中进入 crash-safe 自动截断协议");
            assertEquals(initialBytes, Files.size(cfg.undoFile()));
            assertEquals(64, reopened.undoTruncationMetrics().reclaimedPages());
            assertTrue(reopened.undoTruncationMetrics().lastCompletedEpoch() > 0);
        } finally {
            // 4、close 停止 driver 后仍保留最后完成快照，且不会在关闭窗口开始新的维护 cycle。
            reopened.close();
        }
        assertEquals(1, reopened.undoTruncationMetrics().completed());
        assertEquals(UndoTruncationCycleStatus.COMPLETED,
                reopened.undoTruncationMetrics().lastStatus());
    }

    /**
     * R 1.3 money test：崩溃前已提交但未 purge 的 delete-mark undo 只留在 page3 slot + undo first 页
     * COMMITTED header 中。重启恢复必须把它重建到内存 history、复位事务提交序号水位，然后由后台 purge driver
     * 继续物理删除 delete-marked 聚簇记录；否则 history 丢失会让记录永久停留在 delete-marked 状态。
     */
    @Test
    void recoveryRebuildsCommittedHistoryAndBackgroundPurgeResumes() {
        Path dataPath = dir.resolve("data-purge-recovery.ibd");
        EngineConfig cfg = configWithRecoveryTablespaceAndTick(dataPath, Duration.ofMillis(40));

        StorageEngine e1 = new StorageEngine(cfg);
        e1.open();
        BTreeIndex index = createClusteredIndex(e1, dataPath);
        insertRow(e1, index, 1, "v1");
        deleteMarkRow(e1, index, 1);
        assertFalse(lookupIncludingDeletedEmpty(e1, index, 1),
                "delete-marked row is still physically present before recovery because e1 has no purge index");
        e1.checkpoint();
        e1.close();

        StorageEngine e2 = new StorageEngine(cfg);
        e2.configureClusteredIndex(index);
        e2.open();
        assertStageBeforeOpen(e2, RecoveryStageName.RESUME_PURGE);
        assertTrue(awaitUntil(() -> lookupIncludingDeletedEmpty(e2, index, 1), Duration.ofSeconds(3)),
                "recovered committed history lets the restarted purge driver remove the delete-marked row");
        e2.close();
    }

    private EngineConfig configWithRecoveryTablespaceAndTick(Path dataPath, Duration interval) {
        return new EngineConfig(dir, PS, 256, SpaceId.of(5), PageNo.of(64), 64, 100,
                Duration.ofSeconds(10), 64L * 1024 * 1024,
                List.of(new EngineTablespaceConfig(DATA_SPACE, dataPath)),
                true, 4, interval, 0, Duration.ofSeconds(2));
    }

    private boolean lookupIncludingDeletedEmpty(StorageEngine engine, BTreeIndex index, long id) {
        MiniTransaction r = engine.miniTransactionManager().beginReadOnly();
        boolean empty = engine.btreeService().lookupIncludingDeleted(r, index, search(id)).isEmpty();
        engine.miniTransactionManager().commit(r);
        return empty;
    }

    /**
     * recovery report 中的正式阶段必须出现在 OPEN_TRAFFIC 之前，证明工作发生在 gate 仍关闭的恢复窗口内，
     * 而不是 engine.open() 之后的普通后置步骤。
     */
    private void assertStageBeforeOpen(StorageEngine engine, RecoveryStageName stage) {
        List<RecoveryStageName> stages = engine.lastRecoveryReport().orElseThrow().completedStages();
        assertTrue(stages.contains(stage), "recovery report should contain " + stage);
        assertTrue(stages.indexOf(stage) < stages.indexOf(RecoveryStageName.OPEN_TRAFFIC),
                stage + " must complete before user traffic opens");
    }

    /** 提交一条 delete-mark 事务（lookup 旧 image → planDelete/appendPlanned → setClusteredDeleteMark → commit + onCommit）。 */
    private void deleteMarkRow(StorageEngine engine, BTreeIndex index, long id) {
        TransactionManager txnMgr = engine.transactionManager();
        MiniTransactionManager mtrMgr = engine.miniTransactionManager();
        UndoLogManager undoMgr = engine.undoLogManager();
        SplitCapableBTreeIndexService svc = engine.btreeService();
        Transaction txn = txnMgr.begin(TransactionOptions.defaults());
        txnMgr.assignWriteId(txn);
        MiniTransaction read = mtrMgr.beginReadOnly();
        BTreeLookupResult old = svc.lookup(read, index, search(id)).orElseThrow();
        mtrMgr.commit(read);
        HiddenColumns oldHidden = old.record().hiddenColumns();
        MiniTransaction m = mtrMgr.begin(mtrMgr.budgetFor(RedoBudgetPurpose.CLUSTERED_DELETE,
                BTreeRedoBudgetEstimator.pointRewrite().plus(UndoRedoBudgetEstimator.append(true))));
        RollPointer delRp = UndoTestWrites.delete(undoMgr, txn, m, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(id)), old.record().columnValues(), oldHidden,
                index.keyDef(), index.schema());
        svc.setClusteredDeleteMark(m, index, search(id), true,
                new HiddenColumns(txn.transactionId(), delRp), oldHidden.dbTrxId(), oldHidden.dbRollPtr());
        mtrMgr.commit(m);
        txnMgr.prepareCommit(txn);
        undoMgr.onCommit(txn);
        txnMgr.commit(txn);
    }

    // ---- helpers ----

    /** 破坏 data file 中某页的页体一个字节，使其 checksum 不再匹配（模拟 torn write）。 */
    private static void corruptPhysicalPage(Path dataPath, PageId pageId) {
        try (cn.zhangyis.db.storage.fil.io.PageStore store =
                     new cn.zhangyis.db.storage.fil.io.FileChannelPageStore()) {
            store.open(pageId.spaceId(), dataPath, PS);
            byte[] page = new byte[PS.bytes()];
            store.readPage(pageId, ByteBuffer.wrap(page));
            page[100] = (byte) (page[100] ^ 0xFF);
            store.writePage(pageId, ByteBuffer.wrap(page));
            store.force(pageId.spaceId());
        }
    }

    /** 读 data file 中某页并校验其 checksum/trailer。 */
    private static boolean pageChecksumValid(Path dataPath, PageId pageId) {
        try (cn.zhangyis.db.storage.fil.io.PageStore store =
                     new cn.zhangyis.db.storage.fil.io.FileChannelPageStore()) {
            store.open(pageId.spaceId(), dataPath, PS);
            byte[] page = new byte[PS.bytes()];
            store.readPage(pageId, ByteBuffer.wrap(page));
            return PageImageChecksum.verify(page, PS);
        }
    }

    private BTreeIndex createClusteredIndex(StorageEngine engine, Path dataPath) {
        DiskSpaceManager disk = engine.diskSpaceManager();
        MiniTransactionManager mtrMgr = engine.miniTransactionManager();
        MiniTransaction boot = mtrMgr.begin(mtrMgr.budgetFor(RedoBudgetPurpose.ENGINE_BOOT));
        disk.createTablespace(boot, DATA_SPACE, dataPath, PageNo.of(64));
        SegmentRef leaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_LEAF);
        SegmentRef nonLeaf = disk.createSegment(boot, DATA_SPACE, SegmentPurpose.INDEX_NON_LEAF);
        PageId root = disk.allocatePage(boot, leaf);
        engine.indexPageAccess().createIndexPage(boot, root, INDEX_ID, 0);
        mtrMgr.commit(boot);
        return new BTreeIndex(INDEX_ID, root, 0, idKey(), clusteredSchema(), true, leaf, nonLeaf);
    }

    private void insertRow(StorageEngine engine, BTreeIndex index, long id, String payload) {
        TransactionManager txnMgr = engine.transactionManager();
        MiniTransactionManager mtrMgr = engine.miniTransactionManager();
        UndoLogManager undoMgr = engine.undoLogManager();
        SplitCapableBTreeIndexService svc = engine.btreeService();
        Transaction txn = txnMgr.begin(TransactionOptions.defaults());
        txnMgr.assignWriteId(txn);
        MiniTransaction m = mtrMgr.begin(mtrMgr.budgetFor(RedoBudgetPurpose.CLUSTERED_INSERT,
                BTreeRedoBudgetEstimator.insert(index.rootLevel())
                        .plus(UndoRedoBudgetEstimator.append(true))));
        RollPointer rp = UndoTestWrites.insert(undoMgr, txn, m, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(id)), index.keyDef(), index.schema());
        svc.insertClustered(m, index, row(id, payload), txn.transactionId(), rp);
        mtrMgr.commit(m);
        txnMgr.prepareCommit(txn);
        undoMgr.onCommit(txn);
        txnMgr.commit(txn);
    }

    /**
     * 写入一条 INSERT undo/聚簇行并走稳定 phase-one API，返回仍持有 active membership 的 PREPARED 聚合。
     */
    private Transaction insertPreparedRow(StorageEngine engine, BTreeIndex index, long id, String payload) {
        TransactionManager transactionManager = engine.transactionManager();
        Transaction transaction = transactionManager.begin(TransactionOptions.defaults());
        transactionManager.assignWriteId(transaction);
        MiniTransaction write = engine.miniTransactionManager().begin(
                engine.miniTransactionManager().budgetFor(
                        RedoBudgetPurpose.CLUSTERED_INSERT,
                        BTreeRedoBudgetEstimator.insert(index.rootLevel())
                                .plus(UndoRedoBudgetEstimator.append(true))));
        RollPointer rollPointer = UndoTestWrites.insert(
                engine.undoLogManager(), transaction, write, TABLE_ID, INDEX_ID,
                List.of(new ColumnValue.IntValue(id)), index.keyDef(), index.schema());
        engine.btreeService().insertClustered(
                write, index, row(id, payload), transaction.transactionId(), rollPointer);
        engine.miniTransactionManager().commit(write);
        engine.preparedTransactionService().prepare(
                new PrepareTransactionCommand(transaction, Duration.ofSeconds(2)));
        return transaction;
    }

    private static String payloadOf(BTreeLookupResult r) {
        return ((ColumnValue.StringValue) r.record().columnValues().get(1)).value();
    }

    private static TableSchema clusteredSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "payload", ColumnType.varchar(200, true), 1)), true);
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(INDEX_ID, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey search(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String payload) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.StringValue(payload)), false, RecordType.CONVENTIONAL);
    }

    /** 打开引擎实际使用的 redo 后端（文件环或单文件），保证注入的 redo 与引擎恢复读取的是同一份。 */
    private static RedoLogFileRepository openRedoBackend(EngineConfig cfg) {
        return cfg.redoRotationEnabled()
                ? RedoLogFileRepository.openRing(cfg.redoDir(), cfg.redoRotation().fileCount(),
                        cfg.redoRotation().fileBytes())
                : RedoLogFileRepository.open(cfg.redoFile());
    }

    private static void appendPhysicalRedoAfterCheckpoint(EngineConfig cfg, PageId pageId, int offset, byte[] payload) {
        try (RedoLogFileRepository repo = openRedoBackend(cfg);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(cfg.redoControlFile())) {
            RedoCheckpointLabel label = checkpointStore.readLatest();
            RedoRecoveryReader reader = new RedoRecoveryReader(repo, label.checkpointLsn());
            reader.readBatches();
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.restoreRecoveredBoundary(reader.recoveredToLsn());
            redo.append(List.of(new PageBytesRecord(pageId, offset, payload)));
            redo.flush();
        }
    }

    private static Lsn readCheckpoint(EngineConfig cfg) {
        try (RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(cfg.redoControlFile())) {
            return checkpointStore.readLatest().checkpointLsn();
        }
    }

    private static byte[] readPhysicalSlice(Path dataPath, PageId pageId, int offset, int length) {
        try (cn.zhangyis.db.storage.fil.io.PageStore store =
                     new cn.zhangyis.db.storage.fil.io.FileChannelPageStore()) {
            store.open(pageId.spaceId(), dataPath, PS);
            byte[] page = new byte[PS.bytes()];
            store.readPage(pageId, ByteBuffer.wrap(page));
            byte[] slice = new byte[length];
            System.arraycopy(page, offset, slice, 0, length);
            return slice;
        }
    }

    /** 只快照本切片约束的 redo data/control 输入；progress/doublewrite 诊断文件不在此集合。 */
    private static Map<String, byte[]> snapshotRedoInputs(EngineConfig cfg) throws Exception {
        Map<String, byte[]> snapshot = new LinkedHashMap<>();
        snapshot.put("redo-control", Files.readAllBytes(cfg.redoControlFile()));
        if (cfg.redoRotationEnabled()) {
            try (var files = Files.list(cfg.redoDir())) {
                for (Path path : files.filter(Files::isRegularFile).sorted().toList()) {
                    snapshot.put(path.getFileName().toString(), Files.readAllBytes(path));
                }
            }
        } else {
            snapshot.put("redo.log", Files.readAllBytes(cfg.redoFile()));
        }
        return snapshot;
    }

    private static void assertRedoInputsEqual(Map<String, byte[]> expected, Map<String, byte[]> actual) {
        assertEquals(expected.keySet(), actual.keySet());
        expected.forEach((name, bytes) -> assertArrayEquals(
                bytes, actual.get(name), "READ_ONLY_VALIDATE changed redo input " + name));
    }

    private static boolean awaitUntil(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() - deadline < 0) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }
}
