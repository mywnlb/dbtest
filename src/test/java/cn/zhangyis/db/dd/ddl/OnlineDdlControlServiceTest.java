package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.domain.DdlId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Online DDL只读投影、prepare handoff与public durable cancel语义测试。 */
class OnlineDdlControlServiceTest {

    @TempDir
    Path directory;

    /** 高频进度更新与snapshot并发时单字段计数不能倒退，完成记录进入有界history。 */
    @Test
    void publishesMonotonicSnapshotsAndBoundedTerminalHistory() throws Exception {
        OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(2);
        OnlineDdlOperationTracker tracker = registry.register(
                identity(1), DdlExecutionProtocol.ONLINE_INDEX_V1);
        tracker.advanceRuntime(OnlineDdlRuntimePhase.ACTIVATING, OnlineDdlWaitReason.NONE);
        tracker.updateGate(OnlineDdlTablePhase.CAPTURING, 3, 2, 19);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> writer = executor.submit(() -> {
                for (int batch = 1; batch <= 1_000; batch++) {
                    tracker.addScanBatch(5, Optional.of("key-" + batch));
                    tracker.updateChangeLog(batch * 2L, batch * 64L, 1_000_000,
                            4_096, batch * 3L, batch * 3L - 1, 1);
                }
            });
            Future<?> reader = executor.submit(() -> {
                long rows = 0;
                long batches = 0;
                while (!writer.isDone()) {
                    OnlineDdlOperationSnapshot snapshot = tracker.snapshot();
                    assertTrue(snapshot.rowsScanned() >= rows);
                    assertTrue(snapshot.batchesScanned() >= batches);
                    assertTrue(snapshot.highestForcedSequence()
                            <= snapshot.highestAppendedSequence());
                    rows = snapshot.rowsScanned();
                    batches = snapshot.batchesScanned();
                }
            });
            writer.get();
            reader.get();
        }

        OnlineDdlOperationSnapshot completed = registry.complete(
                DdlId.of(1), OnlineDdlTerminalResult.COMPLETED,
                Optional.empty(), false);
        assertEquals(5_000, completed.rowsScanned());
        assertEquals(1_000, completed.batchesScanned());
        assertTrue(registry.find(DdlId.of(1)).isPresent());

        for (long ddlId = 2; ddlId <= 4; ddlId++) {
            registry.register(identity(ddlId), DdlExecutionProtocol.ONLINE_INDEX_V1);
            registry.complete(DdlId.of(ddlId), OnlineDdlTerminalResult.COMPLETED,
                    Optional.empty(), false);
        }
        assertTrue(registry.find(DdlId.of(1)).isEmpty());
        assertEquals(2, registry.list().size());
    }

    /** cancel先赢prepare handoff时不能写marker；coordinator进入PREPARING后cancel必须等待durable结果再CAS。 */
    @Test
    void arbitratesPrePrepareCancellationWithoutLosingDurability() throws Exception {
        Path catalog = directory.resolve("prepare-handoff-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var logs = new cn.zhangyis.db.dd.repo.PersistentDdlLogRepository(store);
            OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(8);
            OnlineDdlControlService service = new OnlineDdlControlService(logs, registry);

            OnlineDdlOperationTracker cancelled = registry.register(
                    identity(11), DdlExecutionProtocol.ONLINE_INDEX_V1);
            OnlineDdlCancelResult before = service.requestCancel(
                    DdlId.of(11), OnlineDdlCancelRequest.admin(
                            DdlCancellationReason.USER_REQUEST, 7), Duration.ofSeconds(2));
            assertEquals(OnlineDdlCancelOutcome.ACCEPTED_BEFORE_PREPARE, before.outcome());
            assertFalse(cancelled.beginDurablePrepare());
            assertTrue(logs.find(DdlId.of(11)).isEmpty());

            OnlineDdlOperationTracker preparing = registry.register(
                    identity(12), DdlExecutionProtocol.ONLINE_INDEX_V1);
            assertTrue(preparing.beginDurablePrepare());
            Future<OnlineDdlCancelResult> waiting = executor.submit(() -> service.requestCancel(
                    DdlId.of(12), OnlineDdlCancelRequest.admin(
                            DdlCancellationReason.USER_REQUEST, 8), Duration.ofSeconds(2)));
            DdlLogRecord prepared = onlinePrepared(12);
            logs.prepare(prepared);
            preparing.markDurablePrepared(prepared);

            assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE, waiting.get().outcome());
            assertEquals(DdlControlState.CANCEL_REQUESTED,
                    logs.find(DdlId.of(12)).orElseThrow().controlState());
        }
    }

    /** facade必须区分幂等取消、forward-only、terminal、not-found与无权限，且list可显示durable-only marker。 */
    @Test
    void returnsStablePublicCancelOutcomesAndDurableOnlyViews() {
        Path catalog = directory.resolve("control-outcomes-mysql.ibd");
        try (FileInternalCatalogStore store = FileInternalCatalogStore.openOrCreate(catalog)) {
            var logs = new cn.zhangyis.db.dd.repo.PersistentDdlLogRepository(store);
            OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(8);
            AtomicInteger durableWakeups = new AtomicInteger();
            OnlineDdlControlService service = new OnlineDdlControlService(
                    logs, registry, identity -> durableWakeups.incrementAndGet());

            OnlineDdlOperationTracker live = registry.register(
                    identity(21), DdlExecutionProtocol.ONLINE_INDEX_V1);
            assertTrue(live.beginDurablePrepare());
            DdlLogRecord livePrepared = onlinePrepared(21);
            logs.prepare(livePrepared);
            live.markDurablePrepared(livePrepared);
            OnlineDdlCancelRequest request = OnlineDdlCancelRequest.admin(
                    DdlCancellationReason.SESSION_KILLED, 91);
            assertEquals(OnlineDdlCancelOutcome.ACCEPTED_DURABLE,
                    service.requestCancel(DdlId.of(21), request, Duration.ofSeconds(1)).outcome());
            assertEquals(OnlineDdlCancelOutcome.ALREADY_REQUESTED,
                    service.requestCancel(DdlId.of(21), request, Duration.ofSeconds(1)).outcome());
            assertEquals(2, durableWakeups.get());

            logs.prepare(onlinePrepared(22));
            logs.compareAndSetControl(DdlId.of(22), DdlLogPhase.PREPARED,
                    DdlControlState.OPEN, DdlControlState.FORWARD_ONLY, Optional.empty());
            assertEquals(OnlineDdlCancelOutcome.TOO_LATE_FORWARD_ONLY,
                    service.requestCancel(DdlId.of(22), request, Duration.ofSeconds(1)).outcome());
            assertEquals(2, durableWakeups.get());

            logs.prepare(onlinePrepared(23));
            logs.transition(DdlId.of(23), DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            assertEquals(OnlineDdlCancelOutcome.TERMINAL,
                    service.requestCancel(DdlId.of(23), request, Duration.ofSeconds(1)).outcome());
            assertEquals(OnlineDdlCancelOutcome.NOT_FOUND,
                    service.requestCancel(DdlId.of(24), request, Duration.ofSeconds(1)).outcome());
            assertThrows(OnlineDdlControlAccessException.class, () -> service.requestCancel(
                    DdlId.of(22), OnlineDdlCancelRequest.unprivileged(
                            DdlCancellationReason.USER_REQUEST, 3), Duration.ofSeconds(1)));

            assertEquals(3, service.list().size());
            assertTrue(service.find(DdlId.of(22)).orElseThrow().cancelCapable());
            assertEquals(DdlControlState.FORWARD_ONLY,
                    service.find(DdlId.of(22)).orElseThrow().controlState());
        }
    }

    /** awaitTerminal使用有界Condition等待，完成发布后返回同一history snapshot，超时不伪造终态。 */
    @Test
    void awaitsTerminalWithBoundedCondition() throws Exception {
        OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(4);
        registry.register(identity(31), DdlExecutionProtocol.ONLINE_INDEX_V1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Optional<OnlineDdlOperationSnapshot>> waiting = executor.submit(
                    () -> registry.awaitTerminal(DdlId.of(31), Duration.ofSeconds(2)));
            registry.complete(DdlId.of(31), OnlineDdlTerminalResult.ROLLED_BACK,
                    Optional.of("USER_CANCEL"), false);
            assertEquals(OnlineDdlTerminalResult.ROLLED_BACK,
                    waiting.get().orElseThrow().terminalResult());
        }
        registry.register(identity(32), DdlExecutionProtocol.ONLINE_INDEX_V1);
        assertTrue(registry.awaitTerminal(
                DdlId.of(32), Duration.ofMillis(5)).isEmpty());
    }

    /** 超大控制面等待应饱和到Condition支持的上界，不能因Duration纳秒换算溢出而拒绝合法正时限。 */
    @Test
    void acceptsPositiveTimeoutLargerThanLongNanos() {
        OnlineDdlOperationRegistry registry = new OnlineDdlOperationRegistry(4);
        registry.register(identity(33), DdlExecutionProtocol.ONLINE_INDEX_V1);
        registry.complete(DdlId.of(33), OnlineDdlTerminalResult.COMPLETED,
                Optional.empty(), false);

        assertEquals(OnlineDdlTerminalResult.COMPLETED,
                registry.awaitTerminal(DdlId.of(33), Duration.ofSeconds(Long.MAX_VALUE))
                        .orElseThrow()
                        .terminalResult());
    }

    private OnlineDdlOperationIdentity identity(long ddlId) {
        return new OnlineDdlOperationIdentity(
                DdlId.of(ddlId), DdlLogOperation.CREATE_INDEX, 41, 8,
                "app.orders", "idx_value", 12, 13, 77, false,
                OptionalLong.empty());
    }

    private DdlLogRecord onlinePrepared(long ddlId) {
        return new DdlLogRecord(
                new DdlUndoMarker(ddlId, 13, 41), 8,
                DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                SpaceId.of(3000), directory.resolve("table-" + ddlId + ".ibd"),
                Optional.of(directory.resolve("table-" + ddlId + ".rowlog")), Optional.empty(),
                DdlExecutionProtocol.ONLINE_INDEX_V1,
                Optional.of(digest(1)), Optional.empty(), Optional.of(digest(2)),
                DdlControlState.OPEN, Optional.empty(), Optional.empty());
    }

    private static DdlSchemaDigest digest(int seed) {
        byte[] bytes = new byte[32];
        java.util.Arrays.fill(bytes, (byte) seed);
        return new DdlSchemaDigest(DdlDigestAlgorithm.SHA_256,
                DdlSchemaCanonicalFormat.TABLE_SCHEMA_V1, bytes);
    }
}
