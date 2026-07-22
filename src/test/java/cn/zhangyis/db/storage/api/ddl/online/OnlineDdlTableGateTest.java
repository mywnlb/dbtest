package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.fil.online.FileOnlineIndexChangeLog;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Online DDL table gate 状态机测试。测试显式区分短 admission、持续到事务终态的 table 引用和 row-log I/O lease。
 */
class OnlineDdlTableGateTest {

    @TempDir
    Path directory;

    /** 正Duration即使大于long纳秒范围也必须饱和；不能在进入gate前泄漏ArithmeticException。 */
    @Test
    void shouldAcceptPositiveTimeoutLargerThanLongNanos() {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        OnlineIndexBuildId buildId = OnlineIndexBuildId.of(9);

        gate.beginActivation(22, buildId, Duration.ofSeconds(Long.MAX_VALUE));

        assertEquals(OnlineDdlTablePhase.ACTIVATING, gate.phase(22));
        gate.beginAbort(buildId, OnlineDdlAbortReason.CANCELLED);
        gate.clearBuild(buildId);
    }

    /** gate snapshot一次锁内复制phase/ref/lease/high-water，且不会泄露内部table/transaction集合。 */
    @Test
    void shouldSnapshotOneTableWithoutCombiningIndependentGetters() {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        TransactionId transactionId = TransactionId.of(100);
        OnlineDmlAdmission admission = gate.admit(transactionId, 22, Duration.ofSeconds(1));

        OnlineDdlGateSnapshot active = gate.snapshot(22);
        assertEquals(OnlineDdlTablePhase.ABSENT, active.phase());
        assertTrue(active.buildId().isEmpty());
        assertEquals(1, active.inFlightAdmissions());
        assertEquals(1, active.activeTransactions());

        admission.close();
        gate.completeCommit(transactionId, Lsn.of(69));
        gate.beginActivation(22, OnlineIndexBuildId.of(10), Duration.ofSeconds(1));
        OnlineDdlGateSnapshot activating = gate.snapshot(22);
        assertEquals(OnlineDdlTablePhase.ACTIVATING, activating.phase());
        assertEquals(OnlineIndexBuildId.of(10), activating.buildId().orElseThrow());
        assertEquals(0, activating.inFlightAdmissions());
        assertEquals(0, activating.ioLeases());
        assertEquals(0, activating.activeTransactions());
        assertEquals(Lsn.of(69), activating.terminalRedoHighWater());
    }

    /** build 尚未出现时的普通 DML 也必须登记 affected table，activation 只能在旧事务终态后完成。 */
    @Test
    void shouldTrackTransactionsBeforeBuildActivation() throws Exception {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        TransactionId transactionId = TransactionId.of(101);
        try (OnlineDmlAdmission ignored = gate.admit(transactionId, 22, Duration.ofSeconds(1));
             ExecutorService executor = Executors.newSingleThreadExecutor()) {
            // admission close 仅表示没有页级 DML 正在运行，不代表事务已经提交或回滚。
        }

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            CountDownLatch started = new CountDownLatch(1);
            Future<?> activation = executor.submit(() -> {
                started.countDown();
                gate.beginActivation(22, OnlineIndexBuildId.of(11), Duration.ofSeconds(2));
            });
            assertTrue(started.await(1, TimeUnit.SECONDS));
            Thread.sleep(80);
            assertFalse(activation.isDone());

            gate.completeRollback(transactionId);
            activation.get(1, TimeUnit.SECONDS);
        }

        assertEquals(OnlineDdlTablePhase.ACTIVATING, gate.phase(22));
    }

    /** CAPTURING admission 固定同一 target；append sequence 归属事务，commit force 后发布 redo high-water。 */
    @Test
    void shouldCaptureAndForceTransactionHighWater() {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        try (FileOnlineIndexChangeLog log = createLog(11)) {
            OnlineIndexCaptureTarget target = target(log, 11, 22);
            gate.beginActivation(22, target.buildId(), Duration.ofSeconds(1));
            gate.publishCapture(target);

            TransactionId transactionId = TransactionId.of(102);
            long sequence;
            try (OnlineDmlAdmission admission = gate.admit(transactionId, 22, Duration.ofSeconds(1))) {
                assertSame(target, admission.captureTarget().orElseThrow());
                sequence = admission.appendCandidate(new byte[]{1, 2, 3});
            }

            assertEquals(sequence, gate.candidateHighWater(transactionId, target.buildId()));
            gate.forceTransactionCandidates(transactionId, Duration.ofSeconds(1));
            assertTrue(log.highestForcedSequence() >= sequence);

            gate.completeCommit(transactionId, Lsn.of(70));
            assertEquals(Lsn.of(70), gate.terminalRedoHighWater(22));
        }
    }

    /** seal 必须同时排空 in-flight admission 和尚未进入 terminal 状态的事务引用。 */
    @Test
    void shouldSealOnlyAfterAdmissionAndTransactionDrain() throws Exception {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        try (FileOnlineIndexChangeLog log = createLog(12);
             ExecutorService executor = Executors.newSingleThreadExecutor()) {
            OnlineIndexCaptureTarget target = target(log, 12, 22);
            gate.beginActivation(22, target.buildId(), Duration.ofSeconds(1));
            gate.publishCapture(target);
            TransactionId transactionId = TransactionId.of(103);
            OnlineDmlAdmission admission = gate.admit(transactionId, 22, Duration.ofSeconds(1));

            Future<?> seal = executor.submit(() -> gate.beginSeal(target.buildId(), Duration.ofSeconds(2)));
            Thread.sleep(80);
            assertFalse(seal.isDone());
            admission.close();
            Thread.sleep(80);
            assertFalse(seal.isDone());

            gate.completeCommit(transactionId, Lsn.of(71));
            seal.get(1, TimeUnit.SECONDS);
            assertEquals(OnlineDdlTablePhase.SEALED, gate.phase(22));
        }
    }

    /** Online DROP在prepare期只声明单表owner而不捕获row-log，finalization仍必须排空跨界事务。 */
    @Test
    void shouldFinalizeRetirementWithoutCreatingCaptureTarget() throws Exception {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        OnlineIndexBuildId operationId = OnlineIndexBuildId.of(120);
        gate.beginRetirement(22, operationId, Duration.ofSeconds(1));
        assertEquals(OnlineDdlTablePhase.RETIREMENT_OPEN, gate.phase(22));

        TransactionId transactionId = TransactionId.of(1200);
        try (OnlineDmlAdmission admission = gate.admit(
                transactionId, 22, Duration.ofSeconds(1))) {
            assertTrue(admission.captureTarget().isEmpty(),
                    "DROP prepare must let source DML maintain the old index through normal metadata");
        }

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<?> finalization = executor.submit(() ->
                    gate.beginSeal(operationId, Duration.ofSeconds(2)));
            Thread.sleep(80);
            assertFalse(finalization.isDone());
            gate.completeCommit(transactionId, Lsn.of(712));
            finalization.get(1, TimeUnit.SECONDS);
        }

        assertEquals(OnlineDdlTablePhase.SEALED, gate.phase(22));
        gate.clearBuild(operationId);
        assertEquals(OnlineDdlTablePhase.ABSENT, gate.phase(22));
    }

    /** ABORTING 作废 candidate durability requirement；日志关闭后旧事务提交不得再次访问该文件。 */
    @Test
    void shouldInvalidateCandidateRequirementWhenAborting() {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        FileOnlineIndexChangeLog log = createLog(13);
        OnlineIndexCaptureTarget target = target(log, 13, 22);
        gate.beginActivation(22, target.buildId(), Duration.ofSeconds(1));
        gate.publishCapture(target);
        TransactionId transactionId = TransactionId.of(104);
        try (OnlineDmlAdmission admission = gate.admit(transactionId, 22, Duration.ofSeconds(1))) {
            assertTrue(admission.appendCandidate(new byte[]{4}) > 0);
        }

        gate.beginAbort(target.buildId(), OnlineDdlAbortReason.CANCELLED);
        log.close();

        gate.forceTransactionCandidates(transactionId, Duration.ofSeconds(1));
        gate.completeCommit(transactionId, Lsn.of(72));
        assertEquals(0, gate.candidateHighWater(transactionId, target.buildId()));
    }

    /** 一个表处于 activation freeze 时，不相关表的 admission 不能被全局大锁串行化。 */
    @Test
    void shouldKeepDifferentTablesIndependent() throws Exception {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        TransactionId old = TransactionId.of(105);
        try (OnlineDmlAdmission ignored = gate.admit(old, 22, Duration.ofSeconds(1))) {
            // 登记 table 22 的旧事务。
        }
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<?> activation = executor.submit(() ->
                    gate.beginActivation(22, OnlineIndexBuildId.of(14), Duration.ofSeconds(2)));
            Thread.sleep(80);

            try (OnlineDmlAdmission admission = gate.admit(
                    TransactionId.of(106), 23, Duration.ofMillis(300))) {
                assertTrue(admission.captureTarget().isEmpty());
            }
            gate.completeRollback(TransactionId.of(106));
            gate.completeRollback(old);
            activation.get(1, TimeUnit.SECONDS);
        }
    }

    /** activation 等待必须有界；超时不能把半完成 build 留在 gate 中阻塞后续 DML。 */
    @Test
    void shouldRestoreAbsentStateAfterActivationTimeout() {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        TransactionId transactionId = TransactionId.of(107);
        try (OnlineDmlAdmission ignored = gate.admit(transactionId, 22, Duration.ofSeconds(1))) {
            // 关闭短 admission，但保留 active transaction 引用制造 timeout。
        }

        assertThrows(DatabaseRuntimeException.class, () -> gate.beginActivation(
                22, OnlineIndexBuildId.of(15), Duration.ofMillis(30)));
        assertEquals(OnlineDdlTablePhase.ABSENT, gate.phase(22));
        gate.completeRollback(transactionId);
    }

    /** ABORTING 必须等待已经开始的文件 append 退出，但不必等待不再访问 staged tree/log 的聚簇 DML 完成。 */
    @Test
    void shouldDrainRowLogIoBeforeAbortCleanup() throws Exception {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        BlockingChangeLog log = new BlockingChangeLog();
        OnlineIndexCaptureTarget target = new OnlineIndexCaptureTarget(
                OnlineIndexBuildId.of(16), 22, 33, log, new GateCandidateCodec());
        gate.beginActivation(22, target.buildId(), Duration.ofSeconds(1));
        gate.publishCapture(target);
        CountDownLatch finishDml = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> dml = executor.submit(() -> {
                try (OnlineDmlAdmission admission = gate.admit(
                        TransactionId.of(108), 22, Duration.ofSeconds(1))) {
                    admission.appendCandidate(new byte[]{1});
                    finishDml.await();
                }
                return null;
            });
            assertTrue(log.appendStarted.await(1, TimeUnit.SECONDS));
            gate.beginAbort(target.buildId(), OnlineDdlAbortReason.CANCELLED);
            Future<?> drain = executor.submit(() -> gate.awaitAbortQuiescence(
                    target.buildId(), Duration.ofSeconds(2)));
            Thread.sleep(80);
            assertFalse(drain.isDone());

            log.allowAppend.countDown();
            drain.get(1, TimeUnit.SECONDS);
            gate.clearBuild(target.buildId());
            assertEquals(OnlineDdlTablePhase.ABSENT, gate.phase(22));

            finishDml.countDown();
            dml.get(1, TimeUnit.SECONDS);
            gate.completeRollback(TransactionId.of(108));
        }
    }

    /** SEALING 等待期间的容量 abort 必须保留 ABORTING，不能被 seal 醒来后覆盖成 SEALED。 */
    @Test
    void shouldNotOverwriteCapacityAbortWhileSealing() throws Exception {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        BlockingChangeLog log = new BlockingChangeLog(true);
        OnlineIndexCaptureTarget target = new OnlineIndexCaptureTarget(
                OnlineIndexBuildId.of(16), 22, 33, log, new GateCandidateCodec());
        gate.beginActivation(22, target.buildId(), Duration.ofSeconds(1));
        gate.publishCapture(target);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> dml = executor.submit(() -> {
                try (OnlineDmlAdmission admission = gate.admit(
                        TransactionId.of(109), 22, Duration.ofSeconds(1))) {
                    admission.appendCandidate(new byte[]{1});
                }
                return null;
            });
            assertTrue(log.appendStarted.await(1, TimeUnit.SECONDS));
            Future<?> seal = executor.submit(() -> gate.beginSeal(
                    target.buildId(), Duration.ofSeconds(2)));
            Thread.sleep(80);
            log.allowAppend.countDown();
            dml.get(1, TimeUnit.SECONDS);

            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> seal.get(1, TimeUnit.SECONDS));
            assertEquals(OnlineDdlTablePhase.ABORTING, gate.phase(22));
            gate.completeRollback(TransactionId.of(109));
            gate.awaitAbortQuiescence(target.buildId(), Duration.ofSeconds(1));
            gate.clearBuild(target.buildId());
        }
    }

    /** admission 在 CAPTURING 取得、append 在 SEALING 完成时，事务仍必须登记并 force 自己的 candidate。 */
    @Test
    void shouldRetainSuccessfulCandidateThatFinishesDuringSealing() throws Exception {
        OnlineDdlTableGate gate = new OnlineDdlTableGate();
        BlockingChangeLog log = new BlockingChangeLog();
        OnlineIndexCaptureTarget target = new OnlineIndexCaptureTarget(
                OnlineIndexBuildId.of(16), 22, 33, log, new GateCandidateCodec());
        gate.beginActivation(22, target.buildId(), Duration.ofSeconds(1));
        gate.publishCapture(target);
        TransactionId transactionId = TransactionId.of(110);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> dml = executor.submit(() -> {
                try (OnlineDmlAdmission admission = gate.admit(
                        transactionId, 22, Duration.ofSeconds(1))) {
                    assertEquals(1, admission.appendCandidate(new byte[]{1}));
                }
                return null;
            });
            assertTrue(log.appendStarted.await(1, TimeUnit.SECONDS));
            Future<?> seal = executor.submit(() -> gate.beginSeal(
                    target.buildId(), Duration.ofSeconds(2)));
            awaitPhase(gate, 22, OnlineDdlTablePhase.SEALING);

            log.allowAppend.countDown();
            dml.get(1, TimeUnit.SECONDS);
            assertEquals(1, gate.candidateHighWater(transactionId, target.buildId()));

            gate.forceTransactionCandidates(transactionId, Duration.ofSeconds(1));
            assertEquals(1, log.forceCalls.get());
            gate.completeCommit(transactionId, Lsn.of(73));
            seal.get(1, TimeUnit.SECONDS);
            assertEquals(OnlineDdlTablePhase.SEALED, gate.phase(22));
        }
    }

    /** 等待并发状态转换的测试辅助；使用有界轮询避免依赖线程调度时序。 */
    private static void awaitPhase(OnlineDdlTableGate gate, long tableId,
                                   OnlineDdlTablePhase expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (gate.phase(tableId) != expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, gate.phase(tableId));
    }

    private FileOnlineIndexChangeLog createLog(long buildId) {
        FileOnlineIndexChangeLog log = FileOnlineIndexChangeLog.create(
                directory.resolve("online-index-" + buildId + ".log"),
                new OnlineIndexLogHeader(OnlineIndexBuildId.of(buildId), 22, 33,
                        44, 45, 7, new byte[]{1, 2, 3}),
                64 * 1024, 4096);
        log.appendState(OnlineIndexLogRecordType.GENERATION_STARTED, new byte[0]);
        log.appendState(OnlineIndexLogRecordType.CAPTURING, new byte[0]);
        return log;
    }

    private static OnlineIndexCaptureTarget target(
            OnlineIndexChangeLog changeLog, long buildId, long tableId) {
        return new OnlineIndexCaptureTarget(OnlineIndexBuildId.of(buildId), tableId, 33,
                changeLog, new GateCandidateCodec());
    }

    /** gate 测试直接传 opaque payload，不在此重复验证 record codec。 */
    private static final class GateCandidateCodec implements OnlineIndexCandidateCodec {
        @Override
        public Optional<byte[]> encodeInsert(LogicalRecord after) {
            return Optional.empty();
        }

        @Override
        public Optional<byte[]> encodeUpdate(LogicalRecord before, LogicalRecord after) {
            return Optional.empty();
        }

        @Override
        public Optional<byte[]> encodeDelete(LogicalRecord before) {
            return Optional.empty();
        }

        @Override
        public OnlineIndexCandidate decode(byte[] payload) {
            throw new UnsupportedOperationException("gate test does not decode candidate payload");
        }
    }

    /** 用 latch 把 append 固定在 gate 外部 I/O lease 中，验证 abort drain 的真实等待边界。 */
    private static final class BlockingChangeLog implements OnlineIndexChangeLog {
        private final CountDownLatch appendStarted = new CountDownLatch(1);
        private final CountDownLatch allowAppend = new CountDownLatch(1);
        private final AtomicInteger forceCalls = new AtomicInteger();
        private final boolean abortAfterAppend;

        private BlockingChangeLog() {
            this(false);
        }

        private BlockingChangeLog(boolean abortAfterAppend) {
            this.abortAfterAppend = abortAfterAppend;
        }

        @Override
        public OnlineIndexLogHeader header() {
            return new OnlineIndexLogHeader(OnlineIndexBuildId.of(16), 22, 33,
                    44, 45, 7, new byte[]{1});
        }

        @Override
        public Path path() {
            return Path.of("online-index-16.log").toAbsolutePath();
        }

        @Override
        public long appendCandidate(TransactionId transactionId, byte[] payload) {
            appendStarted.countDown();
            try {
                if (!allowAppend.await(2, TimeUnit.SECONDS)) {
                    throw new DatabaseRuntimeException("blocking row-log test append timed out");
                }
                return abortAfterAppend ? 0 : 1;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new DatabaseRuntimeException("blocking row-log test append interrupted", interrupted);
            }
        }

        @Override public long appendState(OnlineIndexLogRecordType type, byte[] payload) { return 2; }
        @Override public void forceThrough(long sequence, Duration timeout) { forceCalls.incrementAndGet(); }
        @Override public void markAbortRequired(OnlineDdlAbortReason reason, Duration timeout) { }
        @Override public boolean abortRequired() { return abortAfterAppend; }
        @Override public long highestAppendedSequence() { return 1; }
        @Override public long highestForcedSequence() { return 0; }
        @Override public long sizeBytes() { return 0; }
        @Override public OnlineChangeLogSnapshot snapshot() {
            return new OnlineChangeLogSnapshot(1, 0, 0, 1_024, 256,
                    1, 0, abortAfterAppend, false, false,
                    true, false, false);
        }
        @Override public List<OnlineIndexLogRecord> readAll() { return List.of(); }
        @Override public void resetToManifest(Duration timeout) { }
        @Override public void close() { }
    }
}
