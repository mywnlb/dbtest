package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteFileRepository;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.flush.doublewrite.DetectOnlyDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.doublewrite.RecoverableDoublewriteStrategy;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.RedoLogCorruptedException;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogFormatException;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaReason;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R2 crash recovery startup 测试：恢复总控只编排 doublewrite repair 与 redo replay，成功后开放 gate，失败时 fail closed。
 */
class CrashRecoveryServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));
    private static final int FIRST_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 64;
    private static final int SECOND_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 128;

    @TempDir
    Path dir;

    /** redo-control 声明的 data format 必须与实际 repository 一致，恢复不得跨格式猜测。 */
    @Test
    void redoControlAndDataFormatMismatchFailsBeforeReplay() {
        Path redoPath = dir.resolve("format-mismatch-redo.log");
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository delegate = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(
                     dir.resolve("format-mismatch-control"))) {
            RedoLogFileRepository mismatched = new VersionedRepositoryView(delegate, 2);
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            CrashRecoveryService service = new CrashRecoveryService(gate);
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, mismatched,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS));

            RecoveryStartupException failure = assertThrows(
                    RecoveryStartupException.class, () -> service.recover(request));

            assertTrue(failure.getCause() instanceof RedoLogFormatException);
            assertEquals(RecoveryState.FAILED, gate.state());
        }
    }

    @Test
    void recoverRepairsDoublewriteBeforeCheckpointAwareRedoReplay() {
        Path redoPath = dir.resolve("redo.log");
        Path controlPath = dir.resolve("redo-control");
        LogRange first;
        LogRange second;

        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            first = redo.append(List.of(
                    new PageInitRecord(PAGE, PageType.INDEX),
                    new PageBytesRecord(PAGE, FIRST_OFFSET, new byte[]{7, 7, 7})));
            second = redo.append(List.of(new PageBytesRecord(PAGE, SECOND_OFFSET, new byte[]{9, 9, 9})));
            redo.flush();
        }

        try (RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            checkpointStore.write(RedoCheckpointLabel.of(first.end(), second.end(), 1_000L));
        }

        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository doublewriteRepo = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            writeDoublewriteCopyAndBrokenDataPage(store, doublewriteRepo, first.end());
            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(doublewriteRepo, store, PS);

            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            CrashRecoveryService service = new CrashRecoveryService(gate);
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                    .withDoublewriteRepair(scanner, List.of(PAGE));

            RecoveryReport report = service.recover(request);

            byte[] recovered = readPage(store);
            assertArrayEquals(new byte[]{7, 7, 7}, slice(recovered, FIRST_OFFSET, 3));
            assertArrayEquals(new byte[]{9, 9, 9}, slice(recovered, SECOND_OFFSET, 3));
            assertEquals(second.end().value(), ByteBuffer.wrap(recovered).getLong(PageEnvelopeLayout.PAGE_LSN));
            assertEquals(RecoveryState.OPEN, gate.state());
            assertEquals(second.end(), report.recoveredToLsn());
            assertEquals(1, report.repairedPageCount());
            assertEquals(1, report.appliedBatchCount());
            assertEquals(List.of(RecoveryStageName.TRAFFIC_CLOSED,
                            RecoveryStageName.DOUBLEWRITE_REPAIR,
                            RecoveryStageName.REDO_REPLAY,
                            RecoveryStageName.OPEN_TRAFFIC),
                    report.completedStages());
        }
    }

    @Test
    void recoverReportsDetectOnlyPagesSeparatelyFromRepairedPages() {
        Path redoPath = dir.resolve("detect-only-redo.log");
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }

        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository doublewriteRepo = DoublewriteFileRepository.open(dir.resolve("detect-only-dw.dat"), PS);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("detect-only-control"))) {
            store.create(SPACE, dir.resolve("detect-only.ibd"), PS, PageNo.of(4));
            writeDetectOnlyMetadataAndBrokenDataPage(store, doublewriteRepo, Lsn.of(10));
            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(doublewriteRepo, store, PS);

            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                    .withDoublewriteRepair(scanner, List.of(PAGE));

            RecoveryReport report = new CrashRecoveryService(new RecoveryTrafficGate()).recover(request);

            assertEquals(0, report.repairedPageCount());
            assertEquals(1, report.detectedOnlyPageCount());
        }
    }

    @Test
    void recoverRecordsProgressForNormalStages() throws Exception {
        Path redoPath = dir.resolve("progress-redo.log");
        LogRange range;
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            range = redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }

        try (PageStore store = new FileChannelPageStore();
            RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("progress-control"))) {
            store.create(SPACE, dir.resolve("progress.ibd"), PS, PageNo.of(4));
            Path progressPath = dir.resolve("progress.jsonl");
            RecoveryProgressJournal journal = RecoveryProgressJournal.persistent(progressPath);
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS));

            RecoveryReport report = new CrashRecoveryService(gate, journal).recover(request);

            List<RecoveryProgressEvent> events = journal.snapshot();
            assertTrue(hasProgress(events, RecoveryProgressEventKind.STARTED,
                    RecoveryStageName.TRAFFIC_CLOSED, RecoveryState.RECOVERING));
            assertTrue(hasProgress(events, RecoveryProgressEventKind.COMPLETED,
                    RecoveryStageName.REDO_REPLAY, RecoveryState.RECOVERING));
            assertTrue(events.stream().anyMatch(event ->
                            event.kind() == RecoveryProgressEventKind.COMPLETED
                                    && event.stageName() == RecoveryStageName.OPEN_TRAFFIC
                                    && event.state() == RecoveryState.OPEN
                                    && event.recoveredToLsn().equals(report.recoveredToLsn())),
                    "OPEN_TRAFFIC completion should publish the final recovered LSN");
            assertEquals(range.end(), report.recoveredToLsn());
            String persisted = Files.readString(progressPath);
            assertTrue(persisted.contains("\"kind\":\"STARTED\""));
            assertTrue(persisted.contains("\"stageName\":\"TRAFFIC_CLOSED\""));
            assertTrue(persisted.contains("\"kind\":\"COMPLETED\""));
            assertTrue(persisted.contains("\"stageName\":\"OPEN_TRAFFIC\""));
        }
    }

    @Test
    void readOnlyValidateReportsRecoverableDoublewriteCopyWithoutRepairingDataPage() {
        Path redoPath = dir.resolve("readonly-dw-redo.log");
        LogRange range;
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            range = redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }

        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository doublewriteRepo = DoublewriteFileRepository.open(dir.resolve("readonly-dw.dat"), PS);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("readonly-dw-control"))) {
            store.create(SPACE, dir.resolve("readonly-dw.ibd"), PS, PageNo.of(4));
            writeDoublewriteCopyAndBrokenDataPage(store, doublewriteRepo, range.end());
            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(doublewriteRepo, store, PS);

            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            RecoveryProgressJournal journal = new RecoveryProgressJournal();
            RecoveryRequest request = RecoveryRequest.readOnlyValidate(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                    .withDoublewriteRepair(scanner, List.of(PAGE));

            RecoveryReport report = new CrashRecoveryService(gate, journal).recover(request);

            byte[] current = readPage(store);
            assertEquals(1, current[FIRST_OFFSET],
                    "READ_ONLY_VALIDATE must report the recoverable copy without writing it back");
            assertEquals(RecoveryMode.READ_ONLY_VALIDATE, report.mode());
            assertEquals(RecoveryState.READ_ONLY, report.state());
            assertEquals(RecoveryState.READ_ONLY, gate.state());
            assertEquals(0, report.repairedPageCount());
            assertEquals(1, report.detectedOnlyPageCount());
            assertEquals(0, report.appliedBatchCount());
            assertEquals(List.of(RecoveryStageName.TRAFFIC_CLOSED,
                            RecoveryStageName.DOUBLEWRITE_REPAIR,
                            RecoveryStageName.REDO_REPLAY,
                            RecoveryStageName.READ_ONLY_DIAGNOSTIC_OPEN),
                    report.completedStages());
            assertTrue(hasProgress(journal.snapshot(), RecoveryProgressEventKind.COMPLETED,
                    RecoveryStageName.READ_ONLY_DIAGNOSTIC_OPEN, RecoveryState.READ_ONLY));
        }
    }

    @Test
    void readOnlyValidateScansRedoWithoutApplyingPageChanges() {
        Path redoPath = dir.resolve("readonly-redo.log");
        LogRange range;
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            range = redo.append(List.of(
                    new PageInitRecord(PAGE, PageType.INDEX),
                    new PageBytesRecord(PAGE, SECOND_OFFSET, new byte[]{4, 5, 6})));
            redo.flush();
        }

        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("readonly-redo-control"))) {
            store.create(SPACE, dir.resolve("readonly-redo.ibd"), PS, PageNo.of(4));
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            RecoveryRequest request = RecoveryRequest.readOnlyValidate(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS));

            RecoveryReport report = new CrashRecoveryService(gate).recover(request);

            byte[] current = readPage(store);
            assertArrayEquals(new byte[]{0, 0, 0}, slice(current, SECOND_OFFSET, 3),
                    "READ_ONLY_VALIDATE must scan redo batches without applying page bytes");
            assertEquals(0L, ByteBuffer.wrap(current).getLong(PageEnvelopeLayout.PAGE_LSN));
            assertEquals(range.end(), report.recoveredToLsn());
            assertEquals(RecoveryMode.READ_ONLY_VALIDATE, report.mode());
            assertEquals(RecoveryState.READ_ONLY, report.state());
            assertEquals(0, report.appliedBatchCount());
        }
    }

    @Test
    void recoveryGateCanEnterReadOnlyDiagnosticAfterFailure() {
        RecoveryTrafficGate gate = new RecoveryTrafficGate();
        gate.failClosed(new DatabaseRuntimeException("synthetic failure"));

        gate.enterReadOnlyDiagnostic();

        assertEquals(RecoveryState.READ_ONLY, gate.state());
        assertTrue(gate.lastFailure().isEmpty(),
                "read-only diagnostic is a successful validation state and must clear stale failure");
    }

    @Test
    void recoverFailsClosedWhenRedoIsCorrupted() throws Exception {
        try (PageStore store = new FileChannelPageStore();
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogFileRepository redoRepo = new CorruptingRepository();
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            Path progressPath = dir.resolve("bad-redo-progress.jsonl");
            RecoveryProgressJournal journal = RecoveryProgressJournal.persistent(progressPath);
            CrashRecoveryService service = new CrashRecoveryService(gate, journal);
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS));

            assertThrows(RecoveryStartupException.class, () -> service.recover(request));
            assertEquals(RecoveryState.FAILED, gate.state());
            assertEquals(RecoveryState.FAILED, service.state());
            assertTrue(journal.snapshot().stream().anyMatch(event ->
                            event.kind() == RecoveryProgressEventKind.FAILED
                                    && event.stageName() == RecoveryStageName.REDO_REPLAY
                                    && event.state() == RecoveryState.FAILED
                                    && !event.detail().isBlank()),
                    "corrupted redo should be visible in recovery progress diagnostics");
            String persisted = Files.readString(progressPath);
            assertTrue(persisted.contains("\"kind\":\"FAILED\""));
            assertTrue(persisted.contains("\"stageName\":\"REDO_REPLAY\""));
        }
    }

    @Test
    void recoverFailsClosedWhenCheckpointIsAheadOfRedo() {
        Path redoPath = dir.resolve("ahead-redo.log");
        Path controlPath = dir.resolve("ahead-control");
        LogRange only;

        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            only = redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }

        try (RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            // checkpoint label 领先于唯一一条完整 redo 批次：模拟 redo 截断或 control 文件与 redo 不匹配。
            Lsn ahead = Lsn.of(only.end().value() + 100);
            checkpointStore.write(RedoCheckpointLabel.of(ahead, ahead, 1_000L));
        }

        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            store.create(SPACE, dir.resolve("s-ahead.ibd"), PS, PageNo.of(4));
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            CrashRecoveryService service = new CrashRecoveryService(gate);
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS));

            // checkpoint 无法被 redo 兑现，恢复必须 fail closed，不开放用户流量。
            assertThrows(RecoveryStartupException.class, () -> service.recover(request));
            assertEquals(RecoveryState.FAILED, gate.state());
            assertEquals(RecoveryState.FAILED, service.state());
        }
    }

    /** undo TRUNCATING 续作必须位于 redo replay 之后、开放流量之前，并接收完整 recoveredTo 边界。 */
    @Test
    void resumesUndoTablespaceAfterRedoBeforeOpeningTraffic() {
        Path redoPath = dir.resolve("undo-resume-redo.log");
        LogRange range;
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            range = redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("undo-resume-control"))) {
            store.create(SPACE, dir.resolve("undo-resume.ibd"), PS, PageNo.of(4));
            AtomicReference<Lsn> resumedAt = new AtomicReference<>();
            UndoTablespaceRecoveryParticipant participant = new UndoTablespaceRecoveryParticipant() {
                @Override
                public int prepareDoublewrite(DoublewriteRecoveryScanner scanner) {
                    return 0;
                }

                @Override
                public boolean shouldRepairDoublewritePage(PageId pageId) {
                    return true;
                }

                @Override
                public void resumeAfterRedo(Lsn recoveredToLsn) {
                    resumedAt.set(recoveredToLsn);
                }
            };
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                    .withUndoTablespaceRecovery(participant);

            RecoveryReport report = new CrashRecoveryService(new RecoveryTrafficGate()).recover(request);

            assertEquals(range.end(), resumedAt.get());
            assertEquals(List.of(RecoveryStageName.TRAFFIC_CLOSED,
                            RecoveryStageName.DOUBLEWRITE_REPAIR,
                            RecoveryStageName.REDO_REPLAY,
                            RecoveryStageName.UNDO_TABLESPACE_RESUME,
                            RecoveryStageName.OPEN_TRAFFIC),
                    report.completedStages());
        }
    }

    /**
     * 正式事务恢复阶段必须仍处于 recovery gate 关闭窗口内：redo replay 完成后先执行 recovered active rollback
     * 与 committed history 重建，再开放普通流量。这里用 fake participant 验证 CrashRecoveryService 的编排边界，
     * 真实 undo slot 扫描由 StorageEngine 端到端用例覆盖。
     */
    @Test
    void runsTransactionUndoRecoveryStagesBeforeOpeningTraffic() {
        Path redoPath = dir.resolve("trx-recovery-redo.log");
        LogRange range;
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            range = redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("trx-recovery-control"))) {
            store.create(SPACE, dir.resolve("trx-recovery.ibd"), PS, PageNo.of(4));
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            AtomicReference<RecoveryState> gateStateDuringStage = new AtomicReference<>();
            AtomicReference<Lsn> recoveredBoundarySeen = new AtomicReference<>();
            AtomicReference<RecoveredTransactionSnapshot> transactionSnapshotSeen = new AtomicReference<>();
            TransactionUndoRecoveryParticipant participant = (recoveredToLsn, transactionSnapshot) -> {
                gateStateDuringStage.set(gate.state());
                recoveredBoundarySeen.set(recoveredToLsn);
                transactionSnapshotSeen.set(transactionSnapshot);
                return new TransactionUndoRecoveryResult(3, 1, 0, 2);
            };
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                    .withTransactionRecovery(
                            TransactionRecoveryContext.using(TransactionRecoveryCheckpointSource.empty()),
                            participant);

            RecoveryReport report = new CrashRecoveryService(gate).recover(request);

            assertEquals(RecoveryState.RECOVERING, gateStateDuringStage.get(),
                    "transaction undo recovery must run before user traffic opens");
            assertEquals(range.end(), recoveredBoundarySeen.get());
            assertEquals(TransactionId.of(1), transactionSnapshotSeen.get().nextTransactionId());
            assertEquals(List.of(RecoveryStageName.TRAFFIC_CLOSED,
                            RecoveryStageName.DOUBLEWRITE_REPAIR,
                            RecoveryStageName.REDO_REPLAY,
                            RecoveryStageName.UNDO_ROLLBACK,
                            RecoveryStageName.RESUME_PURGE,
                            RecoveryStageName.OPEN_TRAFFIC),
                    report.completedStages());
        }
    }

    /** 事务恢复阶段失败必须和 redo/doublewrite 失败一样 fail-closed，不能开放普通流量。 */
    @Test
    void recoverFailsClosedWhenTransactionUndoRecoveryFails() {
        Path redoPath = dir.resolve("trx-recovery-fail-redo.log");
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("trx-recovery-fail-control"))) {
            store.create(SPACE, dir.resolve("trx-recovery-fail.ibd"), PS, PageNo.of(4));
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                    .withTransactionRecovery(
                            TransactionRecoveryContext.using(TransactionRecoveryCheckpointSource.empty()),
                            (recoveredToLsn, transactionSnapshot) -> {
                                throw new DatabaseRuntimeException("synthetic transaction recovery failure");
                            });

            assertThrows(RecoveryStartupException.class, () -> new CrashRecoveryService(gate).recover(request));
            assertEquals(RecoveryState.FAILED, gate.state());
        }
    }

    /** checkpoint 基线与 redo delta 必须由同一 context 汇合，再以不可变 snapshot 交给事务 undo 恢复。 */
    @Test
    void transactionRecoveryContextCollectsRedoBeforeUndoParticipant() {
        Path redoPath = dir.resolve("formal-trx-redo.log");
        TransactionStateDeltaRecord committed = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.ACTIVE,
                TransactionStateDeltaState.COMMITTED, TransactionNo.of(3),
                TransactionStateDeltaReason.COMMIT);
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            redo.append(List.of(committed));
            redo.flush();
        }

        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("formal-trx-redo-control"));
             TransactionRecoveryCheckpointStore transactionStore =
                     TransactionRecoveryCheckpointStore.open(dir.resolve("formal-trx-control"))) {
            TransactionRecoveryContext context = TransactionRecoveryContext.using(transactionStore);
            AtomicReference<RecoveredTransactionSnapshot> seen = new AtomicReference<>();
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(),
                            new RedoApplyContext(store, PS))
                    .withTransactionRecovery(context, (recoveredToLsn, snapshot) -> {
                        seen.set(snapshot);
                        return new TransactionUndoRecoveryResult(0, 0, 0, 0);
                    });

            new CrashRecoveryService(new RecoveryTrafficGate()).recover(request);

            RecoveredTransactionEntry entry = seen.get().entry(TransactionId.of(7)).orElseThrow();
            assertEquals(RecoveredTransactionState.COMMITTED, entry.state());
            assertEquals(TransactionNo.of(3), entry.transactionNo());
            assertEquals(TransactionId.of(8), seen.get().nextTransactionId());
            assertEquals(TransactionNo.of(4), seen.get().nextTransactionNo());
        }
    }

    /** READ_ONLY_VALIDATE 不能跳过事务 sidecar 完整性诊断；非零 checkpoint 缺基线时同样 fail closed。 */
    @Test
    void readOnlyValidateRejectsMissingTransactionSidecarWithoutApplyingRedo() {
        Path redoPath = dir.resolve("readonly-missing-trx-redo.log");
        LogRange range;
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            range = redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore =
                     RedoCheckpointStore.open(dir.resolve("readonly-missing-trx-redo-control"))) {
            store.create(SPACE, dir.resolve("readonly-missing-trx.ibd"), PS, PageNo.of(4));
            checkpointStore.write(RedoCheckpointLabel.of(range.end(), range.end(), 1_000L));
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            TransactionRecoveryContext context = TransactionRecoveryContext.using(
                    TransactionRecoveryCheckpointSource.empty());
            RecoveryRequest request = RecoveryRequest.readOnlyValidate(
                            checkpointStore, redoRepo, RedoApplyDispatcher.pageDispatcher(),
                            new RedoApplyContext(store, PS))
                    .withTransactionRecoveryValidation(context);

            assertThrows(RecoveryStartupException.class,
                    () -> new CrashRecoveryService(gate).recover(request));
            assertEquals(RecoveryState.FAILED, gate.state());
        }
    }

    /** READ_ONLY_VALIDATE 必须扫描 trx delta 并报告 PREPARED，而不是因为不 apply page redo 就漏诊断。 */
    @Test
    void readOnlyValidateRejectsPreparedTransactionDelta() {
        Path redoPath = dir.resolve("readonly-prepared-redo.log");
        TransactionStateDeltaRecord prepared = new TransactionStateDeltaRecord(
                TransactionId.of(7), TransactionStateDeltaState.ACTIVE,
                TransactionStateDeltaState.PREPARED, TransactionNo.NONE,
                TransactionStateDeltaReason.COMMIT);
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            redo.append(List.of(prepared));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore =
                     RedoCheckpointStore.open(dir.resolve("readonly-prepared-redo-control"))) {
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            TransactionRecoveryContext context = TransactionRecoveryContext.using(
                    TransactionRecoveryCheckpointSource.empty());
            RecoveryRequest request = RecoveryRequest.readOnlyValidate(
                            checkpointStore, redoRepo, RedoApplyDispatcher.pageDispatcher(),
                            new RedoApplyContext(store, PS))
                    .withTransactionRecoveryValidation(context);

            assertThrows(RecoveryStartupException.class,
                    () -> new CrashRecoveryService(gate).recover(request));
            assertEquals(RecoveryState.FAILED, gate.state());
        }
    }

    /** sidecar 领先 redo label 时，完整 redo 尾必须覆盖 sidecar LSN；中间日志丢失不能靠高估 counter 掩盖。 */
    @Test
    void recoveryRejectsRedoTailThatDoesNotCoverNewerTransactionSidecar() {
        Path redoPath = dir.resolve("sidecar-ahead-redo.log");
        LogRange durableRange;
        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            durableRange = redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore =
                     RedoCheckpointStore.open(dir.resolve("sidecar-ahead-redo-control"));
             TransactionRecoveryCheckpointStore transactionStore =
                     TransactionRecoveryCheckpointStore.open(dir.resolve("sidecar-ahead-trx-control"))) {
            store.create(SPACE, dir.resolve("sidecar-ahead.ibd"), PS, PageNo.of(4));
            transactionStore.write(new TransactionRecoveryCheckpoint(
                    Lsn.of(durableRange.end().value() + 1), TransactionId.of(8), TransactionNo.of(4)));
            TransactionRecoveryContext context = TransactionRecoveryContext.using(transactionStore);
            RecoveryRequest request = RecoveryRequest.normal(
                            checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(context.deltaSink()),
                            new RedoApplyContext(store, PS))
                    .withTransactionRecovery(context,
                            (recoveredToLsn, snapshot) -> new TransactionUndoRecoveryResult(0, 0, 0, 0));
            RecoveryTrafficGate gate = new RecoveryTrafficGate();

            assertThrows(RecoveryStartupException.class,
                    () -> new CrashRecoveryService(gate).recover(request));
            assertEquals(RecoveryState.FAILED, gate.state());
        }
    }

    /** public canonical request 也不能把 formal context 与 no-op dispatcher 组合，绕过安全 fluent API。 */
    @Test
    void formalTransactionContextRejectsUnboundDispatcher() {
        Path redoPath = dir.resolve("unbound-context-redo.log");
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore =
                     RedoCheckpointStore.open(dir.resolve("unbound-context-redo-control"))) {
            TransactionRecoveryContext context = TransactionRecoveryContext.using(
                    TransactionRecoveryCheckpointSource.empty());
            TransactionUndoRecoveryParticipant participant = (recoveredToLsn, snapshot) ->
                    new TransactionUndoRecoveryResult(0, 0, 0, 0);

            assertThrows(DatabaseValidationException.class, () -> new RecoveryRequest(
                    RecoveryMode.NORMAL, checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS),
                    null, List.of(), null, List.of(), null, context, participant,
                    RecoverySkipPolicy.none()));
        }
    }

    private void writeDoublewriteCopyAndBrokenDataPage(PageStore store,
                                                       DoublewriteFileRepository doublewriteRepo,
                                                       Lsn pageLsn) {
        byte[] image = new byte[PS.bytes()];
        ByteBuffer page = ByteBuffer.wrap(image);
        page.putInt(PageEnvelopeLayout.SPACE_ID, SPACE.value());
        page.putInt(PageEnvelopeLayout.PAGE_NO, (int) PAGE.pageNo().value());
        page.putLong(PageEnvelopeLayout.PAGE_LSN, pageLsn.value());
        page.putInt(PageEnvelopeLayout.PAGE_TYPE, PageType.INDEX.code());
        image[FIRST_OFFSET] = 7;
        image[FIRST_OFFSET + 1] = 7;
        image[FIRST_OFFSET + 2] = 7;
        PageImageChecksum.stamp(image, PS);
        new RecoverableDoublewriteStrategy(doublewriteRepo)
                .beforeDataFileWrite(new FlushPageSnapshot(PAGE, pageLsn, 1, image));

        byte[] broken = image.clone();
        broken[FIRST_OFFSET] = 1;
        store.writePage(PAGE, ByteBuffer.wrap(broken));
        store.force(SPACE);
    }

    private void writeDetectOnlyMetadataAndBrokenDataPage(PageStore store,
                                                          DoublewriteFileRepository doublewriteRepo,
                                                          Lsn pageLsn) {
        byte[] image = new byte[PS.bytes()];
        ByteBuffer page = ByteBuffer.wrap(image);
        page.putInt(PageEnvelopeLayout.SPACE_ID, SPACE.value());
        page.putInt(PageEnvelopeLayout.PAGE_NO, (int) PAGE.pageNo().value());
        page.putLong(PageEnvelopeLayout.PAGE_LSN, pageLsn.value());
        page.putInt(PageEnvelopeLayout.PAGE_TYPE, PageType.INDEX.code());
        image[FIRST_OFFSET] = 7;
        PageImageChecksum.stamp(image, PS);
        FlushPageSnapshot snapshot = new FlushPageSnapshot(PAGE, pageLsn, 1, image);
        DetectOnlyDoublewriteStrategy strategy = new DetectOnlyDoublewriteStrategy(doublewriteRepo);
        strategy.beforeDataFileWrite(snapshot);
        strategy.afterDataFileWrite(snapshot);

        byte[] broken = image.clone();
        broken[FIRST_OFFSET] = 1;
        store.writePage(PAGE, ByteBuffer.wrap(broken));
        store.force(SPACE);
    }

    private byte[] readPage(PageStore store) {
        byte[] page = new byte[PS.bytes()];
        store.readPage(PAGE, ByteBuffer.wrap(page));
        return page;
    }

    private static byte[] slice(byte[] bytes, int offset, int length) {
        byte[] out = new byte[length];
        System.arraycopy(bytes, offset, out, 0, length);
        return out;
    }

    private static boolean hasProgress(List<RecoveryProgressEvent> events,
                                       RecoveryProgressEventKind kind,
                                       RecoveryStageName stageName,
                                       RecoveryState state) {
        return events.stream().anyMatch(event ->
                event.kind() == kind && event.stageName() == stageName && event.state() == state);
    }

    /** 在不伪造物理文件的前提下，模拟 repository 报告不同 redo data format。 */
    private static final class VersionedRepositoryView implements RedoLogFileRepository {

        private final RedoLogFileRepository delegate;
        private final int formatVersion;

        private VersionedRepositoryView(RedoLogFileRepository delegate, int formatVersion) {
            this.delegate = delegate;
            this.formatVersion = formatVersion;
        }

        @Override
        public int formatVersion() {
            return formatVersion;
        }

        @Override
        public void append(RedoLogBatch batch) {
            delegate.append(batch);
        }

        @Override
        public void force() {
            delegate.force();
        }

        @Override
        public List<RedoLogBatch> readBatches() {
            return delegate.readBatches();
        }

        @Override
        public void close() {
            // delegate 由测试自己的 try-with-resources 释放，view 不拥有其生命周期。
        }
    }

    /** 模拟 repository 已在物理扫描中识别出非 torn-tail 的致命损坏。 */
    private static final class CorruptingRepository implements RedoLogFileRepository {

        @Override
        public void append(RedoLogBatch batch) {
            throw new AssertionError("recovery must not append before redo scan succeeds");
        }

        @Override
        public void force() {
            throw new AssertionError("recovery must not force before redo scan succeeds");
        }

        @Override
        public List<RedoLogBatch> readBatches() {
            throw new RedoLogCorruptedException("synthetic semantic corruption");
        }

        @Override
        public void close() {
            // 内存 stub 无资源。
        }
    }
}
