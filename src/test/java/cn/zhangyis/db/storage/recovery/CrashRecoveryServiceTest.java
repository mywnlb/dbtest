package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteFileRepository;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
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
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
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
    void recoverFailsClosedWhenRedoIsCorrupted() throws Exception {
        Path redoPath = dir.resolve("bad-redo.log");
        Files.write(redoPath, new byte[]{0x12, 0x34, 0x56, 0x78, 0, 0, 0, 1, 0, 0, 0, 0});

        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            CrashRecoveryService service = new CrashRecoveryService(gate);
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS));

            assertThrows(RecoveryStartupException.class, () -> service.recover(request));
            assertEquals(RecoveryState.FAILED, gate.state());
            assertEquals(RecoveryState.FAILED, service.state());
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
}
