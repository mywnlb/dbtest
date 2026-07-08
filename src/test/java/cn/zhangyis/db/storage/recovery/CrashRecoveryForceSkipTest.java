package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteFileRepository;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
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
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FORCE_SKIP_CORRUPT_TABLESPACE 端到端恢复测试。被跳过表空间在 PageStore 中故意不打开；
 * 若 doublewrite、redo 或 reconcile 任一阶段绕过 skip 过滤访问它，测试会因未打开句柄失败。
 */
class CrashRecoveryForceSkipTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId ACTIVE_SPACE = SpaceId.of(1);
    private static final SpaceId SKIPPED_SPACE = SpaceId.of(7);
    private static final PageId ACTIVE_PAGE = PageId.of(ACTIVE_SPACE, PageNo.of(2));
    private static final PageId SKIPPED_PAGE = PageId.of(SKIPPED_SPACE, PageNo.of(2));
    private static final int PAYLOAD_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 48;

    @TempDir
    Path dir;

    /**
     * force-skip 模式只恢复未跳过表空间：doublewrite 页列表、redo 记录和 reconcile 空间集都会先过滤，
     * 最终报告保留各阶段跳过计数，帮助管理员确认哪些坏表空间被强制隔离。
     */
    @Test
    void forceSkipFiltersAllRecoveryStagesAndReportsSkippedDiagnostics() {
        Path redoPath = dir.resolve("redo.log");
        Path controlPath = dir.resolve("redo-control");
        byte[] payload = new byte[]{8, 6, 4};
        LogRange range;

        try (RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            range = redo.append(List.of(
                    new PageInitRecord(SKIPPED_PAGE, PageType.INDEX),
                    new PageBytesRecord(SKIPPED_PAGE, PAYLOAD_OFFSET, new byte[]{1, 2, 3}),
                    new PageInitRecord(ACTIVE_PAGE, PageType.INDEX),
                    new PageBytesRecord(ACTIVE_PAGE, PAYLOAD_OFFSET, payload)));
            redo.flush();
        }
        try (RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            checkpointStore.write(RedoCheckpointLabel.of(Lsn.of(0), range.end(), 1_000L));
        }

        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository doublewriteRepo = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            store.create(ACTIVE_SPACE, dir.resolve("active.ibd"), PS, PageNo.of(2));
            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(doublewriteRepo, store, PS);
            RecoveryRequest request = RecoveryRequest.forceSkip(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS),
                            Set.of(SKIPPED_SPACE))
                    .withDoublewriteRepair(scanner, List.of(SKIPPED_PAGE))
                    .withSpaceFileReconcile(List.of(SKIPPED_SPACE));

            RecoveryReport report = new CrashRecoveryService(new RecoveryTrafficGate()).recover(request);

            byte[] active = readPage(store, ACTIVE_PAGE);
            assertEquals(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE, report.mode());
            assertEquals(RecoveryState.OPEN, report.state());
            assertEquals(Set.of(SKIPPED_SPACE), report.skippedSpaces());
            assertEquals(1, report.skippedDoublewritePageCount());
            assertEquals(2, report.skippedRedoRecordCount());
            assertEquals(1, report.skippedReconcileSpaceCount());
            assertEquals(1, report.appliedBatchCount());
            assertEquals(range.end().value(), ByteBuffer.wrap(active).getLong(PageEnvelopeLayout.PAGE_LSN));
            assertArrayEquals(payload, slice(active, PAYLOAD_OFFSET, payload.length));
            assertEquals(List.of(RecoveryStageName.TRAFFIC_CLOSED,
                            RecoveryStageName.DOUBLEWRITE_REPAIR,
                            RecoveryStageName.REDO_REPLAY,
                            RecoveryStageName.SPACE_FILE_RECONCILE,
                            RecoveryStageName.OPEN_TRAFFIC),
                    report.completedStages());
        }
    }

    private static byte[] readPage(PageStore store, PageId pageId) {
        byte[] page = new byte[PS.bytes()];
        store.readPage(pageId, ByteBuffer.wrap(page));
        return page;
    }

    private static byte[] slice(byte[] bytes, int offset, int length) {
        byte[] out = new byte[length];
        System.arraycopy(bytes, offset, out, 0, length);
        return out;
    }
}
