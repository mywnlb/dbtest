package cn.zhangyis.db.storage.recovery;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
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
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * autoextend crash-safety 集成测试（SPACE_FILE_RECONCILE 切片验收）。
 *
 * <p>场景：{@code allocatePage} 触发整 extent 的 autoextend（物理文件 64→128 页，只分配新 extent 首页，其余为尾部零页）。
 * 用 durable redo 写入并 flush 该批次后，把数据文件 truncate 回 64 页，模拟 autoExtend 未 fsync 在崩溃后丢失的 extent。
 * 恢复必须：(1) extend-on-demand 在 replay 期重建被截掉、但有 PAGE_INIT redo 的新 extent 首页；
 * (2) SPACE_FILE_RECONCILE 据 page0 权威大小把物理文件补回 128 页，覆盖 extent 内无 redo 描述的尾部零页。
 */
class SpaceFileReconcileRecoveryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    /** 初始大小恰为 1 个 extent（16KB 页=64 页），保证首次 autoextend 按整 extent 而非单页增长，从而产生尾部零页。 */
    private static final long INITIAL_PAGES = 64;

    @TempDir
    Path dir;

    /**
     * 验证 {@code recoveryReconcilesFileLengthToPage0AfterLostAutoextendExtent} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void recoveryReconcilesFileLengthToPage0AfterLostAutoextendExtent() {
        Path dataPath = dir.resolve("space.ibd");
        Path redoPath = dir.resolve("redo.log");
        Path controlPath = dir.resolve("redo-control");

        PageId allocatedPageId = null;
        long preExtendPages = 0;
        long postExtendPages = 0;
        Lsn checkpointLsn = null;
        Lsn finalLsn = null;

        // Phase A：真实 DiskSpaceManager + durable-redo MTR 跑到首次 autoextend，捕获该自增批次。
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 256);
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(redoRepo);
            MiniTransactionManager mgr =
                    new MiniTransactionManager(new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, SPACE, dataPath, PageNo.of(INITIAL_PAGES), TablespaceType.GENERAL);
            SegmentRef segment = disk.createSegment(boot, SPACE, SegmentPurpose.INDEX_LEAF);
            mgr.commit(boot);

            long before = store.currentSizeInPages(SPACE).value();
            assertEquals(INITIAL_PAGES, before, "tablespace must start at exactly one extent");

            for (int i = 0; i < 4096; i++) {
                flushAllDirty(pool, store, redo);
                Lsn beforeThisAllocate = mgr.redoLogManager().currentLsn();
                MiniTransaction mtr = mgr.begin();
                PageId p = disk.allocatePage(mtr, segment);
                mgr.commit(mtr);
                long after = store.currentSizeInPages(SPACE).value();
                if (after > before) {
                    // 这一次分配触发了 autoextend：page0 大小更新 + 新页 PAGE_INIT 同处一个 MTR 批次。
                    checkpointLsn = beforeThisAllocate;
                    allocatedPageId = p;
                    preExtendPages = before;
                    postExtendPages = after;
                    redo.flush();
                    finalLsn = mgr.redoLogManager().currentLsn();
                    break;
                }
                before = after;
            }
            if (allocatedPageId == null) {
                fail("autoextend was never triggered within the allocation budget");
            }
        }

        assertEquals(INITIAL_PAGES, preExtendPages, "autoextend must fire at the 1-extent boundary");
        assertTrue(postExtendPages > preExtendPages + 1,
                "extent-granularity autoextend must add more than one page (trailing zero pages exist)");
        assertTrue(allocatedPageId.pageNo().value() >= preExtendPages,
                "the autoextending allocation must return a page inside the new extent");

        // Phase B：写 checkpoint label（指向自增批次之前），供恢复从该处重放。
        try (RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            checkpointStore.write(RedoCheckpointLabel.of(checkpointLsn, finalLsn, 1_000L));
        }

        // Phase C：truncate 数据文件回 64 页模拟崩溃丢失 extent，再恢复。
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(controlPath)) {
            store.open(SPACE, dataPath, PS);
            store.truncate(SPACE, PageNo.of(preExtendPages));
            assertEquals(preExtendPages, store.currentSizeInPages(SPACE).value(),
                    "crash simulation must leave the file physically short");

            RecoveryReport report = new CrashRecoveryService(new RecoveryTrafficGate())
                    .recover(RecoveryRequest.normal(checkpointStore, redoRepo,
                                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                            .withSpaceFileReconcile(List.of(SPACE)));

            // (1) reconcile 把物理文件补回 page0 权威大小。
            assertEquals(postExtendPages, store.currentSizeInPages(SPACE).value(),
                    "SPACE_FILE_RECONCILE must regrow the file to page0 currentSizeInPages");

            // (2) extend-on-demand 在 replay 期重建被截掉的新 extent 首页（有 PAGE_INIT redo）。
            byte[] allocated = new byte[PS.bytes()];
            store.readPage(allocatedPageId, ByteBuffer.wrap(allocated));
            assertEquals(PageType.ALLOCATED.code(),
                    ByteBuffer.wrap(allocated).getInt(PageEnvelopeLayout.PAGE_TYPE),
                    "extend-on-demand must let replay recreate the allocated page beyond the truncated EOF");

            // (3) 尾部零页（无 redo 描述）被 reconcile 补齐，可读不越界。
            byte[] trailing = new byte[PS.bytes()];
            store.readPage(PageId.of(SPACE, PageNo.of(postExtendPages - 1)), ByteBuffer.wrap(trailing));
            assertEquals(0, trailing[100], "reconciled trailing page must be zero-filled and readable");

            // (4) 阶段顺序：SPACE_FILE_RECONCILE 位于 REDO_REPLAY 之后、OPEN_TRAFFIC 之前。
            assertEquals(List.of(RecoveryStageName.TRAFFIC_CLOSED,
                            RecoveryStageName.DOUBLEWRITE_REPAIR,
                            RecoveryStageName.REDO_REPLAY,
                            RecoveryStageName.SPACE_FILE_RECONCILE,
                            RecoveryStageName.OPEN_TRAFFIC),
                    report.completedStages());
        }
    }

    /**
     * 为 checkpoint 候选 LSN 建立真实数据页前像：先 fsync redo，再让 FlushCoordinator 写出当前 dirty view。
     */
    private static void flushAllDirty(BufferPool pool, PageStore store, RedoLogManager redo) {
        redo.flush();
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        coordinator.flushList(Lsn.of(Long.MAX_VALUE), pool.capacity());
    }
}
