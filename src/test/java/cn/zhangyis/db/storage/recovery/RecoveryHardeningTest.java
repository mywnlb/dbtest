package cn.zhangyis.db.storage.recovery;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 恢复加固测试：
 * <ul>
 *   <li>F4：提供 recoveredRedoManager 时，REDO_BOUNDARY_INSTALL 把恢复边界装到它，新 append 从 recoveredTo 续写；</li>
 *   <li>F5：reconcile 前校验 page0 自描述身份，spaceId 不一致即判损坏 fail closed，不据损坏 header 扩展文件。</li>
 * </ul>
 */
class RecoveryHardeningTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void installsRecoveredRedoBoundaryWhenManagerProvided() {
        Path redoPath = dir.resolve("redo.log");
        Path controlPath = dir.resolve("control");
        LogRange first;
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            first = redo.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            redo.flush();
        }
        try (RedoCheckpointStore cp = RedoCheckpointStore.open(controlPath)) {
            cp.write(RedoCheckpointLabel.of(first.start(), first.end(), 1_000L));
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore cp = RedoCheckpointStore.open(controlPath)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            RedoLogManager recovered = RedoLogManager.durable(repo);

            RecoveryReport report = new CrashRecoveryService(new RecoveryTrafficGate())
                    .recover(RecoveryRequest.normal(cp, repo,
                                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                            .withRedoBoundaryInstall(recovered));

            assertEquals(first.end(), recovered.currentLsn(), "新 redo 管理器必须从 recoveredTo 续写");
            assertTrue(report.completedStages().contains(RecoveryStageName.REDO_BOUNDARY_INSTALL));
            assertTrue(report.completedStages().indexOf(RecoveryStageName.REDO_BOUNDARY_INSTALL)
                    < report.completedStages().indexOf(RecoveryStageName.OPEN_TRAFFIC));
        }
    }

    @Test
    void reconcileRejectsPage0WithMismatchedSpaceId() {
        Path data = dir.resolve("space5.ibd");
        Path redoPath = dir.resolve("redo5.log");
        Path controlPath = dir.resolve("control5");
        SpaceId real = SpaceId.of(5);
        SpaceId wrong = SpaceId.of(1);

        // 用真实 page0 建空间 5（page0 自描述 spaceId=5），关闭刷盘。
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 64)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, real, data, PageNo.of(8), TablespaceType.GENERAL);
            mgr.commit(boot);
        }
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            // 空 redo：恢复只走 reconcile 校验路径。
        }
        try (RedoCheckpointStore cp = RedoCheckpointStore.open(controlPath)) {
            cp.write(RedoCheckpointLabel.of(Lsn.of(0), Lsn.of(0), 1_000L));
        }
        // 把空间 5 的文件按错误 SpaceId=1 打开后 reconcile([1]) → page0.spaceId=5 ≠ 1 → 损坏 → fail closed，不扩展文件。
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore cp = RedoCheckpointStore.open(controlPath)) {
            store.open(wrong, data, PS);
            RecoveryRequest req = RecoveryRequest.normal(cp, repo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                    .withSpaceFileReconcile(List.of(wrong));

            assertThrows(RecoveryStartupException.class,
                    () -> new CrashRecoveryService(new RecoveryTrafficGate()).recover(req));
        }
    }
}
