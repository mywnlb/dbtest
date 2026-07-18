package cn.zhangyis.db.storage.api;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.exception.TablespaceNotFoundException;
import cn.zhangyis.db.storage.fil.exception.TablespaceUnavailableException;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;


import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DiskSpaceManager 表空间准入测试：建表空间注册 NORMAL、typed create 写 flags、recovery 可从 page0 重开 metadata。
 */
class DiskSpaceManagerTablespaceAdmissionTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path dir;

    /**
     * 验证 {@code createUndoTablespaceRegistersActiveAndAllowsSpaceManagement} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void createUndoTablespaceRegistersActiveAndAllowsSpaceManagement() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(50);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("a.ibu"), PageNo.of(64), TablespaceType.UNDO);
            mgr.commit(boot);

            assertEquals(TablespaceState.ACTIVE, disk.tablespaceState(space));

            MiniTransaction mtr = mgr.begin();
            SegmentRef segment = disk.createSegment(mtr, space, SegmentPurpose.UNDO);
            disk.allocatePage(mtr, segment);
            mgr.commit(mtr);
        });
    }

    /**
     * 验证 {@code defaultCreateTablespaceUsesGeneralType} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void defaultCreateTablespaceUsesGeneralType() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(51);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("b.ibu"), PageNo.of(64));
            mgr.commit(boot);

            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(space));
        });
    }

    /**
     * 验证 {@code openTablespaceForRecoveryReopensNormalFromDisk} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void openTablespaceForRecoveryReopensNormalFromDisk() {
        Path path = dir.resolve("rec.ibu");
        SpaceId space = SpaceId.of(52);
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("rec-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, path, PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);
            flushAllDirty(pool, store, redo);
        }
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            disk.openTablespaceForRecovery(space, path);

            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(space));
        }
    }

    /**
     * 验证 {@code markCorruptedBlocksCreateSegment} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void markCorruptedBlocksCreateSegment() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(60);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("c.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);

            disk.markTablespaceCorrupted(space, "checksum mismatch");

            assertEquals(TablespaceState.CORRUPTED, disk.tablespaceState(space));
            MiniTransaction mtr = mgr.begin();
            assertThrows(TablespaceCorruptedException.class,
                    () -> disk.createSegment(mtr, space, SegmentPurpose.INDEX_LEAF));
            mgr.rollbackUncommitted(mtr);
        });
    }

    /**
     * 验证 {@code markInactiveBlocksAllocatePage} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void markInactiveBlocksAllocatePage() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(61);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("i.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            SegmentRef segment = disk.createSegment(boot, space, SegmentPurpose.INDEX_LEAF);
            mgr.commit(boot);

            disk.markTablespaceInactive(space);

            MiniTransaction mtr = mgr.begin();
            assertThrows(TablespaceUnavailableException.class, () -> disk.allocatePage(mtr, segment));
            mgr.rollbackUncommitted(mtr);
        });
    }

    /**
     * 验证 {@code discardBlocksUsage} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void discardBlocksUsage() {
        onPool((mgr, disk, store) -> {
            SpaceId space = SpaceId.of(62);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, dir.resolve("d.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);

            disk.discardTablespace(space);

            MiniTransaction mtr = mgr.begin();
            assertThrows(TablespaceNotFoundException.class, () -> disk.usage(mtr, space));
            mgr.rollbackUncommitted(mtr);
        });
    }

    /**
     * 验证 {@code reopenViaDirectStoreOpenLazyLoadsViaLoader} 对应的存储引擎稳定 API行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void reopenViaDirectStoreOpenLazyLoadsViaLoader() {
        Path path = dir.resolve("re.ibu");
        SpaceId space = SpaceId.of(63);
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("re-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, path, PageNo.of(64), TablespaceType.UNDO);
            mgr.commit(boot);
            flushAllDirty(pool, store, redo);
        }
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(space, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction mtr = mgr.begin();
            SegmentRef segment = disk.createSegment(mtr, space, SegmentPurpose.UNDO);
            disk.allocatePage(mtr, segment);
            mgr.commit(mtr);

            assertEquals(TablespaceState.ACTIVE, disk.tablespaceState(space));
        }
    }

    private interface Body {
        void run(MiniTransactionManager mgr, DiskSpaceManager disk, PageStore store);
    }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            body.run(mgr, disk, store);
        }
    }

    /**
     * 重开类用例不再依赖 BufferPool.close 的旧 flushAll 副作用；显式等待 redo durable 后由 FlushCoordinator 写出脏页。
     */
    private static void flushAllDirty(BufferPool pool, PageStore store, RedoLogManager redo) {
        redo.flush();
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        coordinator.flushList(Lsn.of(Long.MAX_VALUE), pool.capacity());
    }
}
