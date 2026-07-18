package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.exception.TablespaceCorruptedException;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleRawCodec;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * GENERAL 表空间 lifecycle 持久化测试。0.16b 要求新建 GENERAL 写 page0 NORMAL marker，
 * 持久 CORRUPTED 后重开能恢复状态，并让普通空间管理 API 通过 registry 准入拒绝损坏空间。
 */
class DiskSpaceManagerGeneralLifecyclePersistenceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path dir;

    /**
     * 验证 {@code generalTablespacePersistsNormalLifecycleOnCreate} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void generalTablespacePersistsNormalLifecycleOnCreate() {
        Path path = dir.resolve("general-normal.ibd");
        SpaceId space = SpaceId.of(70);
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("general-normal-redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, path, PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);
            flushAllDirty(pool, store, redo);

            TablespaceLifecycleHeader lifecycle = readLifecycle(store, space);
            assertEquals(TablespaceState.NORMAL, lifecycle.state());
            assertEquals(PageNo.of(64), lifecycle.initialSizeInPages());
            assertEquals(PageNo.of(64), lifecycle.targetSizeInPages());
            assertEquals(TablespaceState.NORMAL, lifecycle.finishState());
        }
    }

    /**
     * 验证 {@code persistedCorruptedBlocksOrdinaryAccessAfterReopen} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void persistedCorruptedBlocksOrdinaryAccessAfterReopen() {
        Path path = dir.resolve("general-corrupted.ibd");
        SpaceId space = SpaceId.of(71);
        createAndPersistCorrupted(space, path, dir.resolve("general-corrupted-redo.log"));

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            disk.openTablespace(space, path);

            assertEquals(TablespaceState.CORRUPTED, disk.tablespaceState(space));
            MiniTransactionManager mgr = new MiniTransactionManager();
            MiniTransaction mtr = mgr.begin();
            assertThrows(TablespaceCorruptedException.class,
                    () -> disk.createSegment(mtr, space, SegmentPurpose.INDEX_LEAF));
            mgr.rollbackUncommitted(mtr);
        }
    }

    /**
     * 验证 {@code recoveryOpenAllowsPersistedCorruptedTablespace} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void recoveryOpenAllowsPersistedCorruptedTablespace() {
        Path path = dir.resolve("general-corrupted-recovery.ibd");
        SpaceId space = SpaceId.of(72);
        createAndPersistCorrupted(space, path, dir.resolve("general-corrupted-recovery-redo.log"));

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);

            disk.openTablespaceForRecovery(space, path);

            assertEquals(TablespaceState.CORRUPTED, disk.tablespaceState(space));
        }
    }

    /**
     * 验证 {@code legacyGeneralWithoutLifecycleMarkerOpensAsNormal} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     */
    @Test
    void legacyGeneralWithoutLifecycleMarkerOpensAsNormal() {
        Path path = dir.resolve("legacy-general.ibd");
        SpaceId space = SpaceId.of(73);
        createLegacyGeneralWithoutLifecycle(space, path, dir.resolve("legacy-general-redo.log"));

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);

            disk.openTablespace(space, path);

            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(space));
        }
    }

    private void createAndPersistCorrupted(SpaceId space, Path path, Path redoPath) {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, space, path, PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);
            flushAllDirty(pool, store, redo);

            MiniTransaction mark = mgr.begin();
            disk.markTablespaceCorrupted(mark, space, "checksum mismatch");
            mgr.commit(mark);
            flushAllDirty(pool, store, redo);
        }
    }

    /**
     * 构造 0.16b 之前的 GENERAL page0：它有 FSP_HDR 信封和普通 SpaceHeader，但没有 lifecycle marker。
     * 该兼容文件用于验证 loader 不会把 magic=0 的旧 GENERAL 误判成损坏或 UNDO lifecycle。
     */
    private void createLegacyGeneralWithoutLifecycle(SpaceId space, Path path, Path redoPath) {
        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            store.create(space, path, PS, PageNo.of(64));
            RedoLogManager redo = RedoLogManager.durable(repo);
            MiniTransactionManager mgr = new MiniTransactionManager(new TablespaceAccessController(), redo);
            SpaceHeaderRepository headerRepo = new SpaceHeaderRepository(pool);
            ExtentDescriptorRepository xdes = new ExtentDescriptorRepository(pool, PS);

            MiniTransaction boot = mgr.begin();
            headerRepo.initialize(boot, new SpaceHeaderSnapshot(space, PS, TablespaceTypeFlags.encode(TablespaceType.GENERAL),
                    PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                    PageNo.of(2), 0L, 80046, 1L));
            xdes.reserveSystemExtent(boot, space);
            mgr.commit(boot);
            flushAllDirty(pool, store, redo);
        }
    }

    private static TablespaceLifecycleHeader readLifecycle(PageStore store, SpaceId space) {
        ByteBuffer page = ByteBuffer.allocate(PS.bytes());
        store.readPage(PageId.of(space, PageNo.of(0)), page);
        return TablespaceLifecycleRawCodec.read(page).orElseThrow();
    }

    /**
     * 重开类用例必须显式 WAL-safe 刷盘；不依赖 BufferPool.close 的旧副作用。
     */
    private static void flushAllDirty(BufferPool pool, PageStore store, RedoLogManager redo) {
        redo.flush();
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        coordinator.flushList(Lsn.of(Long.MAX_VALUE), pool.capacity());
    }
}
