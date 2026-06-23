package cn.zhangyis.db.storage.api.undotruncate;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags;


import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.tablespace.PageZeroTablespaceMetadataLoader;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.meta.CachingTablespaceRegistry;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.recovery.CrashRecoveryService;
import cn.zhangyis.db.storage.recovery.RecoveryRequest;
import cn.zhangyis.db.storage.recovery.RecoveryState;
import cn.zhangyis.db.storage.recovery.RecoveryTrafficGate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** undo 表空间截断端到端测试：空 inode 前置条件、物理收缩/FSP 重建、不可逆故障后的幂等续作。 */
class UndoTablespaceTruncationServiceTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(90);

    @TempDir
    Path dir;

    @Test
    void truncatesEmptyUndoSpaceBackToInitialExtentAndPublishesActive() {
        try (Fixture fixture = new Fixture(dir.resolve("success"))) {
            fixture.growUndoSpace();

            UndoTablespaceTruncationResult result = fixture.service(UndoTruncationFaultInjector.none())
                    .truncate(SPACE, TablespaceState.ACTIVE);

            assertEquals(PageNo.of(64), fixture.store.currentSizeInPages(SPACE));
            assertEquals(TablespaceState.ACTIVE, fixture.disk.tablespaceState(SPACE));
            assertEquals(1L, result.truncateEpoch());
            assertTrue(result.markerLsn().value() > 0);

            MiniTransaction read = fixture.mtrManager.begin();
            SpaceHeaderSnapshot header = fixture.headerRepo.read(read, SPACE);
            TablespaceLifecycleHeader lifecycle = fixture.headerRepo.readLifecycle(read, SPACE).orElseThrow();
            fixture.mtrManager.commit(read);
            assertEquals(PageNo.of(64), header.currentSizeInPages());
            assertEquals(PageNo.of(0), header.freeLimitPageNo());
            assertEquals(TablespaceState.ACTIVE, lifecycle.state());
            assertEquals(1L, lifecycle.truncateEpoch());

            MiniTransaction allocate = fixture.mtrManager.begin();
            SegmentRef segment = fixture.disk.createSegment(allocate, SPACE, SegmentPurpose.UNDO);
            assertTrue(fixture.disk.allocatePage(allocate, segment).pageNo().value() >= 4);
            fixture.mtrManager.commit(allocate);
        }
    }

    @Test
    void rejectsTruncationWhileAnyUndoInodeSlotIsAllocated() {
        try (Fixture fixture = new Fixture(dir.resolve("not-empty"))) {
            fixture.growUndoSpace();
            MiniTransaction allocate = fixture.mtrManager.begin();
            fixture.disk.createSegment(allocate, SPACE, SegmentPurpose.UNDO);
            fixture.mtrManager.commit(allocate);

            assertThrows(UndoTablespaceNotEmptyException.class,
                    () -> fixture.service(UndoTruncationFaultInjector.none())
                            .truncate(SPACE, TablespaceState.ACTIVE));
        }
    }

    @Test
    void resumesAfterCrashPointFollowingPhysicalTruncate() {
        try (Fixture fixture = new Fixture(dir.resolve("resume"))) {
            fixture.growUndoSpace();
            UndoTruncationFaultInjector crash = phase -> {
                if (phase == UndoTruncationPhase.AFTER_PHYSICAL_TRUNCATE) {
                    throw new SimulatedTruncationCrashException("simulated crash after physical truncate");
                }
            };
            assertThrows(SimulatedTruncationCrashException.class,
                    () -> fixture.service(crash).truncate(SPACE, TablespaceState.ACTIVE));
            assertEquals(PageNo.of(64), fixture.store.currentSizeInPages(SPACE));
            assertEquals(TablespaceState.TRUNCATING, fixture.disk.tablespaceState(SPACE));

            UndoTablespaceTruncationResult resumed = fixture.service(UndoTruncationFaultInjector.none())
                    .truncate(SPACE, TablespaceState.ACTIVE);

            assertEquals(1L, resumed.truncateEpoch());
            assertEquals(TablespaceState.ACTIVE, fixture.disk.tablespaceState(SPACE));
            assertEquals(PageNo.of(64), fixture.store.currentSizeInPages(SPACE));
        }
    }

    /** durable marker 一旦可从 page0 读取，doublewrite 不得再恢复 target 之外的旧尾页。 */
    @Test
    void recoveryParticipantFiltersDurableTruncatingTail() {
        try (Fixture fixture = new Fixture(dir.resolve("tail-filter"))) {
            fixture.growUndoSpace();
            UndoTruncationFaultInjector stopAfterMarker = phase -> {
                if (phase == UndoTruncationPhase.AFTER_BUFFER_INVALIDATION) {
                    throw new SimulatedTruncationCrashException("stop after marker page flush");
                }
            };
            assertThrows(SimulatedTruncationCrashException.class,
                    () -> fixture.service(stopAfterMarker).truncate(SPACE, TablespaceState.ACTIVE));

            UndoTablespaceTruncationRecovery recovery = new UndoTablespaceTruncationRecovery(
                    Set.of(SPACE), fixture.store, PS, fixture.registry, fixture.redo,
                    fixture.service(UndoTruncationFaultInjector.none()));
            recovery.prepareDoublewrite(null);

            assertTrue(recovery.shouldRepairDoublewritePage(
                    cn.zhangyis.db.domain.PageId.of(SPACE, PageNo.of(63))));
            assertFalse(recovery.shouldRepairDoublewritePage(
                    cn.zhangyis.db.domain.PageId.of(SPACE, PageNo.of(64))));
            assertFalse(recovery.shouldRepairDoublewritePage(
                    cn.zhangyis.db.domain.PageId.of(SPACE, PageNo.of(0))));
        }
    }

    @Test
    void rejectsGeneralTablespaceWithoutChangingPhysicalSize() {
        try (Fixture fixture = new Fixture(dir.resolve("general-reject"))) {
            SpaceId general = SpaceId.of(91);
            MiniTransaction create = fixture.mtrManager.begin();
            fixture.disk.createTablespace(create, general, dir.resolve("general-reject/general.ibd"),
                    PageNo.of(64), TablespaceType.GENERAL);
            Lsn lsn = fixture.mtrManager.commit(create);
            fixture.flushService.flushThrough(lsn, Duration.ofSeconds(2));

            assertThrows(UndoTablespaceTruncationException.class,
                    () -> fixture.service(UndoTruncationFaultInjector.none())
                            .truncate(general, TablespaceState.ACTIVE));
            assertEquals(PageNo.of(64), fixture.store.currentSizeInPages(general));
        }
    }

    @Test
    void rejectsLegacyUndoWithoutLifecycleHeader() {
        try (Fixture fixture = new Fixture(dir.resolve("legacy-reject"))) {
            SpaceId legacy = SpaceId.of(92);
            Path path = dir.resolve("legacy-reject/legacy.ibu");
            fixture.store.create(legacy, path, PS, PageNo.of(64));
            MiniTransaction create = fixture.mtrManager.begin();
            fixture.headerRepo.initialize(create, new SpaceHeaderSnapshot(legacy, PS,
                    cn.zhangyis.db.storage.fil.state.TablespaceTypeFlags.encode(TablespaceType.UNDO),
                    PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 1L));
            new ExtentDescriptorRepository(fixture.pool, PS).reserveSystemExtent(create, legacy);
            Lsn lsn = fixture.mtrManager.commit(create);
            fixture.flushService.flushThrough(lsn, Duration.ofSeconds(2));
            fixture.registry.refresh(legacy);

            assertThrows(UndoTablespaceTruncationException.class,
                    () -> fixture.service(UndoTruncationFaultInjector.none())
                            .truncate(legacy, TablespaceState.ACTIVE));
            assertEquals(PageNo.of(64), fixture.store.currentSizeInPages(legacy));
        }
    }

    @Test
    void recoveryFailsWhenConfiguredUndoSpaceIsNotOpen() {
        try (Fixture fixture = new Fixture(dir.resolve("missing-config"))) {
            UndoTablespaceTruncationRecovery recovery = new UndoTablespaceTruncationRecovery(
                    Set.of(SpaceId.of(999)), fixture.store, PS, fixture.registry, fixture.redo,
                    fixture.service(UndoTruncationFaultInjector.none()));
            assertThrows(RuntimeException.class, () -> recovery.prepareDoublewrite(null));
        }
    }

    /**
     * 跨实例恢复：第一套对象在 marker 已刷盘、物理截断前“崩溃”；第二套对象重开文件/redo，
     * CrashRecoveryService 完成 redo replay、安装 LSN 边界、续作 truncate 后才开放流量。
     */
    @Test
    void crashRecoveryReopensAndResumesDurableTruncatingSpace() {
        Path base = dir.resolve("real-recovery");
        Path dataPath = base.resolve("undo.ibu");
        Path redoPath = base.resolve("redo.log");
        base.toFile().mkdirs();

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repository = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(repository);
            TablespaceAccessController controller = new TablespaceAccessController(Duration.ofSeconds(2));
            MiniTransactionManager manager = new MiniTransactionManager(controller, redo);
            TablespaceRegistry registry = new CachingTablespaceRegistry(
                    new PageZeroTablespaceMetadataLoader(store, PS, controller));
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS, registry);
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ofSeconds(1), controller);
            FlushService flush = new FlushService(pool, coordinator, new CheckpointCoordinator(pool, redo), redo,
                    RedoCapacityPolicy.fixed(1_000_000), AdaptiveFlushPolicy.fixed(1, 128));
            MiniTransaction create = manager.begin();
            disk.createTablespace(create, SPACE, dataPath, PageNo.of(64), TablespaceType.UNDO);
            flush.flushThrough(manager.commit(create), Duration.ofSeconds(2));
            PageNo grown = store.extend(SPACE);
            MiniTransaction update = manager.begin();
            new SpaceHeaderRepository(pool).setCurrentSizeInPages(update, SPACE, grown);
            flush.flushThrough(manager.commit(update), Duration.ofSeconds(2));
            UndoTablespaceTruncationService service = new UndoTablespaceTruncationService(
                    pool, store, PS, registry, controller, manager, flush, Duration.ofSeconds(2), phase -> {
                if (phase == UndoTruncationPhase.AFTER_BUFFER_INVALIDATION) {
                    throw new SimulatedTruncationCrashException("crash before physical truncate");
                }
            });
            assertThrows(SimulatedTruncationCrashException.class,
                    () -> service.truncate(SPACE, TablespaceState.ACTIVE));
        }

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128);
             RedoLogFileRepository repository = RedoLogFileRepository.open(redoPath);
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(base.resolve("redo-control"))) {
            RedoLogManager redo = RedoLogManager.durable(repository);
            TablespaceAccessController controller = new TablespaceAccessController(Duration.ofSeconds(2));
            MiniTransactionManager manager = new MiniTransactionManager(controller, redo);
            TablespaceRegistry registry = new CachingTablespaceRegistry(
                    new PageZeroTablespaceMetadataLoader(store, PS, controller));
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS, registry);
            disk.openTablespaceForRecovery(SPACE, dataPath);
            FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ofSeconds(1), controller);
            FlushService flush = new FlushService(pool, coordinator, new CheckpointCoordinator(pool, redo), redo,
                    RedoCapacityPolicy.fixed(1_000_000), AdaptiveFlushPolicy.fixed(1, 128));
            UndoTablespaceTruncationService service = new UndoTablespaceTruncationService(
                    pool, store, PS, registry, controller, manager, flush, Duration.ofSeconds(2),
                    UndoTruncationFaultInjector.none());
            UndoTablespaceTruncationRecovery undoRecovery = new UndoTablespaceTruncationRecovery(
                    Set.of(SPACE), store, PS, registry, redo, service);
            RecoveryTrafficGate gate = new RecoveryTrafficGate();
            RecoveryRequest request = RecoveryRequest.normal(checkpointStore, repository,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS))
                    .withUndoTablespaceRecovery(undoRecovery);

            new CrashRecoveryService(gate).recover(request);

            assertEquals(RecoveryState.OPEN, gate.state());
            assertEquals(TablespaceState.ACTIVE, disk.tablespaceState(SPACE));
            assertEquals(PageNo.of(64), store.currentSizeInPages(SPACE));
        }
    }

    private final class Fixture implements AutoCloseable {
        private final Path path;
        private final PageStore store = new FileChannelPageStore();
        private final BufferPool pool = new LruBufferPool(store, PS, 128);
        private final RedoLogFileRepository redoRepository;
        private final RedoLogManager redo;
        private final TablespaceAccessController accessController =
                new TablespaceAccessController(Duration.ofSeconds(2));
        private final MiniTransactionManager mtrManager;
        private final TablespaceRegistry registry;
        private final DiskSpaceManager disk;
        private final SpaceHeaderRepository headerRepo = new SpaceHeaderRepository(pool);
        private final FlushService flushService;

        private Fixture(Path base) {
            base.toFile().mkdirs();
            this.path = base.resolve("undo.ibu");
            this.redoRepository = RedoLogFileRepository.open(base.resolve("redo.log"));
            this.redo = RedoLogManager.durable(redoRepository);
            this.mtrManager = new MiniTransactionManager(accessController, redo);
            this.registry = new CachingTablespaceRegistry(
                    new PageZeroTablespaceMetadataLoader(store, PS, accessController));
            this.disk = new DiskSpaceManager(pool, store, PS, registry);
            FlushCoordinator flushCoordinator = new FlushCoordinator(pool, store, redo, PS,
                    new NoDoublewriteStrategy(), Duration.ofSeconds(1), accessController);
            CheckpointCoordinator checkpoint = new CheckpointCoordinator(pool, redo);
            this.flushService = new FlushService(pool, flushCoordinator, checkpoint, redo,
                    RedoCapacityPolicy.fixed(1_000_000), AdaptiveFlushPolicy.fixed(1, 128));

            MiniTransaction create = mtrManager.begin();
            disk.createTablespace(create, SPACE, path, PageNo.of(64), TablespaceType.UNDO);
            Lsn createLsn = mtrManager.commit(create);
            flushService.flushThrough(createLsn, Duration.ofSeconds(2));
        }

        private void growUndoSpace() {
            PageNo grown = store.extend(SPACE);
            MiniTransaction update = mtrManager.begin();
            headerRepo.setCurrentSizeInPages(update, SPACE, grown);
            Lsn lsn = mtrManager.commit(update);
            flushService.flushThrough(lsn, Duration.ofSeconds(2));
            assertTrue(grown.value() > 64);
        }

        private UndoTablespaceTruncationService service(UndoTruncationFaultInjector faultInjector) {
            return new UndoTablespaceTruncationService(pool, store, PS, registry, accessController,
                    mtrManager, flushService, Duration.ofSeconds(2), faultInjector);
        }

        @Override
        public void close() {
            pool.close();
            store.close();
            redoRepository.close();
        }
    }
}
