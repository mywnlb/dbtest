package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
import cn.zhangyis.db.storage.btree.BTreeRootSnapshotService;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.SecondaryIndexLayout;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 使用真实 FSP/B+Tree/redo 和两代 Buffer Pool 验证生产 coordinator 的“未驻留 leaf → durable buffer →
 * 首次加载前 merge”完整链路，不用 mock 绕过 residency、后台 worker 或页发布协议。
 */
class SecondaryIndexMutationCoordinatorIntegrationTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(4 * 1024);
    private static final SpaceId USER_SPACE = SpaceId.of(151);
    private static final long TABLE_ID = 81L;
    private static final long PRIMARY_ID = 901L;
    private static final long SECONDARY_ID = 902L;

    @TempDir
    Path tempDir;

    /**
     * 非唯一升序二级 leaf 在第二代 Buffer Pool 中未驻留时必须只写 global tree/bitmap；随后后台 worker
     * 应通过普通 page load 触发发布前局部 INSERT、consume header 计数，lookup 再命中已合并结果。
     */
    @Test
    void buffersEligibleInsertAndBackgroundWorkerMergesBeforeLookup() throws Exception {
        Path systemFile = tempDir.resolve("system.ibd");
        Path userFile = tempDir.resolve("user.ibd");
        TypeCodecRegistry registry = new TypeCodecRegistry();
        PreparedIndex prepared;

        try (RedoLogFileRepository redoRepository = RedoLogFileRepository.open(tempDir.resolve("redo.log"))) {
            RedoLogManager redo = RedoLogManager.durable(redoRepository);
            // 1、第一代实例建立 level-1 二级树，选择一个不存在但稳定路由到非 root leaf 的 key，并刷清重启边界。
            try (PageStore firstStore = new FileChannelPageStore();
                 LruBufferPool firstPool = new LruBufferPool(firstStore, PAGE_SIZE, 96)) {
                PreparedIndex built = buildSplitSecondary(firstStore, firstPool, redo, registry,
                        systemFile, userFile);
                prepared = built;
                MiniTransactionManager firstMtr = built.mtrManager();
                ChangeBufferBitmapRepository firstBitmaps = new ChangeBufferBitmapRepository(firstPool, PAGE_SIZE);
                MiniTransaction bitmap = firstMtr.begin();
                firstBitmaps.write(bitmap, built.targetPage(), new ChangeBufferBitmapState(3, false, false));
                firstMtr.commit(bitmap);
                flushAll(firstPool, firstStore, redo);
            }

            // 2、重开物理空间但不加载目标 leaf；coordinator 只读取 root parent，提交 mutation/header/bitmap 原子批次。
            try (PageStore secondStore = new FileChannelPageStore();
                 LruBufferPool secondPool = new LruBufferPool(secondStore, PAGE_SIZE, 96)) {
                TablespaceAccessController access = new TablespaceAccessController();
                MiniTransactionManager manager = new MiniTransactionManager(access, redo);
                DiskSpaceManager disk = new DiskSpaceManager(secondPool, secondStore, PAGE_SIZE);
                disk.openTablespace(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID, systemFile);
                disk.openTablespace(USER_SPACE, userFile);
                IndexPageAccess pages = new IndexPageAccess(secondPool, PAGE_SIZE);
                SplitCapableBTreeIndexService btree = new SplitCapableBTreeIndexService(pages, disk, registry);
                ChangeBufferHeaderRepository headers = new ChangeBufferHeaderRepository(secondPool, PAGE_SIZE);
                ChangeBufferBitmapRepository bitmaps = new ChangeBufferBitmapRepository(secondPool, PAGE_SIZE);
                ChangeBufferStore store = new ChangeBufferStore(headers, btree, PAGE_SIZE);
                ChangeBufferMetadataCatalog catalog = new ChangeBufferMetadataCatalog(
                        (tableId, schemaVersion, indexId) -> prepared.secondary());
                ChangeBufferPageGate gate = new ChangeBufferPageGate(64);
                ChangeBufferCounters counters = new ChangeBufferCounters();
                SecondaryIndexMutationCoordinator coordinator = new SecondaryIndexMutationCoordinator(
                        ChangeBufferConfig.defaults(), secondPool, manager, btree,
                        new BTreeRootSnapshotService(pages), pages, store, bitmaps, gate,
                        catalog, registry, PAGE_SIZE, counters);
                coordinator.register(prepared.tableMetadata());

                assertFalse(secondPool.isResident(prepared.targetPage()));
                SecondaryIndexMutationResult result = coordinator.insertOrRevive(
                        TABLE_ID, 1L, prepared.secondary(), prepared.candidate(), false,
                        RedoBudgetPurpose.SECONDARY_INDEX);
                assertTrue(result.buffered());
                assertEquals(prepared.targetPage(), result.targetPageId());
                assertFalse(secondPool.isResident(prepared.targetPage()));

                MiniTransaction pendingRead = manager.beginReadOnly();
                assertEquals(1L, store.pendingOperations(pendingRead));
                assertTrue(bitmaps.read(pendingRead, prepared.targetPage()).buffered());
                manager.commit(pendingRead);
                assertEquals(1L, counters.snapshot().bufferedOperations());

                // 3、安装与生产相同的发布前拦截器并启动低优先级 worker；worker 只做普通 demand load。
                ChangeBufferPageMerger merger = new ChangeBufferPageMerger(catalog, registry, PAGE_SIZE);
                secondPool.attachPageLoadInterceptor(new ChangeBufferPageMergeInterceptor(
                        bitmaps, store, merger, manager, gate, Duration.ofSeconds(5), PAGE_SIZE, counters));
                ChangeBufferMergeWorker worker = new ChangeBufferMergeWorker(
                        new ChangeBufferConfig(ChangeBufferMode.ALL, 25, Duration.ofMillis(20),
                                4, Duration.ofSeconds(5), Duration.ofSeconds(2)),
                        store, manager, secondPool);
                try (worker) {
                    worker.start();
                    worker.requestMerge();
                    assertTrue(awaitPendingOperations(manager, store, 0L, Duration.ofSeconds(2)),
                            () -> "worker failed to consume pending mutation: " + worker.failure());
                }
                assertEquals(ChangeBufferWorkerState.STOPPED, worker.state());

                // worker 触发的 getPage 只有在 interceptor 提交 merge 后才发布；随后普通 lookup 命中已合并页。
                MiniTransaction lookup = manager.beginReadOnly();
                assertTrue(btree.lookup(lookup, prepared.secondary().index(),
                        prepared.secondary().layout().physicalKey(prepared.candidate())).isPresent());
                manager.commit(lookup);

                // 4、目标页已发布且全局证据清零；事件计数只在两个实际提交边界后推进。
                MiniTransaction mergedRead = manager.beginReadOnly();
                assertEquals(0L, store.pendingOperations(mergedRead));
                assertFalse(bitmaps.read(mergedRead, prepared.targetPage()).buffered());
                manager.commit(mergedRead);
                assertEquals(1L, counters.snapshot().mergedOperations());
                assertEquals(0L, counters.snapshot().directFallbacks());
            }
        }
    }

    /** 建立 system/user 空间和刚发生 root split 的二级树，返回稳定 descriptor 与候选 leaf。 */
    private PreparedIndex buildSplitSecondary(PageStore store, LruBufferPool pool, RedoLogManager redo,
                                              TypeCodecRegistry registry, Path systemFile, Path userFile) {
        TablespaceAccessController access = new TablespaceAccessController();
        MiniTransactionManager manager = new MiniTransactionManager(access, redo);
        DiskSpaceManager disk = new DiskSpaceManager(pool, store, PAGE_SIZE);
        IndexPageAccess pages = new IndexPageAccess(pool, PAGE_SIZE);
        ChangeBufferHeaderRepository headers = new ChangeBufferHeaderRepository(pool, PAGE_SIZE);
        SplitCapableBTreeIndexService btree = new SplitCapableBTreeIndexService(pages, disk, registry);

        MiniTransaction boot = manager.begin();
        disk.createTablespace(boot, ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID,
                systemFile, PageNo.of(512), TablespaceType.SYSTEM);
        new ChangeBufferBootstrap(disk, pages, headers, PAGE_SIZE).initialize(boot, ChangeBufferMode.ALL);
        disk.createTablespace(boot, USER_SPACE, userFile, PageNo.of(512), TablespaceType.GENERAL);
        SegmentRef leaf = disk.createSegment(boot, USER_SPACE, SegmentPurpose.INDEX_LEAF);
        SegmentRef nonLeaf = disk.createSegment(boot, USER_SPACE, SegmentPurpose.INDEX_NON_LEAF);
        PageId root = disk.allocatePage(boot, leaf);
        pages.createIndexPage(boot, root, SECONDARY_ID, 0);
        manager.commit(boot);

        SecondaryIndexLayout layout = layout();
        BTreeIndex index = new BTreeIndex(SECONDARY_ID, root, 0, layout.physicalKeyDef(),
                layout.entrySchema(), true, leaf, nonLeaf);
        int inserted = 0;
        while (index.rootLevel() == 0 && inserted < 600) {
            LogicalRecord entry = layout.toEntry(tableRow(inserted + 1L, inserted * 2L), false);
            MiniTransaction write = manager.begin();
            BTreeInsertResult result = btree.insertSecondary(write, index, entry);
            manager.commit(write);
            index = result.indexAfterInsert();
            inserted++;
        }
        assertTrue(index.rootLevel() > 0, "test fixture must create a non-root leaf");
        LogicalRecord candidate = layout.toEntry(tableRow(10_000L, inserted * 2L - 1L), false);
        MiniTransaction locate = manager.beginReadOnly();
        PageId target = btree.locateLeafWithoutLoading(locate, index, layout.physicalKey(candidate)).pageId();
        manager.commit(locate);

        SecondaryIndexMetadata secondary = new SecondaryIndexMetadata(index, layout, false);
        BTreeIndex clustered = new BTreeIndex(PRIMARY_ID, root, 0, primaryKey(), tableSchema(), true);
        TableIndexMetadata tableMetadata = new TableIndexMetadata(TABLE_ID, 1L, clustered, List.of(secondary));
        return new PreparedIndex(manager, secondary, tableMetadata, candidate, target);
    }

    /** 把全部 dirty 页推进到 durable redo 之后并落盘，确保第二代 Buffer Pool 只从文件读取旧 leaf。 */
    private static void flushAll(LruBufferPool pool, PageStore store, RedoLogManager redo) {
        redo.flush();
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PAGE_SIZE,
                new NoDoublewriteStrategy(), Duration.ofSeconds(5));
        int rounds = 0;
        while (pool.hasDirtyPages() && rounds++ < 20) {
            coordinator.flushList(redo.currentLsn(), 256);
        }
        assertFalse(pool.hasDirtyPages(), "fixture must reach a clean restart boundary");
    }

    /** 在有界 deadline 内只读观察全局 pending；每轮 MTR 都完整释放，不与 worker 页 IO 形成资源等待。 */
    private static boolean awaitPendingOperations(MiniTransactionManager manager, ChangeBufferStore store,
                                                  long expected, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        do {
            MiniTransaction read = manager.beginReadOnly();
            try {
                long pending = store.pendingOperations(read);
                manager.commit(read);
                if (pending == expected) {
                    return true;
                }
            } catch (RuntimeException failure) {
                if (read.state() == cn.zhangyis.db.storage.mtr.MiniTransactionState.ACTIVE) {
                    manager.rollbackUncommitted(read);
                }
                throw failure;
            }
            TimeUnit.MILLISECONDS.sleep(10L);
        } while (System.nanoTime() < deadline);
        return false;
    }

    private static SecondaryIndexLayout layout() {
        return SecondaryIndexLayout.create(tableSchema(), secondaryKey(), primaryKey());
    }

    private static TableSchema tableSchema() {
        return new TableSchema(1L, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.bigint(false, false), 0),
                new ColumnDef(new ColumnId(1), "tenant", ColumnType.bigint(false, false), 1)), true);
    }

    private static IndexKeyDef primaryKey() {
        return new IndexKeyDef(PRIMARY_ID,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static IndexKeyDef secondaryKey() {
        return new IndexKeyDef(SECONDARY_ID,
                List.of(new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0)));
    }

    private static LogicalRecord tableRow(long id, long tenant) {
        return new LogicalRecord(1L, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.IntValue(tenant)), false, RecordType.CONVENTIONAL);
    }

    /** fixture 关闭后仍可跨第二代组合根使用的纯 metadata 与物理 identity。 */
    private record PreparedIndex(MiniTransactionManager mtrManager, SecondaryIndexMetadata secondary,
                                 TableIndexMetadata tableMetadata, LogicalRecord candidate,
                                 PageId targetPage) {
    }
}
