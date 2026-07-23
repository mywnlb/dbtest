package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
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
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 真实落盘后重建 Buffer Pool，验证 target demand load 会在 page hash 发布前应用全局 mutation 并原子 consume。
 */
class ChangeBufferPrepublicationMergeIntegrationTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private static final SpaceId USER_SPACE = SpaceId.of(91);
    private static final long TABLE_ID = 17L;
    private static final long PRIMARY_ID = 101L;
    private static final long SECONDARY_ID = 102L;

    @TempDir
    Path tempDir;

    /** 磁盘旧 leaf 在 merge commit 前不可见，发布后 entry、header 与 bitmap 必须同时到达新状态。 */
    @Test
    void demandLoadMergesBufferedInsertBeforePagePublication() {
        TypeCodecRegistry registry = new TypeCodecRegistry();
        SecondaryIndexMetadata[] metadataHolder = new SecondaryIndexMetadata[1];
        PageId[] targetHolder = new PageId[1];
        ChangeBufferMutation[] mutationHolder = new ChangeBufferMutation[1];

        try (RedoLogFileRepository redoRepository = RedoLogFileRepository.open(tempDir.resolve("redo.log"));
             PageStore pageStore = new FileChannelPageStore()) {
            RedoLogManager redo = RedoLogManager.durable(redoRepository);
            // 1. 第一代 pool 创建并刷清 system.ibd、空用户 leaf、全局 mutation 与 buffered bitmap。
            try (LruBufferPool firstPool = new LruBufferPool(pageStore, PAGE_SIZE, 64)) {
                MiniTransactionManager manager = new MiniTransactionManager(
                        new TablespaceAccessController(), redo);
                DiskSpaceManager disk = new DiskSpaceManager(firstPool, pageStore, PAGE_SIZE);
                IndexPageAccess pages = new IndexPageAccess(firstPool, PAGE_SIZE);
                ChangeBufferHeaderRepository headers = new ChangeBufferHeaderRepository(firstPool, PAGE_SIZE);
                SplitCapableBTreeIndexService btree = new SplitCapableBTreeIndexService(pages, disk, registry);
                ChangeBufferStore changes = new ChangeBufferStore(headers, btree, PAGE_SIZE);
                ChangeBufferBitmapRepository bitmaps = new ChangeBufferBitmapRepository(firstPool, PAGE_SIZE);

                MiniTransaction boot = manager.begin();
                disk.createTablespace(boot, ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID,
                        tempDir.resolve("system.ibd"), PageNo.of(128), TablespaceType.SYSTEM);
                new ChangeBufferBootstrap(disk, pages, headers, PAGE_SIZE).initialize(boot, ChangeBufferMode.ALL);
                disk.createTablespace(boot, USER_SPACE, tempDir.resolve("user.ibd"),
                        PageNo.of(128), TablespaceType.GENERAL);
                SegmentRef leaf = disk.createSegment(boot, USER_SPACE, SegmentPurpose.INDEX_LEAF);
                SegmentRef nonLeaf = disk.createSegment(boot, USER_SPACE, SegmentPurpose.INDEX_NON_LEAF);
                PageId target = disk.allocatePage(boot, leaf);
                pages.createIndexPage(boot, target, SECONDARY_ID, 0);
                manager.commit(boot);

                SecondaryIndexMetadata metadata = secondaryMetadata(target, leaf, nonLeaf);
                LogicalRecord entry = metadata.layout().toEntry(tableRow(1, 700), false);
                byte[] encodedEntry = new RecordEncoder(registry).encode(entry, metadata.index().schema());
                MiniTransaction append = manager.begin();
                ChangeBufferAppendResult appended = changes.append(append, target, TABLE_ID, 1L,
                        SECONDARY_ID, ChangeBufferOperation.INSERT, encodedEntry);
                bitmaps.write(append, target, new ChangeBufferBitmapState(3, true, false));
                manager.commit(append);

                flushAll(firstPool, pageStore, redo);
                metadataHolder[0] = metadata;
                targetHolder[0] = target;
                mutationHolder[0] = appended.mutation();
            }

            // 2. exact-version resolver 失败是持久证据不可解释，必须按格式 fatal 拒绝发布且保留 global mutation。
            try (LruBufferPool failedPool = new LruBufferPool(pageStore, PAGE_SIZE, 64)) {
                MiniTransactionManager manager = new MiniTransactionManager(
                        new TablespaceAccessController(), redo);
                DiskSpaceManager disk = new DiskSpaceManager(failedPool, pageStore, PAGE_SIZE);
                IndexPageAccess pages = new IndexPageAccess(failedPool, PAGE_SIZE);
                ChangeBufferHeaderRepository headers = new ChangeBufferHeaderRepository(failedPool, PAGE_SIZE);
                ChangeBufferBitmapRepository bitmaps = new ChangeBufferBitmapRepository(failedPool, PAGE_SIZE);
                SplitCapableBTreeIndexService btree = new SplitCapableBTreeIndexService(pages, disk, registry);
                ChangeBufferStore changes = new ChangeBufferStore(headers, btree, PAGE_SIZE);
                ChangeBufferPageMerger merger = new ChangeBufferPageMerger(
                        (tableId, schemaVersion, indexId) -> {
                            throw new ChangeBufferStateException("exact metadata intentionally unavailable");
                        }, registry, PAGE_SIZE);
                failedPool.attachPageLoadInterceptor(new ChangeBufferPageMergeInterceptor(
                        bitmaps, changes, merger, manager, new ChangeBufferPageGate(64),
                        Duration.ofSeconds(5), PAGE_SIZE));

                ChangeBufferFormatException failure = assertThrows(ChangeBufferFormatException.class,
                        () -> failedPool.getPage(targetHolder[0],
                                cn.zhangyis.db.storage.buf.PageLatchMode.SHARED));
                assertNotNull(failure.getCause());
                assertFalse(failedPool.isResident(targetHolder[0]));
                MiniTransaction verify = manager.beginReadOnly();
                assertEquals(1L, changes.pendingOperations(verify));
                manager.commit(verify);
            }

            // 3. 下一代 pool 没有目标页驻留；拦截器读取已落盘 bitmap/global tree并在 detached MTR 合并。
            try (LruBufferPool secondPool = new LruBufferPool(pageStore, PAGE_SIZE, 64)) {
                MiniTransactionManager manager = new MiniTransactionManager(
                        new TablespaceAccessController(), redo);
                DiskSpaceManager disk = new DiskSpaceManager(secondPool, pageStore, PAGE_SIZE);
                IndexPageAccess pages = new IndexPageAccess(secondPool, PAGE_SIZE);
                ChangeBufferHeaderRepository headers = new ChangeBufferHeaderRepository(secondPool, PAGE_SIZE);
                ChangeBufferBitmapRepository bitmaps = new ChangeBufferBitmapRepository(secondPool, PAGE_SIZE);
                SplitCapableBTreeIndexService btree = new SplitCapableBTreeIndexService(pages, disk, registry);
                ChangeBufferStore changes = new ChangeBufferStore(headers, btree, PAGE_SIZE);
                ChangeBufferPageMerger merger = new ChangeBufferPageMerger(
                        (tableId, schemaVersion, indexId) -> metadataHolder[0], registry, PAGE_SIZE);
                secondPool.attachPageLoadInterceptor(new ChangeBufferPageMergeInterceptor(
                        bitmaps, changes, merger, manager, new ChangeBufferPageGate(64),
                        Duration.ofSeconds(5), PAGE_SIZE));

                assertFalse(secondPool.isResident(targetHolder[0]));
                MiniTransaction read = manager.beginReadOnly();
                var found = btree.lookup(read, metadataHolder[0].index(),
                        metadataHolder[0].layout().physicalKey(
                                metadataHolder[0].layout().toEntry(tableRow(1, 700), false)));
                assertTrue(found.isPresent());
                assertFalse(found.orElseThrow().record().deleted());
                manager.commit(read);

                // 4. merge write MTR 已消费全局记录并清 pending bit，重复 load/scan 不会再次应用。
                MiniTransaction verify = manager.beginReadOnly();
                assertEquals(0L, changes.pendingOperations(verify));
                assertTrue(changes.scanPage(verify, targetHolder[0], 10).isEmpty());
                assertFalse(bitmaps.read(verify, targetHolder[0]).buffered());
                manager.commit(verify);
                assertEquals(targetHolder[0], mutationHolder[0].targetPageId());
            }
        }
    }

    private void flushAll(LruBufferPool pool, PageStore pageStore, RedoLogManager redo) {
        redo.flush();
        FlushCoordinator coordinator = new FlushCoordinator(pool, pageStore, redo, PAGE_SIZE,
                new NoDoublewriteStrategy(), Duration.ofSeconds(5));
        int rounds = 0;
        while (pool.hasDirtyPages() && rounds++ < 10) {
            coordinator.flushList(redo.currentLsn(), 128);
        }
        assertFalse(pool.hasDirtyPages(), "test bootstrap must reach a clean restart boundary");
    }

    private static SecondaryIndexMetadata secondaryMetadata(PageId root, SegmentRef leaf, SegmentRef nonLeaf) {
        TableSchema table = new TableSchema(1L, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.bigint(false, false), 0),
                new ColumnDef(new ColumnId(1), "tenant", ColumnType.bigint(false, false), 1)), true);
        IndexKeyDef primary = new IndexKeyDef(PRIMARY_ID,
                List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
        IndexKeyDef secondary = new IndexKeyDef(SECONDARY_ID,
                List.of(new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0)));
        SecondaryIndexLayout layout = SecondaryIndexLayout.create(table, secondary, primary);
        BTreeIndex index = new BTreeIndex(SECONDARY_ID, root, 0, layout.physicalKeyDef(),
                layout.entrySchema(), true, leaf, nonLeaf);
        return new SecondaryIndexMetadata(index, layout, false);
    }

    private static LogicalRecord tableRow(long id, long tenant) {
        return new LogicalRecord(1L, List.of(new ColumnValue.IntValue(id),
                new ColumnValue.IntValue(tenant)), false, RecordType.CONVENTIONAL);
    }
}
