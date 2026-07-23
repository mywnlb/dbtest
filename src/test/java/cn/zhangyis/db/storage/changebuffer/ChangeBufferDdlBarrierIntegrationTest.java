package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Change Buffer DDL barrier 的真实 global tree/header/bitmap 原子协作测试。 */
class ChangeBufferDdlBarrierIntegrationTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private static final SpaceId USER_SPACE = SpaceId.of(171);
    private static final long TABLE_ID = 71L;

    @TempDir
    Path tempDir;

    /** DROP INDEX 只清目标 identity，同页其它索引 mutation 必须继续保持 buffered；DROP SPACE 再清空余量。 */
    @Test
    void indexThenSpaceDiscardPreservesAndClearsBitmapAtExactBoundaries() throws Exception {
        withContext(context -> {
            PageId firstTarget = PageId.of(USER_SPACE, PageNo.of(8));
            PageId secondTarget = PageId.of(USER_SPACE, PageNo.of(9));
            append(context, firstTarget, 11L, new byte[]{1});
            append(context, firstTarget, 12L, new byte[]{2});
            append(context, secondTarget, 11L, new byte[]{3});
            markBuffered(context, firstTarget, secondTarget);

            assertEquals(2L, context.barrier.discardIndex(TABLE_ID, 11L, Duration.ofSeconds(2)));
            MiniTransaction afterIndex = context.manager.beginReadOnly();
            assertEquals(1L, context.store.pendingOperations(afterIndex));
            assertTrue(context.bitmaps.read(afterIndex, firstTarget).buffered());
            assertFalse(context.bitmaps.read(afterIndex, secondTarget).buffered());
            context.manager.commit(afterIndex);
            assertEquals(2L, context.counters.snapshot().discardedOperations());

            assertEquals(1L, context.barrier.discardSpace(USER_SPACE, Duration.ofSeconds(2)));
            MiniTransaction afterSpace = context.manager.beginReadOnly();
            assertEquals(0L, context.store.pendingOperations(afterSpace));
            assertFalse(context.bitmaps.read(afterSpace, firstTarget).buffered());
            context.manager.commit(afterSpace);
            assertEquals(3L, context.counters.snapshot().discardedOperations());
        });
    }

    /** gate 被发布前 loader/append owner 占用时，DDL timeout 必须保留 mutation、header count 与 bitmap。 */
    @Test
    void timeoutDoesNotConsumeMutationOrClearBitmap() throws Exception {
        withContext(context -> {
            PageId target = PageId.of(USER_SPACE, PageNo.of(10));
            append(context, target, 21L, new byte[]{9});
            markBuffered(context, target);
            CountDownLatch acquired = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> holder = executor.submit(() -> {
                    try (ChangeBufferPageGate.Lease ignored =
                                 context.gate.acquire(target, Duration.ofSeconds(2))) {
                        acquired.countDown();
                        if (!release.await(2, TimeUnit.SECONDS)) {
                            throw new ChangeBufferStateException("test gate holder timed out");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new ChangeBufferStateException("test gate holder interrupted", interrupted);
                    }
                });
                assertTrue(acquired.await(2, TimeUnit.SECONDS));
                assertThrows(ChangeBufferPageGateTimeoutException.class,
                        () -> context.barrier.discardIndex(TABLE_ID, 21L, Duration.ofMillis(30)));
                release.countDown();
                holder.get(2, TimeUnit.SECONDS);

                MiniTransaction verify = context.manager.beginReadOnly();
                assertEquals(1L, context.store.pendingOperations(verify));
                assertTrue(context.bitmaps.read(verify, target).buffered());
                context.manager.commit(verify);
                assertEquals(0L, context.counters.snapshot().discardedOperations());
            } finally {
                release.countDown();
                executor.shutdownNow();
            }
        });
    }

    /** 单页允许的 64 条边界批次必须能在固定 merge redo 预算内完整 discard，不能到提交时才发现预算低估。 */
    @Test
    void discardsMaximumPerPageBatchWithinMergeRedoBudget() throws Exception {
        withContext(context -> {
            PageId target = PageId.of(USER_SPACE, PageNo.of(11));
            for (int sequence = 0;
                 sequence < SecondaryIndexMutationCoordinator.MAX_PENDING_PER_PAGE;
                 sequence++) {
                append(context, target, 31L, new byte[]{(byte) sequence});
            }
            markBuffered(context, target);

            assertEquals(SecondaryIndexMutationCoordinator.MAX_PENDING_PER_PAGE,
                    context.barrier.discardIndex(TABLE_ID, 31L, Duration.ofSeconds(5)));
            MiniTransaction verify = context.manager.beginReadOnly();
            assertEquals(0L, context.store.pendingOperations(verify));
            assertFalse(context.bitmaps.read(verify, target).buffered());
            context.manager.commit(verify);
        });
    }

    /** IMPORT 在恢复 page0 NORMAL 前必须清除外部文件携带的全部 nibble，不能继承旧空闲等级或 pending/internal 位。 */
    @Test
    void importedSpaceBitmapResetClearsEntireCurrentCoverage() throws Exception {
        withContext(context -> {
            PageId firstTarget = PageId.of(USER_SPACE, PageNo.of(8));
            PageId adjacentTarget = PageId.of(USER_SPACE, PageNo.of(9));
            MiniTransaction dirtyBitmap = context.manager.begin();
            context.bitmaps.write(dirtyBitmap, firstTarget,
                    new ChangeBufferBitmapState(3, true, true));
            context.bitmaps.write(dirtyBitmap, adjacentTarget,
                    new ChangeBufferBitmapState(2, true, false));
            context.manager.commit(dirtyBitmap);

            context.barrier.resetImportedSpaceBitmaps(
                    USER_SPACE, PageNo.of(128), Duration.ofSeconds(2));

            MiniTransaction verify = context.manager.beginReadOnly();
            assertEquals(new ChangeBufferBitmapState(0, false, false),
                    context.bitmaps.read(verify, firstTarget));
            assertEquals(new ChangeBufferBitmapState(0, false, false),
                    context.bitmaps.read(verify, adjacentTarget));
            context.manager.commit(verify);
        });
    }

    private void withContext(TestBody body) throws Exception {
        try (PageStore store = new FileChannelPageStore();
             LruBufferPool pool = new LruBufferPool(store, PAGE_SIZE, 64)) {
            MiniTransactionManager manager = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PAGE_SIZE);
            IndexPageAccess pages = new IndexPageAccess(pool, PAGE_SIZE);
            ChangeBufferHeaderRepository headers = new ChangeBufferHeaderRepository(pool, PAGE_SIZE);
            ChangeBufferBitmapRepository bitmaps = new ChangeBufferBitmapRepository(pool, PAGE_SIZE);
            SplitCapableBTreeIndexService btree = new SplitCapableBTreeIndexService(
                    pages, disk, new TypeCodecRegistry());
            MiniTransaction boot = manager.begin();
            disk.createTablespace(boot, ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID,
                    tempDir.resolve("system.ibd"), PageNo.of(128), TablespaceType.SYSTEM);
            new ChangeBufferBootstrap(disk, pages, headers, PAGE_SIZE).initialize(boot, ChangeBufferMode.ALL);
            disk.createTablespace(boot, USER_SPACE, tempDir.resolve("user.ibd"),
                    PageNo.of(128), TablespaceType.GENERAL);
            manager.commit(boot);

            ChangeBufferStore changes = new ChangeBufferStore(headers, btree, PAGE_SIZE);
            ChangeBufferPageGate gate = new ChangeBufferPageGate(64);
            ChangeBufferCounters counters = new ChangeBufferCounters();
            ChangeBufferDdlBarrier barrier = new ChangeBufferDdlBarrier(changes, bitmaps, gate,
                    manager, new ChangeBufferMetadataCatalog(), counters);
            body.run(new Context(manager, changes, bitmaps, gate, counters, barrier));
        }
    }

    private static void append(Context context, PageId target, long indexId, byte[] entry) {
        MiniTransaction mutation = context.manager.begin();
        context.store.append(mutation, target, TABLE_ID, 1L, indexId,
                ChangeBufferOperation.INSERT, entry);
        context.manager.commit(mutation);
    }

    private static void markBuffered(Context context, PageId... targets) {
        MiniTransaction bitmap = context.manager.begin();
        for (PageId target : targets) {
            context.bitmaps.write(bitmap, target, new ChangeBufferBitmapState(3, true, false));
        }
        context.manager.commit(bitmap);
    }

    @FunctionalInterface
    private interface TestBody {
        void run(Context context) throws Exception;
    }

    /** 每个测试唯一的共享 MTR/global tree/gate 组合根。 */
    private record Context(MiniTransactionManager manager, ChangeBufferStore store,
                           ChangeBufferBitmapRepository bitmaps, ChangeBufferPageGate gate,
                           ChangeBufferCounters counters, ChangeBufferDdlBarrier barrier) {
    }
}
