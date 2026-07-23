package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Change Buffer system.ibd 固定页、bitmap nibble 与全局树原子计数的真实 FIL/Buffer Pool/MTR 协作测试。
 */
class ChangeBufferPersistenceIntegrationTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);
    private static final SpaceId USER_SPACE = SpaceId.of(41);

    @TempDir
    Path tempDir;

    /** page 3/header 与 page 4/IBUF_INDEX root 必须在同一 boot MTR 建立稳定 identity。 */
    @Test
    void bootstrapFormatsSystemHeaderAndStableInternalRoot() {
        withContext(context -> {
            BTreeIndex index = context.bootstrap();

            MiniTransaction read = context.mtrManager.begin();
            ChangeBufferHeaderSnapshot snapshot = context.headerRepository.read(read);
            var root = read.getPage(context.pool, ChangeBufferBootstrap.ROOT_PAGE_ID, PageLatchMode.SHARED);
            assertEquals(PageType.IBUF_INDEX, PageEnvelope.readHeader(root).pageType());
            assertEquals(index.rootPageId(), snapshot.rootPageId());
            assertEquals(index.leafSegment(), snapshot.leafSegment());
            assertEquals(0L, snapshot.pendingOperations());
            context.mtrManager.commit(read);
        });
    }

    /** 相邻页共享一个 bitmap byte 时，改写一个 nibble 不得覆盖另一个 nibble。 */
    @Test
    void bitmapReadModifyWritePreservesNeighbourNibble() {
        withContext(context -> {
            context.bootstrap();
            context.createUserSpace();
            PageId even = PageId.of(USER_SPACE, PageNo.of(8));
            PageId odd = PageId.of(USER_SPACE, PageNo.of(9));

            MiniTransaction write = context.mtrManager.begin();
            context.bitmapRepository.write(write, even, new ChangeBufferBitmapState(3, true, false));
            context.bitmapRepository.write(write, odd, new ChangeBufferBitmapState(1, false, true));
            context.mtrManager.commit(write);

            MiniTransaction read = context.mtrManager.begin();
            assertEquals(new ChangeBufferBitmapState(3, true, false),
                    context.bitmapRepository.read(read, even));
            assertEquals(new ChangeBufferBitmapState(1, false, true),
                    context.bitmapRepository.read(read, odd));
            context.mtrManager.commit(read);
        });
    }

    /** append/前缀扫描/consume 必须让全局树记录与 page 3 pending 计数同进同退。 */
    @Test
    void storeAppendsScansAndConsumesWithHeaderCount() {
        withContext(context -> {
            context.bootstrap();
            ChangeBufferStore store = context.store();
            PageId target = PageId.of(USER_SPACE, PageNo.of(9));

            MiniTransaction append = context.mtrManager.begin();
            ChangeBufferAppendResult first = store.append(append, target, 7L, 3L, 11L,
                    ChangeBufferOperation.INSERT, new byte[]{1, 2, 3});
            ChangeBufferAppendResult second = store.append(append, target, 7L, 3L, 11L,
                    ChangeBufferOperation.DELETE_MARK, new byte[]{4, 5});
            context.mtrManager.commit(append);
            assertTrue(second.mutation().sequence() > first.mutation().sequence());

            MiniTransaction scan = context.mtrManager.begin();
            List<ChangeBufferMutation> mutations = store.scanPage(scan, target, 10);
            assertEquals(List.of(first.mutation(), second.mutation()), mutations);
            assertEquals(2L, store.pendingOperations(scan));
            context.mtrManager.commit(scan);

            MiniTransaction consume = context.mtrManager.begin();
            assertTrue(store.consume(consume, first.mutation()));
            context.mtrManager.commit(consume);

            MiniTransaction repeat = context.mtrManager.begin();
            assertFalse(store.consume(repeat, first.mutation()));
            assertEquals(1L, store.pendingOperations(repeat));
            context.mtrManager.commit(repeat);
        });
    }

    /** 最终容量判断必须持有 page 3 X latch；达到上限时不得先插树或推进 header。 */
    @Test
    void appendCapacityGateRejectsBeforePersistentMutation() {
        withContext(context -> {
            context.bootstrap();
            ChangeBufferStore store = context.store();
            PageId firstTarget = PageId.of(USER_SPACE, PageNo.of(9));
            PageId secondTarget = PageId.of(USER_SPACE, PageNo.of(10));

            MiniTransaction first = context.mtrManager.begin();
            store.requireAppendCapacity(first, 1L);
            store.append(first, firstTarget, 7L, 3L, 11L,
                    ChangeBufferOperation.INSERT, new byte[]{1});
            context.mtrManager.commit(first);

            MiniTransaction rejected = context.mtrManager.begin();
            try {
                assertThrows(ChangeBufferCapacityExceededException.class,
                        () -> store.requireAppendCapacity(rejected, 1L));
            } finally {
                context.mtrManager.rollbackUncommitted(rejected);
            }

            MiniTransaction verify = context.mtrManager.beginReadOnly();
            assertEquals(1L, store.pendingOperations(verify));
            assertEquals(1, store.scanAll(verify, 2).size());
            assertEquals(firstTarget, store.scanAll(verify, 2).getFirst().targetPageId());
            context.mtrManager.commit(verify);
        });
    }

    /** FSP 已负责重复 bitmap 管理区后，store 不再把第二覆盖区间误判为 bootstrap 范围外。 */
    @Test
    void appendAcceptsTargetInRepeatedBitmapRange() {
        withContext(context -> {
            context.bootstrap();
            ChangeBufferStore store = context.store();
            PageId target = PageId.of(USER_SPACE, PageNo.of(PAGE_SIZE.bytes()));
            MiniTransaction append = context.mtrManager.begin();
            store.append(append, target, 7L, 3L, 11L,
                    ChangeBufferOperation.INSERT, new byte[]{1});
            context.mtrManager.commit(append);

            MiniTransaction verify = context.mtrManager.beginReadOnly();
            assertEquals(target, store.scanAll(verify, 1).getFirst().targetPageId());
            context.mtrManager.commit(verify);
        });
    }

    /** redo 后扫描必须多读一条哨兵记录，不能让 header 偏小把全局树尾部隐藏在 scan limit 之外。 */
    @Test
    void recoveryRejectsHeaderCountThatHidesTreeSuffix() {
        withContext(context -> {
            context.bootstrap();
            context.createUserSpace();
            ChangeBufferStore store = context.store();
            PageId target = PageId.of(USER_SPACE, PageNo.of(9));

            MiniTransaction append = context.mtrManager.begin();
            store.append(append, target, 7L, 3L, 11L,
                    ChangeBufferOperation.INSERT, new byte[]{1});
            store.append(append, target, 7L, 3L, 11L,
                    ChangeBufferOperation.DELETE_MARK, new byte[]{2});
            context.bitmapRepository.write(append, target,
                    new ChangeBufferBitmapState(3, true, false));
            context.mtrManager.commit(append);

            MiniTransaction corrupt = context.mtrManager.begin();
            ChangeBufferHeaderSnapshot header = context.headerRepository.readForUpdate(corrupt);
            context.headerRepository.write(corrupt, new ChangeBufferHeaderSnapshot(
                    header.state(), header.configuredMode(), header.rootPageId(), header.rootLevel(),
                    header.indexId(), header.leafSegment(), header.nonLeafSegment(),
                    header.nextSequence(), 1L, header.formatEpoch()));
            context.mtrManager.commit(corrupt);

            ChangeBufferRecoveryValidator validator = context.recoveryValidator();
            assertThrows(ChangeBufferFormatException.class, validator::validateAfterRedo);
            assertEquals(0L, validator.validatedPendingOperations());
        });
    }

    /** 全局树有记录而 target bitmap 未置 buffered 会让 loader 发布陈旧 leaf，恢复必须在开放流量前拒绝。 */
    @Test
    void recoveryRejectsGlobalRecordWithoutBufferedBitmap() {
        withContext(context -> {
            context.bootstrap();
            context.createUserSpace();
            ChangeBufferStore store = context.store();
            PageId target = PageId.of(USER_SPACE, PageNo.of(9));

            MiniTransaction append = context.mtrManager.begin();
            store.append(append, target, 7L, 3L, 11L,
                    ChangeBufferOperation.INSERT, new byte[]{1});
            context.mtrManager.commit(append);

            ChangeBufferRecoveryValidator validator = context.recoveryValidator();
            assertThrows(ChangeBufferFormatException.class, validator::validateAfterRedo);
            assertEquals(0L, validator.validatedPendingOperations());

            MiniTransaction repair = context.mtrManager.begin();
            context.bitmapRepository.write(repair, target,
                    new ChangeBufferBitmapState(3, true, false));
            context.mtrManager.commit(repair);
            validator.validateAfterRedo();
            assertEquals(1L, validator.validatedPendingOperations());

            MiniTransaction corruptAgain = context.mtrManager.begin();
            context.bitmapRepository.write(corruptAgain, target,
                    new ChangeBufferBitmapState(3, false, false));
            context.mtrManager.commit(corruptAgain);
            assertThrows(ChangeBufferFormatException.class, validator::validateAfterRedo);
            assertEquals(0L, validator.validatedPendingOperations(),
                    "failed revalidation must not expose the previous successful count");
        });
    }

    /** 恢复必须在 OPEN 前拒绝单页超过 64 条的持久证据，不能把确定性超限推迟到首次页加载。 */
    @Test
    void recoveryRejectsTargetAbovePerPageMutationLimit() {
        withContext(context -> {
            context.bootstrap();
            context.createUserSpace();
            ChangeBufferStore store = context.store();
            PageId target = PageId.of(USER_SPACE, PageNo.of(9));

            for (int i = 0; i <= SecondaryIndexMutationCoordinator.MAX_PENDING_PER_PAGE; i++) {
                MiniTransaction append = context.mtrManager.begin();
                store.append(append, target, 7L, 3L, 11L,
                        ChangeBufferOperation.DELETE_MARK, new byte[]{(byte) i});
                context.mtrManager.commit(append);
            }
            MiniTransaction bitmap = context.mtrManager.begin();
            context.bitmapRepository.write(bitmap, target,
                    new ChangeBufferBitmapState(3, true, false));
            context.mtrManager.commit(bitmap);

            ChangeBufferRecoveryValidator validator = context.recoveryValidator();
            assertThrows(ChangeBufferFormatException.class, validator::validateAfterRedo);
            assertEquals(0L, validator.validatedPendingOperations());
        });
    }

    /** page3 即使 CRC 正确，也不能把全局树 root 重定向到其它系统页；固定 identity 在 scan 前拒绝。 */
    @Test
    void recoveryRejectsWrongStableRootIdentity() {
        withContext(context -> {
            context.bootstrap();
            MiniTransaction corrupt = context.mtrManager.begin();
            ChangeBufferHeaderSnapshot header = context.headerRepository.readForUpdate(corrupt);
            context.headerRepository.write(corrupt, new ChangeBufferHeaderSnapshot(
                    header.state(), header.configuredMode(),
                    PageId.of(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID, PageNo.of(5)),
                    header.rootLevel(), header.indexId(), header.leafSegment(), header.nonLeafSegment(),
                    header.nextSequence(), header.pendingOperations(), header.formatEpoch()));
            context.mtrManager.commit(corrupt);

            ChangeBufferRecoveryValidator validator = context.recoveryValidator();
            assertThrows(ChangeBufferFormatException.class, validator::validateAfterRedo);
            assertEquals(0L, validator.validatedPendingOperations());
        });
    }

    /** worker 目标加载失败必须保留 pending 证据、进入 FAILED，并把带原始 cause 的 fatal 发布给组合根。 */
    @Test
    void workerFailurePublishesFatalWithoutConsumingEvidence() {
        withContext(context -> {
            context.bootstrap();
            ChangeBufferStore store = context.store();
            PageId unopenedTarget = PageId.of(USER_SPACE, PageNo.of(9));
            MiniTransaction append = context.mtrManager.begin();
            store.append(append, unopenedTarget, 7L, 3L, 11L,
                    ChangeBufferOperation.INSERT, new byte[]{1});
            context.mtrManager.commit(append);

            AtomicReference<cn.zhangyis.db.common.exception.DatabaseFatalException> published =
                    new AtomicReference<>();
            ChangeBufferMergeWorker worker = new ChangeBufferMergeWorker(
                    new ChangeBufferConfig(ChangeBufferMode.ALL, 25, Duration.ofMillis(10),
                            1, Duration.ofSeconds(1), Duration.ofSeconds(2)),
                    store, context.mtrManager, context.pool, published::set);
            try (worker) {
                worker.start();
                worker.requestMerge();
                assertTrue(awaitWorkerState(worker, ChangeBufferWorkerState.FAILED,
                        Duration.ofSeconds(2)));
            }

            assertNotNull(published.get());
            assertTrue(published.get() instanceof ChangeBufferWorkerFailureException);
            assertNotNull(published.get().getCause());
            MiniTransaction verify = context.mtrManager.beginReadOnly();
            assertEquals(1L, store.pendingOperations(verify));
            context.mtrManager.commit(verify);
        });
    }

    /** 使用有界轮询观察 daemon 生命周期；测试线程不做无界等待。 */
    private static boolean awaitWorkerState(ChangeBufferMergeWorker worker,
                                            ChangeBufferWorkerState expected,
                                            Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (worker.state() == expected) {
                return true;
            }
            LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
        }
        return worker.state() == expected;
    }

    private void withContext(TestBody body) {
        try (PageStore pageStore = new FileChannelPageStore();
             LruBufferPool pool = new LruBufferPool(pageStore, PAGE_SIZE, 64)) {
            Context context = new Context(pageStore, pool);
            body.run(context);
        }
    }

    @FunctionalInterface
    private interface TestBody {
        void run(Context context);
    }

    /** 测试所需协作者的单一组合根，确保仓储共用同一 pool/MTR/space manager。 */
    private final class Context {
        private final PageStore pageStore;
        private final LruBufferPool pool;
        private final MiniTransactionManager mtrManager = new MiniTransactionManager();
        private final DiskSpaceManager disk;
        private final IndexPageAccess pageAccess;
        private final ChangeBufferHeaderRepository headerRepository;
        private final ChangeBufferBitmapRepository bitmapRepository;
        private final SplitCapableBTreeIndexService btree;

        private Context(PageStore pageStore, LruBufferPool pool) {
            this.pageStore = pageStore;
            this.pool = pool;
            this.disk = new DiskSpaceManager(pool, pageStore, PAGE_SIZE);
            this.pageAccess = new IndexPageAccess(pool, PAGE_SIZE);
            this.headerRepository = new ChangeBufferHeaderRepository(pool, PAGE_SIZE);
            this.bitmapRepository = new ChangeBufferBitmapRepository(pool, PAGE_SIZE);
            this.btree = new SplitCapableBTreeIndexService(pageAccess, disk, new TypeCodecRegistry());
        }

        private BTreeIndex bootstrap() {
            MiniTransaction boot = mtrManager.begin();
            disk.createTablespace(boot, ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID,
                    tempDir.resolve("system.ibd"), PageNo.of(128), TablespaceType.SYSTEM);
            BTreeIndex index = new ChangeBufferBootstrap(disk, pageAccess, headerRepository, PAGE_SIZE)
                    .initialize(boot, ChangeBufferMode.ALL);
            mtrManager.commit(boot);
            return index;
        }

        private void createUserSpace() {
            MiniTransaction create = mtrManager.begin();
            disk.createTablespace(create, USER_SPACE, tempDir.resolve("user.ibd"),
                    PageNo.of(64), TablespaceType.GENERAL);
            mtrManager.commit(create);
        }

        private ChangeBufferStore store() {
            return new ChangeBufferStore(headerRepository, btree, PAGE_SIZE);
        }

        private ChangeBufferRecoveryValidator recoveryValidator() {
            return new ChangeBufferRecoveryValidator(headerRepository, store(), bitmapRepository,
                    mtrManager, PAGE_SIZE);
        }
    }
}
