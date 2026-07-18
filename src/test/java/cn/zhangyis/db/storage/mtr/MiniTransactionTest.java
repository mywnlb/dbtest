package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.BufferPoolExhaustedException;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.FlushResultStatus;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MiniTransaction 集成测试：用 buf+fil 真实驱动，固定 commit/rollback/savepoint 释放、终态保护、写后落盘。
 */
class MiniTransactionTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final int PAYLOAD_OFFSET = 100;

    @TempDir
    Path dir;

    private PageStore openStore(int pages) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return store;
    }

    private PageId page(long n) {
        return PageId.of(SPACE, PageNo.of(n));
    }

    private MiniTransaction activeMtr(long id) {
        MiniTransaction mtr = new MiniTransaction(id, new RedoLogManager());
        mtr.activate();
        return mtr;
    }

    private MiniTransaction activeMtr(long id, RedoLogManager redo) {
        MiniTransaction mtr = new MiniTransaction(id, redo);
        mtr.activate();
        return mtr;
    }

    /**
     * 验证 {@code commitShouldReleaseHeldFixes} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void commitShouldReleaseHeldFixes() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 1);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(1), PageLatchMode.SHARED));
            mtr.commit();
            assertEquals(MiniTransactionState.COMMITTED, mtr.state());
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.SHARED)) {
                assertEquals(page(1), g.pageId());
            }
            pool.close();
        }
    }

    /**
     * 验证 {@code savepointShouldReleaseResourcesAcquiredAfterIt} 对应的Mini Transaction行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void savepointShouldReleaseResourcesAcquiredAfterIt() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 2);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            MtrSavepoint sp = mtr.savepoint();
            mtr.getPage(pool, page(1), PageLatchMode.SHARED);
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(2), PageLatchMode.SHARED));
            mtr.rollbackToSavepoint(sp);
            try (PageGuard g = pool.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(page(2), g.pageId());
            }
            mtr.commit();
            pool.close();
        }
    }

    /**
     * 验证 {@code rollbackUncommittedShouldReleaseAndTerminate} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void rollbackUncommittedShouldReleaseAndTerminate() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 1);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            mtr.rollbackUncommitted();
            assertEquals(MiniTransactionState.ROLLED_BACK, mtr.state());
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.SHARED)) {
                assertEquals(page(1), g.pageId());
            }
            pool.close();
        }
    }

    /**
     * 验证 {@code sharedExclusiveToExclusiveUpgradeIsForbidden} 对应的Mini Transaction行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void sharedExclusiveToExclusiveUpgradeIsForbidden() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED_EXCLUSIVE);
            // SX 持该页 readLock，同 MTR 再求 X 会在 ReentrantReadWriteLock 上自死锁 → 取 latch 前拦为领域异常。
            assertThrows(MtrStateException.class,
                    () -> mtr.getPage(pool, page(0), PageLatchMode.EXCLUSIVE));
            mtr.rollbackUncommitted();
            assertEquals(MiniTransactionState.ROLLED_BACK, mtr.state());
            pool.close();
        }
    }

    /**
     * 验证 {@code operationsAfterTerminalShouldThrow} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void operationsAfterTerminalShouldThrow() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            mtr.commit();
            assertThrows(MtrStateException.class, () -> mtr.getPage(pool, page(0), PageLatchMode.SHARED));
            assertThrows(MtrStateException.class, mtr::savepoint);
            assertThrows(MtrStateException.class, mtr::commit);
            pool.close();
        }
    }

    /**
     * 验证 {@code writesShouldBeDirtyAndPersistAfterCommitAndFlush} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
    @Test
    void writesShouldBeDirtyAndPersistAfterCommitAndFlush() {
        try (PageStore store = openStore(8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("mtr-write.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1, redo);
            PageGuard g = mtr.getPage(pool, page(2), PageLatchMode.EXCLUSIVE);
            g.writeInt(PAYLOAD_OFFSET, 0x99);
            mtr.commit();
            redo.flush();
            assertEquals(FlushResultStatus.CLEAN, flushPage(pool, store, redo, page(2)));

            BufferPool pool2 = new LruBufferPool(store, PS, 4);
            try (PageGuard r = pool2.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(0x99, r.readInt(PAYLOAD_OFFSET));
            }
            pool.close();
            pool2.close();
        }
    }

    /**
     * 验证 {@code savepointFromAnotherMtrShouldBeRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void savepointFromAnotherMtrShouldBeRejected() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtrA = activeMtr(1);
            MiniTransaction mtrB = activeMtr(2);
            MtrSavepoint spA = mtrA.savepoint();
            assertThrows(MtrStateException.class, () -> mtrB.rollbackToSavepoint(spA));
            mtrA.commit();
            mtrB.commit();
            pool.close();
        }
    }

    /**
     * 验证 {@code newPageShouldHoldZeroFrameAndPersistAfterCommit} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void newPageShouldHoldZeroFrameAndPersistAfterCommit() {
        try (PageStore store = openStore(8);
             RedoLogFileRepository repo = RedoLogFileRepository.open(dir.resolve("mtr-new-page.log"))) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1, redo);
            PageGuard g = mtr.newPage(pool, page(3), PageLatchMode.EXCLUSIVE, PageType.INDEX);
            assertEquals(0, g.readInt(PAYLOAD_OFFSET)); // 新页为零，未读盘
            g.writeInt(PAYLOAD_OFFSET, 0x55);
            mtr.commit();
            redo.flush();
            assertEquals(FlushResultStatus.CLEAN, flushPage(pool, store, redo, page(3)));

            BufferPool pool2 = new LruBufferPool(store, PS, 4);
            try (PageGuard r = pool2.getPage(page(3), PageLatchMode.SHARED)) {
                assertEquals(0x55, r.readInt(PAYLOAD_OFFSET));
            }
            pool.close();
            pool2.close();
        }
    }

    /**
     * 验证 {@code rollbackToSavepointShouldRejectNull} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rollbackToSavepointShouldRejectNull() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            assertThrows(DatabaseValidationException.class, () -> mtr.rollbackToSavepoint(null));
            mtr.commit();
            pool.close();
        }
    }

    /** 通过生产 flush 协调器写出 MTR 修改页；调用方必须先 flush 对应 redo 以满足 WAL gate。 */
    private FlushResultStatus flushPage(BufferPool pool, PageStore store, RedoLogManager redo, PageId pageId) {
        FlushCoordinator coordinator = new FlushCoordinator(pool, store, redo, PS,
                new NoDoublewriteStrategy(), Duration.ofMillis(50));
        return coordinator.singlePageFlush(pageId).status();
    }

    /**
     * 同一 MTR 内对同一页先取 S 再取 X 必须被显式拒绝。
     *
     * <p>底层 page latch 是 {@code ReentrantReadWriteLock}，不支持读→写升级；若放任 fix 直接 latch.lock()，
     * 持读锁的线程再求写锁会自死锁（永久阻塞）。因此 fix 必须在取 latch 前检测并抛 {@link MtrStateException}，
     * 把"会挂死的并发误用"转成"可观测的领域异常"。断言只确认拒绝发生，不触发真正的阻塞 latch 获取。
     */
    @Test
    void sameMtrShouldRejectSharedThenExclusiveOnSamePage() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            assertThrows(MtrStateException.class, () -> mtr.getPage(pool, page(0), PageLatchMode.EXCLUSIVE));
            mtr.rollbackUncommitted();
            pool.close();
        }
    }

    /**
     * 0.23a：普通多页获取必须按 PageId 升序。先持有高页号再获取低页号会形成与其它线程的低→高路径相反的等待边，
     * 因此应在 MTR 层直接转成领域异常，而不是让线程进入可能永久阻塞的 page latch 等待。
     */
    @Test
    void independentPageLatchMustBeAscending() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            mtr.getPage(pool, page(5), PageLatchMode.SHARED);
            assertThrows(MtrStateException.class, () -> mtr.getPage(pool, page(4), PageLatchMode.SHARED));
            mtr.rollbackUncommitted();
            pool.close();
        }
    }

    /**
     * 0.23a：顺序守卫只约束“当前仍持有的不同页”之间的新增 latch。重复获取同一页是重入；高页释放后，
     * memo 中已没有该页，后续再获取较低页不构成 hold-and-wait 环。
     */
    @Test
    void samePageAndReleasedPageDoNotViolateOrder() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction same = activeMtr(1);
            same.getPage(pool, page(4), PageLatchMode.SHARED);
            same.getPage(pool, page(4), PageLatchMode.SHARED);
            same.rollbackUncommitted();

            MiniTransaction released = activeMtr(2);
            PageGuard high = released.getPage(pool, page(5), PageLatchMode.SHARED);
            released.releaseLatch(page(5), high);
            released.getPage(pool, page(4), PageLatchMode.SHARED);
            released.rollbackUncommitted();
            pool.close();
        }
    }

    /**
     * 0.23a：B+Tree sibling/FIL 右邻等已有局部死锁证明的路径可以显式进入 out-of-order scope；
     * scope 关闭后 MTR 立即恢复默认升序约束，避免例外泄漏到后续普通页访问。
     */
    @Test
    void outOfOrderScopeAllowsProvenLocalExceptionOnlyInsideScope() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 6);
            MiniTransaction scoped = activeMtr(1);
            try (var ignored = scoped.allowOutOfOrderPageLatch("test-proven-sibling-path")) {
                scoped.getPage(pool, page(5), PageLatchMode.SHARED);
                scoped.getPage(pool, page(4), PageLatchMode.SHARED);
            }
            scoped.rollbackUncommitted();

            MiniTransaction normal = activeMtr(2);
            normal.getPage(pool, page(5), PageLatchMode.SHARED);
            assertThrows(MtrStateException.class, () -> normal.getPage(pool, page(4), PageLatchMode.SHARED));
            normal.rollbackUncommitted();
            pool.close();
        }
    }

    /**
     * 用 savepoint 提前释放 S guard 后，再对同一页取 X 应被允许（不是真正的 S→X 升级）。
     *
     * <p>rollbackToSavepoint 已把 S guard 弹出 memo 并 close，memo 不再持有该页 S latch，
     * 因此 fix 的 S→X 检测看不到残留 S，X 可正常获取。该用例固定"释放后重取"这条合法路径不被误杀。
     */
    @Test
    void exclusiveAfterSavepointReleaseShouldBeAllowed() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            MtrSavepoint sp = mtr.savepoint();
            mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            mtr.rollbackToSavepoint(sp);
            PageGuard g = mtr.getPage(pool, page(0), PageLatchMode.EXCLUSIVE);
            g.writeInt(0, 0x12345678);
            mtr.commit();
            pool.close();
        }
    }

    /**
     * 选择性提前释放（latch coupling crab 早释放）：MTR 仍在 ACTIVE 时对某个已固定页调用 releaseLatch，
     * 应立即放掉它的 page latch + buffer fix（不必等 commit）。用 1 帧池证明：释放后该帧可被另一页复用。
     */
    @Test
    void releaseLatchShouldFreeFrameBeforeCommit() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 1); // 仅 1 帧
            MiniTransaction mtr = activeMtr(1);
            PageGuard g = mtr.getPage(pool, page(0), PageLatchMode.SHARED);
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(1), PageLatchMode.SHARED)); // 池满
            mtr.releaseLatch(page(0), g); // 提前选择性释放
            try (PageGuard r = pool.getPage(page(1), PageLatchMode.SHARED)) {
                assertEquals(page(1), r.pageId()); // 帧已复用
            }
            mtr.commit(); // memo 已空，正常收尾
            assertEquals(MiniTransactionState.COMMITTED, mtr.state());
            pool.close();
        }
    }

    /**
     * 已写（touched）页的 <b>X</b> guard 禁止提前释放：commit 需据 touchedPages 用它盖 pageLSN，
     * 提前放掉盖戳时 guardFor 取不到。releaseLatch 必须拒绝。
     */
    @Test
    void releaseLatchOnWrittenPageExclusiveGuardShouldThrow() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            PageGuard g = mtr.getPage(pool, page(2), PageLatchMode.EXCLUSIVE);
            g.writeInt(0, 0x99); // 该页 touched，X 持有
            assertThrows(MtrStateException.class, () -> mtr.releaseLatch(page(2), g));
            mtr.rollbackUncommitted();
            pool.close();
        }
    }

    /**
     * 同一 MTR 多算子协作：前一算子已写并 X 持有某页，后一算子的乐观 crab 以 SHARED 可重入重开同页再提前释放该 SHARED guard。
     * 这是安全的——touched 页的 X guard 仍在 memo，commit 仍能盖 pageLSN。releaseLatch 只拦 X guard，不得误拦 SHARED。
     */
    @Test
    void releaseLatchOfSharedGuardOnWrittenPageShouldSucceed() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            PageGuard x = mtr.getPage(pool, page(2), PageLatchMode.EXCLUSIVE);
            x.writeInt(0, 0x99); // page 2 touched，X 持有
            PageGuard s = mtr.getPage(pool, page(2), PageLatchMode.SHARED); // 可重入 S 重开同页（模拟多算子 crab）
            mtr.releaseLatch(page(2), s); // 释放 SHARED guard：安全，X guard 仍在
            mtr.commit(); // 仍能给 touched 页盖 pageLSN
            assertEquals(MiniTransactionState.COMMITTED, mtr.state());
            pool.close();
        }
    }
}
