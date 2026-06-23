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
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MiniTransaction 集成测试：用 buf+fil 真实驱动，固定 commit/rollback/savepoint 释放、终态保护、写后落盘。
 */
class MiniTransactionTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

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

    @Test
    void writesShouldBeDirtyAndPersistAfterCommitAndFlush() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            PageGuard g = mtr.getPage(pool, page(2), PageLatchMode.EXCLUSIVE);
            g.writeInt(0, 0x99);
            mtr.commit();
            pool.flush(page(2));

            BufferPool pool2 = new LruBufferPool(store, PS, 4);
            try (PageGuard r = pool2.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(0x99, r.readInt(0));
            }
            pool.close();
            pool2.close();
        }
    }

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

    @Test
    void newPageShouldHoldZeroFrameAndPersistAfterCommit() {
        try (PageStore store = openStore(8)) {
            BufferPool pool = new LruBufferPool(store, PS, 4);
            MiniTransaction mtr = activeMtr(1);
            PageGuard g = mtr.newPage(pool, page(3), PageLatchMode.EXCLUSIVE, PageType.INDEX);
            assertEquals(0, g.readInt(0)); // 新页为零，未读盘
            g.writeInt(0, 0x55);
            mtr.commit();
            pool.flush(page(3));

            BufferPool pool2 = new LruBufferPool(store, PS, 4);
            try (PageGuard r = pool2.getPage(page(3), PageLatchMode.SHARED)) {
                assertEquals(0x55, r.readInt(0));
            }
            pool.close();
            pool2.close();
        }
    }

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
}
