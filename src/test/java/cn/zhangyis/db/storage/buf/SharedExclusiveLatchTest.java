package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SHARED_EXCLUSIVE（SIX，0.13d SX latch）page latch 兼容矩阵验收：SX 与 SHARED 并发，但排斥其它
 * SHARED_EXCLUSIVE 与 EXCLUSIVE；SX 只授予读权限（写须 EXCLUSIVE）。用后台线程 + 有界等待观测「兼容=立即拿到、
 * 冲突=在短窗内拿不到，释放持有者后拿到」，不做时序假设、只判是否在窗口内被授予。
 */
class SharedExclusiveLatchTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    /** 兼容判定：请求方应在此宽松窗内被授予。 */
    private static final long GRANT_MS = 2000;
    /** 冲突判定：请求方在此短窗内应仍被阻塞（未授予）。 */
    private static final long BLOCKED_MS = 200;

    @TempDir
    Path dir;

    @Test
    void sharedAndSharedExclusiveCoexistEitherOrder() {
        onPool(4, (pool, p) -> {
            LatchHolder sxFirst = new LatchHolder(pool, p, PageLatchMode.SHARED_EXCLUSIVE);
            assertTrue(sxFirst.acquiredWithin(GRANT_MS), "SX acquires on a free page");
            LatchHolder sReader = new LatchHolder(pool, p, PageLatchMode.SHARED);
            assertTrue(sReader.acquiredWithin(GRANT_MS), "S coexists with a held SX");
            sReader.releaseAndJoin();
            sxFirst.releaseAndJoin();

            LatchHolder sFirst = new LatchHolder(pool, p, PageLatchMode.SHARED);
            assertTrue(sFirst.acquiredWithin(GRANT_MS));
            LatchHolder sxSecond = new LatchHolder(pool, p, PageLatchMode.SHARED_EXCLUSIVE);
            assertTrue(sxSecond.acquiredWithin(GRANT_MS), "SX coexists with a held S");
            sxSecond.releaseAndJoin();
            sFirst.releaseAndJoin();
        });
    }

    @Test
    void sharedExclusiveExcludesAnotherSharedExclusive() {
        onPool(4, (pool, p) -> {
            LatchHolder sx1 = new LatchHolder(pool, p, PageLatchMode.SHARED_EXCLUSIVE);
            assertTrue(sx1.acquiredWithin(GRANT_MS));
            LatchHolder sx2 = new LatchHolder(pool, p, PageLatchMode.SHARED_EXCLUSIVE);
            assertFalse(sx2.acquiredWithin(BLOCKED_MS), "a second SX must block while the first is held");
            sx1.releaseAndJoin();
            assertTrue(sx2.acquiredWithin(GRANT_MS), "second SX is granted once the first releases");
            sx2.releaseAndJoin();
        });
    }

    @Test
    void sharedExclusiveExcludesExclusive() {
        onPool(4, (pool, p) -> {
            LatchHolder sx = new LatchHolder(pool, p, PageLatchMode.SHARED_EXCLUSIVE);
            assertTrue(sx.acquiredWithin(GRANT_MS));
            LatchHolder x = new LatchHolder(pool, p, PageLatchMode.EXCLUSIVE);
            assertFalse(x.acquiredWithin(BLOCKED_MS), "X must block while SX is held");
            sx.releaseAndJoin();
            assertTrue(x.acquiredWithin(GRANT_MS), "X is granted once SX releases");
            x.releaseAndJoin();
        });
    }

    @Test
    void exclusiveExcludesSharedExclusive() {
        onPool(4, (pool, p) -> {
            LatchHolder x = new LatchHolder(pool, p, PageLatchMode.EXCLUSIVE);
            assertTrue(x.acquiredWithin(GRANT_MS));
            LatchHolder sx = new LatchHolder(pool, p, PageLatchMode.SHARED_EXCLUSIVE);
            assertFalse(sx.acquiredWithin(BLOCKED_MS), "SX must block while X is held");
            x.releaseAndJoin();
            assertTrue(sx.acquiredWithin(GRANT_MS), "SX is granted once X releases");
            sx.releaseAndJoin();
        });
    }

    @Test
    void sharedExclusiveGrantsReadOnlyContentAccess() {
        onPool(4, (pool, p) -> {
            PageGuard g = pool.getPage(p, PageLatchMode.SHARED_EXCLUSIVE);
            try {
                g.readInt(0); // 读允许
                assertThrows(DatabaseValidationException.class, () -> g.writeInt(0, 1),
                        "SX grants read-only content access; writes require EXCLUSIVE");
            } finally {
                g.close();
            }
        });
    }

    private void onPool(int capacity, BiConsumer<BufferPool, PageId> body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, capacity)) {
            body.accept(pool, PageId.of(SPACE, PageNo.of(3)));
        }
    }

    /**
     * 后台线程持有一把 page latch 直到收到释放信号；latch 的 lock/unlock 必须同线程，故获取与释放都在该线程内做。
     * {@link #acquiredWithin} 观测是否在窗口内被授予（阻塞时返回 false），{@link #releaseAndJoin} 触发释放并等线程收尾。
     */
    private static final class LatchHolder {
        private final CountDownLatch acquired = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch done = new CountDownLatch(1);
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final Thread thread;

        LatchHolder(BufferPool pool, PageId pageId, PageLatchMode mode) {
            this.thread = new Thread(() -> {
                try (PageGuard g = pool.getPage(pageId, mode)) {
                    acquired.countDown();
                    release.await();
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            }, "sx-latch-holder-" + mode);
            this.thread.setDaemon(true);
            this.thread.start();
        }

        boolean acquiredWithin(long ms) {
            return await(acquired, ms);
        }

        void releaseAndJoin() {
            release.countDown();
            if (!await(done, 2000)) {
                throw new AssertionError("latch holder did not finish after release");
            }
            if (error.get() != null) {
                throw new AssertionError("latch holder thread failed", error.get());
            }
        }

        private static boolean await(CountDownLatch latch, long ms) {
            try {
                return latch.await(ms, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while awaiting latch", e);
            }
        }
    }
}
