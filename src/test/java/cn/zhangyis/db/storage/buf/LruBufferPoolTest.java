package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LruBufferPool 集成测试：用 FileChannelPageStore + 临时文件真实驱动，固定读写往返、LRU 淘汰+脏页写回+读穿、
 * 帧耗尽、newPage、flush 落盘。
 */
class LruBufferPoolTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private PageStore openStore(int pages) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(pages));
        return store;
    }

    private PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    @Test
    void shouldRoundTripPageThroughBufferPool() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard w = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                w.writeInt(0, 0xCAFEBABE);
            }
            try (PageGuard r = pool.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(0xCAFEBABE, r.readInt(0));
            }
            pool.close();
        }
    }

    @Test
    void shouldEvictLruWriteBackDirtyAndReReadFromDisk() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 2);
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xAA);
            }
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xBB);
            }
            try (PageGuard g = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xCC);
            }
            assertEquals(2, pool.residentCount());
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.SHARED)) {
                assertEquals(0xAA, g.readInt(0));
            }
            pool.close();
        }
    }

    @Test
    void shouldThrowWhenAllFramesFixed() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 1);
            PageGuard held = pool.getPage(page(0), PageLatchMode.SHARED);
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(1), PageLatchMode.SHARED));
            held.close();
            pool.close();
        }
    }

    @Test
    void newPageShouldNotReadDiskAndPersistAfterFlush() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard g = pool.newPage(page(3), PageLatchMode.EXCLUSIVE)) {
                assertEquals(0, g.readInt(0));
                g.writeInt(0, 0x1234);
            }
            pool.flush(page(3));
            LruBufferPool pool2 = new LruBufferPool(store, PS, 4);
            try (PageGuard g = pool2.getPage(page(3), PageLatchMode.SHARED)) {
                assertEquals(0x1234, g.readInt(0));
            }
            pool.close();
            pool2.close();
        }
    }

    @Test
    void newPageReinitializesResidentPage() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard g = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0x12345678); // page2 驻留且有内容
            }
            try (PageGuard g = pool.newPage(page(2), PageLatchMode.EXCLUSIVE)) {
                assertEquals(0, g.readInt(0), "resident page reinitialized to zero");
                g.writeInt(0, 0x55);
                assertEquals(0x55, g.readInt(0));
            }
            pool.close();
        }
    }

    @Test
    void newPageRejectsSharedMode() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            assertThrows(DatabaseValidationException.class,
                    () -> pool.newPage(page(2), PageLatchMode.SHARED));
            pool.close();
        }
    }

    @Test
    void newPageOnResidentBlocksUntilSharedReleasedThenZeroes() throws Exception {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            try (PageGuard w = pool.getPage(page(2), PageLatchMode.EXCLUSIVE)) {
                w.writeInt(0, 0xABCD); // page2 驻留且有非零内容
            }
            PageGuard shared = pool.getPage(page(2), PageLatchMode.SHARED); // A 持 S
            CountDownLatch bStarted = new CountDownLatch(1);
            AtomicInteger bReadAfter = new AtomicInteger(-1);
            AtomicBoolean bDone = new AtomicBoolean(false);
            Thread b = new Thread(() -> {
                bStarted.countDown();
                try (PageGuard g = pool.newPage(page(2), PageLatchMode.EXCLUSIVE)) { // 阻塞至 A 释放 S
                    bReadAfter.set(g.readInt(0));
                }
                bDone.set(true);
            });
            b.start();
            assertTrue(bStarted.await(2, TimeUnit.SECONDS));
            Thread.sleep(150); // 给 B 时间尝试取 X（应阻塞）
            assertFalse(bDone.get(), "newPage(X) must block while SHARED held");
            assertEquals(0xABCD, shared.readInt(0), "content must not be zeroed before X latch acquired");
            shared.close(); // 释放 A 的 S
            b.join(2000);
            assertTrue(bDone.get(), "newPage proceeds after S released");
            assertEquals(0, bReadAfter.get(), "newPage returns zeroed page");
            pool.close();
        }
    }

    @Test
    void fixCountShouldPreventEvictionUntilAllGuardsClosed() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 1);
            PageGuard g1 = pool.getPage(page(0), PageLatchMode.SHARED);
            PageGuard g2 = pool.getPage(page(0), PageLatchMode.SHARED); // 同页二次固定，fixCount=2
            g1.close(); // fixCount=1，仍被固定
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(1), PageLatchMode.SHARED));
            g2.close(); // fixCount=0，可淘汰
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.SHARED)) {
                assertEquals(1, pool.residentCount());
            }
            pool.close();
        }
    }

    @Test
    void flushShouldSkipFixedPageAndPersistAfterRelease() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 4);
            PageGuard g = pool.getPage(page(2), PageLatchMode.EXCLUSIVE);
            g.writeInt(0, 0x77);
            pool.flush(page(2)); // 帧仍 fix → 跳过，不写回

            // 另一个池经同一 PageStore 读盘：page2 仍是 0，证明 flush 跳过了 fixed 帧
            LruBufferPool probe = new LruBufferPool(store, PS, 4);
            try (PageGuard r = probe.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(0, r.readInt(0));
            }
            probe.close();

            g.close(); // 释放 → 未 fix（此时帧才被置脏）
            pool.flush(page(2)); // 现在写回

            LruBufferPool probe2 = new LruBufferPool(store, PS, 4);
            try (PageGuard r = probe2.getPage(page(2), PageLatchMode.SHARED)) {
                assertEquals(0x77, r.readInt(0));
            }
            probe2.close();
            pool.close();
        }
    }

    @Test
    void shouldRejectInvalidConstruction() {
        try (PageStore store = openStore(2)) {
            assertThrows(DatabaseValidationException.class, () -> new LruBufferPool(store, PS, 0));
            assertThrows(DatabaseValidationException.class, () -> new LruBufferPool(null, PS, 1));
        }
    }
}
