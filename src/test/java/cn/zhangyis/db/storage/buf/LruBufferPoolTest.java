package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
    void attachVictimFlusherRejectsNullAndRepeat() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 2);
            assertThrows(DatabaseValidationException.class, () -> pool.attachVictimFlusher(null));
            DirtyVictimFlusher flusher = pageId -> true;
            pool.attachVictimFlusher(flusher);
            // set-once：重复注入必须拒绝，避免运行期换刷盘实现造成不一致。
            assertThrows(DatabaseValidationException.class, () -> pool.attachVictimFlusher(flusher));
        }
    }

    /** 注入 flusher 后，淘汰脏 victim 必须经端口刷干净再复用帧，不在锁内直接 writeBack。 */
    @Test
    void dirtyVictimEvictedThroughAttachedFlusher() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 1);
            FakeVictimFlusher flusher = new FakeVictimFlusher(pool);
            pool.attachVictimFlusher(flusher);
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xAA);
            }
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xBB);
            }
            assertEquals(List.of(page(0)), flusher.calls);
            assertEquals(1, pool.residentCount());
        }
    }

    /**
     * 核心正确性：flusher 返回 false（模拟 redo 未 durable）时脏 victim 不得被写盘，且本轮只尝试一次
     * （skip set 防空转），无干净帧可用即抛耗尽——WAL 不被破坏。
     */
    @Test
    void dirtyVictimNotWrittenAndExhaustsWhenFlusherReturnsFalse() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 1);
            FakeVictimFlusher flusher = new FakeVictimFlusher(pool);
            flusher.succeed = false;
            pool.attachVictimFlusher(flusher);
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xAA);
            }
            assertThrows(BufferPoolExhaustedException.class,
                    () -> pool.getPage(page(1), PageLatchMode.EXCLUSIVE));
            assertEquals(List.of(page(0)), flusher.calls);
            byte[] disk = new byte[PS.bytes()];
            store.readPage(page(0), ByteBuffer.wrap(disk));
            assertEquals(0, ByteBuffer.wrap(disk).getInt(0), "dirty page must not reach disk via eviction");
        }
    }

    /** flusher 抛真 IO 失败必须向上传播，不能被吞成 BufferPoolExhaustedException 掩盖盘故障。 */
    @Test
    void flusherFailurePropagatesAndIsNotSwallowedAsExhaustion() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 1);
            FakeVictimFlusher flusher = new FakeVictimFlusher(pool);
            flusher.fail = true;
            pool.attachVictimFlusher(flusher);
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xAA);
            }
            DatabaseRuntimeException ex = assertThrows(DatabaseRuntimeException.class,
                    () -> pool.getPage(page(1), PageLatchMode.EXCLUSIVE));
            assertFalse(ex instanceof BufferPoolExhaustedException);
            assertTrue(ex.getMessage().contains("induced"));
        }
    }

    /** 干净 victim 直接复用，不应调用 flusher（只对脏帧走刷盘管线）。 */
    @Test
    void cleanVictimEvictedWithoutCallingFlusher() {
        try (PageStore store = openStore(8)) {
            LruBufferPool pool = new LruBufferPool(store, PS, 1);
            FakeVictimFlusher flusher = new FakeVictimFlusher(pool);
            pool.attachVictimFlusher(flusher);
            try (PageGuard g = pool.getPage(page(0), PageLatchMode.SHARED)) {
                g.readInt(0);
            }
            try (PageGuard g = pool.getPage(page(1), PageLatchMode.EXCLUSIVE)) {
                g.writeInt(0, 0xBB);
            }
            assertTrue(flusher.calls.isEmpty());
            assertEquals(1, pool.residentCount());
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

    /**
     * 测试用淘汰刷盘端口：用 Buffer Pool 真实 snapshot/completeFlush 模拟"刷干净"的副作用（不写盘、不走 WAL），
     * 以便聚焦验证淘汰逻辑（路由、复用、skip set、失败传播），不引入完整 flush 管线 infra。
     */
    private static final class FakeVictimFlusher implements DirtyVictimFlusher {
        private final BufferPool pool;
        private final List<PageId> calls = new ArrayList<>();
        private boolean succeed = true;
        private boolean fail = false;

        FakeVictimFlusher(BufferPool pool) {
            this.pool = pool;
        }

        @Override
        public boolean flushVictim(PageId pageId) {
            calls.add(pageId);
            if (fail) {
                throw new DatabaseRuntimeException("induced flush failure for " + pageId);
            }
            if (!succeed) {
                return false;
            }
            return pool.snapshotForFlush(pageId).map(pool::completeFlush).orElse(false);
        }
    }
}
