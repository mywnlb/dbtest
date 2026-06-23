package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 截断前 Buffer Pool 排空测试：等待 fix、拒绝脏帧、成功后彻底移除该空间驻留页。 */
class BufferPoolTablespaceInvalidationTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(80);

    @TempDir
    Path dir;

    @Test
    void waitsForFixedFrameThenEvictsAllSpaceFrames() throws Exception {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("wait.ibu"), PS, PageNo.of(2));
            PageGuard guard = pool.getPage(PageId.of(SPACE, PageNo.of(0)), PageLatchMode.SHARED);
            CompletableFuture<Void> invalidation = CompletableFuture.runAsync(() ->
                    pool.invalidateTablespace(SPACE, Duration.ofSeconds(2)));

            TimeUnit.MILLISECONDS.sleep(100);
            assertFalse(invalidation.isDone());
            guard.close();
            invalidation.get(1, TimeUnit.SECONDS);
            assertEquals(0, pool.residentCount());
        }
    }

    @Test
    void rejectsDirtyFrameInsteadOfSilentlyDroppingIt() {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("dirty.ibu"), PS, PageNo.of(2));
            try (PageGuard guard = pool.newPage(PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE)) {
                guard.writeInt(100, 1);
            }
            assertThrows(DirtyTablespaceInvalidationException.class,
                    () -> pool.invalidateTablespace(SPACE, Duration.ofSeconds(1)));
            assertEquals(1, pool.residentCount());
        }
    }

    @Test
    void fixedFrameWaitHasTimeout() {
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 4)) {
            store.create(SPACE, dir.resolve("timeout.ibu"), PS, PageNo.of(2));
            try (PageGuard ignored = pool.getPage(PageId.of(SPACE, PageNo.of(0)), PageLatchMode.SHARED)) {
                assertThrows(BufferPoolInvalidationTimeoutException.class,
                        () -> pool.invalidateTablespace(SPACE, Duration.ofMillis(30)));
            }
        }
    }
}
