package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * legacy BufferPool flush API 移除验收测试。
 *
 * <p>Buffer Pool 只暴露 dirty view 与 snapshot/callback 原语；真正写盘必须通过 FlushCoordinator/FlushService，
 * 否则会绕过 WAL gate、checksum、doublewrite 和 data-file force。
 */
class BufferPoolLegacyFlushRemovalTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(92);

    @TempDir
    Path dir;

    /** public BufferPool API 不再暴露可绕过 flush 模块的直写入口。 */
    @Test
    void bufferPoolInterfaceDoesNotExposeLegacyFlushMethods() {
        assertFalse(hasPublicMethod("flush"), "BufferPool.flush(PageId) must be removed");
        assertFalse(hasPublicMethod("flushAll"), "BufferPool.flushAll() must be removed");
    }

    /** 未注入 WAL-safe DirtyVictimFlusher 时，脏页淘汰必须显式失败，不能 fallback 直写 PageStore。 */
    @Test
    void dirtyVictimWithoutFlusherFailsInsteadOfWritingPageStoreDirectly() {
        PageId dirty = page(0);
        PageId incoming = page(1);
        try (PageStore store = openStore(4);
             LruBufferPool pool = new LruBufferPool(store, PS, 1)) {
            dirtyPage(pool, dirty, Lsn.of(10));

            assertThrows(BufferPoolExhaustedException.class,
                    () -> loadClean(pool, incoming));
        }
    }

    private static boolean hasPublicMethod(String name) {
        return Arrays.stream(BufferPool.class.getMethods())
                .map(Method::getName)
                .anyMatch(name::equals);
    }

    private PageStore openStore(int pages) {
        FileChannelPageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("legacy-removal.ibu"), PS, PageNo.of(pages));
        return store;
    }

    private static PageId page(long pageNo) {
        return PageId.of(SPACE, PageNo.of(pageNo));
    }

    private static void dirtyPage(BufferPool pool, PageId pageId, Lsn lsn) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.EXCLUSIVE)) {
            guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
            guard.writeInt(100, 0xCAFE);
        }
    }

    private static void loadClean(BufferPool pool, PageId pageId) {
        try (PageGuard guard = pool.getPage(pageId, PageLatchMode.SHARED)) {
            guard.readInt(0);
        }
    }
}
