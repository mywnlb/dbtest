package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** FilePageHeader 经 PageEnvelope + 真实 PageGuard 编解码往返（含邻居与 FIL_NULL）+ 构造校验。 */
class FilePageHeaderTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void roundTripWithNeighbors() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                FilePageHeader h = new FilePageHeader(SPACE, 3, 2, 7, 0L, PageType.INDEX);
                PageEnvelope.writeHeader(g, h);
                assertEquals(h, PageEnvelope.readHeader(g));
            }
        }
    }

    @Test
    void roundTripWithFilNullNeighbors() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                FilePageHeader h = new FilePageHeader(SPACE, 3,
                        FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.FSP_HDR);
                PageEnvelope.writeHeader(g, h);
                FilePageHeader got = PageEnvelope.readHeader(g);
                assertEquals(h, got);
                assertEquals(FilePageHeader.FIL_NULL, got.prevPageNo());
                assertEquals(FilePageHeader.FIL_NULL, got.nextPageNo());
            }
        }
    }

    @Test
    void constructorRejectsNulls() {
        assertThrows(DatabaseValidationException.class, () -> new FilePageHeader(
                null, 1, FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INDEX));
        assertThrows(DatabaseValidationException.class, () -> new FilePageHeader(
                SPACE, 1, FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, null));
    }
}
