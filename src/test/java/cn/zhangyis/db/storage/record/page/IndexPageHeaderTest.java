package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** INDEX page header 经真实 PageGuard 编解码往返 + 构造校验。 */
class IndexPageHeaderTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void roundTripThroughGuard() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                IndexPageHeader h = new IndexPageHeader(5, 1024, 7, 200, 64, 512,
                        IndexPageDirection.RIGHT, 3, 5, 1, 0xCAFEL);
                h.writeTo(g);
                assertEquals(h, IndexPageHeader.readFrom(g));
            }
        }
    }

    @Test
    void constructorRejectsInvalidFields() {
        assertThrows(DatabaseValidationException.class, () -> header(0x10000, 98, 2)); // nDirSlots u16
        assertThrows(DatabaseValidationException.class, () -> header(2, 0x10000, 2)); // heapTop u16
        assertThrows(DatabaseValidationException.class, () -> header(1, 98, 2));      // nDirSlots < 2
        assertThrows(DatabaseValidationException.class, () -> header(2, 98, 1));      // nHeap < 2
        assertThrows(DatabaseValidationException.class, () -> new IndexPageHeader(2, 98, 2, 0, 0, 0,
                null, 0, 0, 0, 1));                                                   // direction null
        assertThrows(DatabaseValidationException.class, () -> new IndexPageHeader(2, 98, 2, 0, 0, 0,
                IndexPageDirection.NO_DIRECTION, 0, 0, 0, -1));                       // indexId < 0
    }

    private static IndexPageHeader header(int nDirSlots, int heapTop, int nHeap) {
        return new IndexPageHeader(nDirSlots, heapTop, nHeap, 0, 0, 0,
                IndexPageDirection.NO_DIRECTION, 0, 0, 0, 1L);
    }
}
