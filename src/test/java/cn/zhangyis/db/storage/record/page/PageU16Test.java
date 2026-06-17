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

/** 页内 u16 大端读写经真实 PageGuard 往返；越界值被拒。 */
class PageU16Test {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void roundTripAndRangeCheck() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                PageU16.put(g, 100, 0xABCD);
                assertEquals(0xABCD, PageU16.get(g, 100));
                PageU16.put(g, 102, 0);
                assertEquals(0, PageU16.get(g, 102));
                PageU16.put(g, 104, 0xFFFF);
                assertEquals(0xFFFF, PageU16.get(g, 104));
                assertThrows(DatabaseValidationException.class, () -> PageU16.put(g, 106, -1));
                assertThrows(DatabaseValidationException.class, () -> PageU16.put(g, 106, 0x10000));
            }
        }
    }
}
