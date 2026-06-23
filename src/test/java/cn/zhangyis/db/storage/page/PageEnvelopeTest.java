package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.domain.Lsn;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** PageEnvelope：header 往返 + stampPageLsn/readPageLsn 单字段往返。 */
class PageEnvelopeTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void headerAndPageLsnRoundTrip() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            PageId pid = PageId.of(SPACE, PageNo.of(3));
            try (PageGuard g = pool.getPage(pid, PageLatchMode.EXCLUSIVE)) {
                FilePageHeader h = new FilePageHeader(SPACE, 3, FilePageHeader.FIL_NULL,
                        FilePageHeader.FIL_NULL, 0, PageType.INDEX);
                PageEnvelope.writeHeader(g, h);
                assertEquals(h, PageEnvelope.readHeader(g));

                PageEnvelope.stampPageLsn(g, Lsn.of(12345));
                assertEquals(Lsn.of(12345), PageEnvelope.readPageLsn(g));
            }
        }
    }
}
