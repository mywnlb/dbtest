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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** PageGuard.attachWriteListener：writeInt/writeBytes 回调上报实际字节；默认无 listener 不回调。 */
class PageGuardWriteListenerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private record Write(PageId pageId, int offset, byte[] bytes) { }

    @Test
    void reportsWritesAfterAttach() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        PageId pid = PageId.of(SPACE, PageNo.of(3));
        List<Write> seen = new ArrayList<>();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(pid, PageLatchMode.EXCLUSIVE)) {
                g.attachWriteListener((p, off, b) -> seen.add(new Write(p, off, b)));
                g.writeInt(100, 0x01020304);
                g.writeBytes(200, new byte[]{9, 8, 7});
            }
        }
        assertEquals(2, seen.size());
        assertEquals(pid, seen.get(0).pageId());
        assertEquals(100, seen.get(0).offset());
        assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04}, seen.get(0).bytes());
        assertEquals(200, seen.get(1).offset());
        assertArrayEquals(new byte[]{9, 8, 7}, seen.get(1).bytes());
    }

    @Test
    void noListenerByDefaultMeansNoCallback() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        PageId pid = PageId.of(SPACE, PageNo.of(3));
        int[] count = {0};
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(pid, PageLatchMode.EXCLUSIVE)) {
                g.writeInt(100, 42); // 未 attach → NO_OP
            }
        }
        assertEquals(0, count[0]);
    }
}
