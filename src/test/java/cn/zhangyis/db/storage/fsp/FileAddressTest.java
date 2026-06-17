package cn.zhangyis.db.storage.fsp;

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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FileAddress 测试：of/NULL/isNull/equals、(0,0) 碰撞拒绝；writeTo/readFrom 经真实 PageGuard 往返（含 NULL）。
 */
class FileAddressTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void ofAndNullSemantics() {
        FileAddress a = FileAddress.of(PageNo.of(5), 100);
        assertFalse(a.isNull());
        assertEquals(PageNo.of(5), a.pageNo());
        assertEquals(100, a.offset());
        assertTrue(FileAddress.NULL.isNull());
        assertEquals(FileAddress.NULL, FileAddress.NULL);
        assertEquals(FileAddress.of(PageNo.of(5), 100), a);
    }

    @Test
    void shouldRejectReservedZeroAndNegativeOffset() {
        assertThrows(DatabaseValidationException.class, () -> FileAddress.of(PageNo.of(0), 0));
        assertThrows(DatabaseValidationException.class, () -> FileAddress.of(PageNo.of(5), -1));
        assertThrows(DatabaseValidationException.class, FileAddress.NULL::pageNo);
    }

    @Test
    void shouldRoundTripThroughPageGuard() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(3));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(0)), PageLatchMode.EXCLUSIVE)) {
                FileAddress a = FileAddress.of(PageNo.of(7), 250);
                a.writeTo(g, 100);
                assertEquals(a, FileAddress.readFrom(g, 100));

                FileAddress.NULL.writeTo(g, 200);
                assertTrue(FileAddress.readFrom(g, 200).isNull());
            }
        }
    }
}
