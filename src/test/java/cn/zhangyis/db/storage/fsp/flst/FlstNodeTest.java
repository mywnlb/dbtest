package cn.zhangyis.db.storage.fsp.flst;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FlstNode 测试：null 拒绝、经真实 PageGuard 编解码往返（含全零→(NULL,NULL)）。
 */
class FlstNodeTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void shouldRejectNullPointers() {
        assertThrows(DatabaseValidationException.class, () -> new FlstNode(null, FileAddress.NULL));
        assertThrows(DatabaseValidationException.class, () -> new FlstNode(FileAddress.NULL, null));
    }

    @Test
    void shouldRoundTripThroughPageGuard() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                FlstNode n = new FlstNode(FileAddress.of(PageNo.of(0), 100), FileAddress.of(PageNo.of(0), 200));
                n.writeTo(g, 300);
                assertEquals(n, FlstNode.readFrom(g, 300));

                assertEquals(new FlstNode(FileAddress.NULL, FileAddress.NULL), FlstNode.readFrom(g, 400));
                assertTrue(FlstNode.readFrom(g, 400).prev().isNull());
            }
        }
    }
}
