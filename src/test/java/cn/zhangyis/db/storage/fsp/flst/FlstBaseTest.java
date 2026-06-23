package cn.zhangyis.db.storage.fsp.flst;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;


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

/**
 * FlstBase 测试：构造校验（length>=0、first/last 非 null）、编解码往返、零态→EMPTY、解码空链一致性损坏拒绝。
 */
class FlstBaseTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void constructorValidates() {
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(-1, FileAddress.NULL, FileAddress.NULL));
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(0, null, FileAddress.NULL));
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(0, FileAddress.NULL, null));
        assertEquals(new FlstBase(0, FileAddress.NULL, FileAddress.NULL), FlstBase.EMPTY);
    }

    @Test
    void roundTripAndZeroDecodesEmpty() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                FlstBase b = new FlstBase(2, FileAddress.of(PageNo.of(0), 100), FileAddress.of(PageNo.of(0), 200));
                b.writeTo(g, 300);
                assertEquals(b, FlstBase.readFrom(g, 300));

                // 全零槽位解码为 EMPTY
                assertEquals(FlstBase.EMPTY, FlstBase.readFrom(g, 500));
            }
        }
    }

    @Test
    void decodeRejectsLengthEndpointInconsistency() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                // length>0 但 first/last 全零（NULL）：不一致 → FspMetadataException
                g.writeLong(700 + FlstBaseLayout.LEN, 5L);
                assertThrows(FspMetadataException.class, () -> FlstBase.readFrom(g, 700));
            }
        }
    }
}
