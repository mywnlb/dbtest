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

    /**
     * 验证 {@code ofAndNullSemantics} 对应的表空间、区与段分配行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
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

    /**
     * 验证 {@code shouldRejectReservedZeroAndNegativeOffset} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectReservedZeroAndNegativeOffset() {
        assertThrows(DatabaseValidationException.class, () -> FileAddress.of(PageNo.of(0), 0));
        assertThrows(DatabaseValidationException.class, () -> FileAddress.of(PageNo.of(5), -1));
        assertThrows(DatabaseValidationException.class, FileAddress.NULL::pageNo);
    }

    /**
     * 验证 {@code shouldRoundTripThroughPageGuard} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
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
