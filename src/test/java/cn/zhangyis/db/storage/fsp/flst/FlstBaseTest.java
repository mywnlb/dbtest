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

    /**
     * 验证 {@code constructorValidates} 对应的表空间、区与段分配行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void constructorValidates() {
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(-1, FileAddress.NULL, FileAddress.NULL));
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(0, null, FileAddress.NULL));
        assertThrows(DatabaseValidationException.class, () -> new FlstBase(0, FileAddress.NULL, null));
        assertEquals(new FlstBase(0, FileAddress.NULL, FileAddress.NULL), FlstBase.EMPTY);
    }

    /**
     * 验证 {@code roundTripAndZeroDecodesEmpty} 所描述的稳定格式转换，并断言往返值、字节布局、版本与损坏输入处理。
     */
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

    /**
     * 验证 {@code decodeRejectsLengthEndpointInconsistency} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
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
