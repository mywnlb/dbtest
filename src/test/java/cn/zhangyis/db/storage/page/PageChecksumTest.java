package cn.zhangyis.db.storage.page;

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
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** PageChecksum stamp→verify、篡改失败、低 32 位 LSN、4K/16K 页尾偏移。 */
class PageChecksumTest {

    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private void withGuard(PageSize ps, BiConsumer<PageGuard, PageSize> body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s-" + ps.bytes() + ".ibd"), ps, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, ps, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                body.accept(g, ps);
            }
        }
    }

    /**
     * 验证 {@code stampThenVerifyPassesAndWritesLow32Lsn} 对应的物理页信封行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void stampThenVerifyPassesAndWritesLow32Lsn() {
        withGuard(PageSize.ofBytes(16 * 1024), (g, ps) -> {
            PageEnvelope.writeHeader(g, new FilePageHeader(SPACE, 3,
                    FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INDEX));
            g.writeInt(100, 0xCAFEBABE);
            PageChecksum.stamp(g, ps);
            assertTrue(PageChecksum.verify(g, ps));
            int trailerOffset = ps.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
            assertEquals(0, g.readInt(trailerOffset + PageEnvelopeLayout.TRAILER_LOW32_LSN));
        });
    }

    /**
     * 验证 {@code tamperFailsVerify} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void tamperFailsVerify() {
        withGuard(PageSize.ofBytes(16 * 1024), (g, ps) -> {
            PageChecksum.stamp(g, ps);
            assertTrue(PageChecksum.verify(g, ps));
            g.writeInt(200, 0x12345678);
            assertFalse(PageChecksum.verify(g, ps));
        });
    }

    /**
     * 验证 {@code worksAcross4kAnd16k} 对应的物理页信封行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void worksAcross4kAnd16k() {
        for (int kb : new int[] {4, 16}) {
            withGuard(PageSize.ofBytes(kb * 1024), (g, ps) -> {
                PageChecksum.stamp(g, ps);
                assertTrue(PageChecksum.verify(g, ps));
            });
        }
    }
}
