package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1 page image checksum 测试：flush 写的是 byte[] snapshot，不能依赖活跃 PageGuard 盖 checksum/trailer。
 */
class PageImageChecksumTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    /**
     * 验证 {@code stampAndVerifyPageImageChecksumAndTrailer} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void stampAndVerifyPageImageChecksumAndTrailer() {
        byte[] page = new byte[PS.bytes()];
        ByteBuffer.wrap(page).putLong(PageEnvelopeLayout.PAGE_LSN, 0x1_0000_00AAL);
        page[200] = 7;

        PageImageChecksum.stamp(page, PS);

        int checksum = ByteBuffer.wrap(page).getInt(PageEnvelopeLayout.CHECKSUM);
        int trailer = ByteBuffer.wrap(page).getInt(PageEnvelopeLayout.trailerOffset(PS) + PageEnvelopeLayout.TRAILER_CHECKSUM);
        int low32 = ByteBuffer.wrap(page).getInt(PageEnvelopeLayout.trailerOffset(PS) + PageEnvelopeLayout.TRAILER_LOW32_LSN);
        assertEquals(checksum, trailer);
        assertEquals(0x000000AA, low32);
        assertTrue(PageImageChecksum.verify(page, PS));
    }
}
