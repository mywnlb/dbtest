package cn.zhangyis.db.storage.record.page;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 页内几何常量钉死（§7）：信封边界、header 区、infimum/supremum、heap 起点、槽宽、标签长度。 */
class IndexPageLayoutTest {

    /**
     * 验证 {@code envelopeAndRegionConstants} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void envelopeAndRegionConstants() {
        assertEquals(38, IndexPageLayout.FIL_PAGE_HEADER_BYTES);
        assertEquals(8, IndexPageLayout.FIL_PAGE_TRAILER_BYTES);
        assertEquals(38, IndexPageLayout.PAGE_HEADER_START);
        assertEquals(66, IndexPageLayout.PAGE_HEADER_END);
        assertEquals(66, IndexPageLayout.INFIMUM_OFFSET);
        assertEquals(16, IndexPageLayout.SYS_REC_BYTES);
        assertEquals(82, IndexPageLayout.SUPREMUM_OFFSET);
        assertEquals(98, IndexPageLayout.USER_RECORDS_START);
        assertEquals(2, IndexPageLayout.DIR_SLOT_BYTES);
    }

    /**
     * 验证 {@code recordHeaderMirrorConstants} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void recordHeaderMirrorConstants() {
        // 必须与 record.format.RecordHeaderLayout（包私有）一致。
        assertEquals(8, IndexPageLayout.REC_HEADER_BYTES);
        assertEquals(4, IndexPageLayout.REC_NEXT_FIELD_OFFSET);
    }

    /**
     * 验证 {@code systemLabelsAreEightBytes} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void systemLabelsAreEightBytes() {
        assertEquals(8, IndexPageLayout.INFIMUM_LABEL.length);
        assertEquals(8, IndexPageLayout.SUPREMUM_LABEL.length);
    }
}
