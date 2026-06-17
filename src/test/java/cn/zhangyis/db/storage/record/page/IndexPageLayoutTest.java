package cn.zhangyis.db.storage.record.page;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 页内几何常量钉死（§7）：信封边界、header 区、infimum/supremum、heap 起点、槽宽、标签长度。 */
class IndexPageLayoutTest {

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

    @Test
    void recordHeaderMirrorConstants() {
        // 必须与 record.format.RecordHeaderLayout（包私有）一致。
        assertEquals(8, IndexPageLayout.REC_HEADER_BYTES);
        assertEquals(4, IndexPageLayout.REC_NEXT_FIELD_OFFSET);
    }

    @Test
    void systemLabelsAreEightBytes() {
        assertEquals(8, IndexPageLayout.INFIMUM_LABEL.length);
        assertEquals(8, IndexPageLayout.SUPREMUM_LABEL.length);
    }
}
