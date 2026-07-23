package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 固定 4-bit/page bitmap 的寻址、位语义和保守空闲空间等级。 */
class ChangeBufferBitmapLayoutTest {

    private static final PageSize PAGE_SIZE = PageSize.ofBytes(16 * 1024);

    /** bitmap 页按 MySQL 物理页大小周期重复，边界页必须落入下一张 bitmap。 */
    @Test
    void mapsTargetPagesToRepeatedBitmapPages() {
        assertEquals(PageNo.of(1), ChangeBufferBitmapLayout.bitmapPageNo(PAGE_SIZE, PageNo.of(0)));
        assertEquals(PageNo.of(1), ChangeBufferBitmapLayout.bitmapPageNo(PAGE_SIZE, PageNo.of(16_383)));
        assertEquals(PageNo.of(16_385), ChangeBufferBitmapLayout.bitmapPageNo(PAGE_SIZE, PageNo.of(16_384)));
        assertEquals(0, ChangeBufferBitmapLayout.byteOffsetInBody(PAGE_SIZE, PageNo.of(0)));
        assertEquals(0, ChangeBufferBitmapLayout.nibbleShift(PAGE_SIZE, PageNo.of(0)));
        assertEquals(4, ChangeBufferBitmapLayout.nibbleShift(PAGE_SIZE, PageNo.of(1)));
    }

    /** v1 只把首张公式 bitmap 纳入普通 DML；跨第二区间必须回退直写，不能读取尚未预留的固定页。 */
    @Test
    void distinguishesBootstrapCoverageFromFutureRepeatedBitmapRanges() {
        assertTrue(ChangeBufferBitmapLayout.isBootstrapBitmapCovered(PAGE_SIZE, PageNo.of(16_383)));
        assertFalse(ChangeBufferBitmapLayout.isBootstrapBitmapCovered(PAGE_SIZE, PageNo.of(16_384)));
    }

    /** 两个目标页共享一个 byte，但 pending/internal 位和 free class 不得互相污染。 */
    @Test
    void packsAndUpdatesOneNibbleWithoutChangingItsNeighbour() {
        int left = ChangeBufferBitmapLayout.encodeNibble(2, true, false);
        int right = ChangeBufferBitmapLayout.encodeNibble(1, false, true);
        byte packed = (byte) (left | (right << 4));

        ChangeBufferBitmapState leftState = ChangeBufferBitmapLayout.decodeNibble(packed, 0);
        ChangeBufferBitmapState rightState = ChangeBufferBitmapLayout.decodeNibble(packed, 4);
        assertEquals(2, leftState.freeSpaceClass());
        assertTrue(leftState.buffered());
        assertFalse(leftState.changeBufferInternal());
        assertEquals(1, rightState.freeSpaceClass());
        assertFalse(rightState.buffered());
        assertTrue(rightState.changeBufferInternal());
    }

    /** 空闲空间按 pageSize/32 单位保守分级，精确三个单位降为 2，更多空间才发布等级 3。 */
    @Test
    void classifiesFreeSpaceConservatively() {
        int unit = PAGE_SIZE.bytes() / 32;
        assertEquals(0, ChangeBufferBitmapLayout.freeSpaceClass(PAGE_SIZE, unit - 1));
        assertEquals(1, ChangeBufferBitmapLayout.freeSpaceClass(PAGE_SIZE, unit));
        assertEquals(2, ChangeBufferBitmapLayout.freeSpaceClass(PAGE_SIZE, unit * 2));
        assertEquals(2, ChangeBufferBitmapLayout.freeSpaceClass(PAGE_SIZE, unit * 3));
        assertEquals(3, ChangeBufferBitmapLayout.freeSpaceClass(PAGE_SIZE, unit * 4));
        assertEquals(unit * 2, ChangeBufferBitmapLayout.freeSpaceLowerBound(PAGE_SIZE, 2));
    }
}
