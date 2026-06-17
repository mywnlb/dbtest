package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 钉死 XDES/INODE 落盘依赖的枚举 ordinal（改序会破坏已写盘数据），并固定 FspMetadataException 可恢复分类。
 */
class FspEnumTest {

    @Test
    void extentStateOrdinalsAreStable() {
        assertEquals(0, ExtentState.FREE.ordinal());
        assertEquals(1, ExtentState.FREE_FRAG.ordinal());
        assertEquals(2, ExtentState.FULL_FRAG.ordinal());
        assertEquals(3, ExtentState.FSEG.ordinal());
        assertEquals(4, ExtentState.FSEG_FRAG.ordinal());
    }

    @Test
    void segmentPurposeOrdinalsAreStable() {
        assertEquals(0, SegmentPurpose.INDEX_LEAF.ordinal());
        assertEquals(1, SegmentPurpose.INDEX_NON_LEAF.ordinal());
        assertEquals(2, SegmentPurpose.LOB.ordinal());
        assertEquals(3, SegmentPurpose.UNDO.ordinal());
        assertEquals(4, SegmentPurpose.SYSTEM.ordinal());
    }

    @Test
    void metadataExceptionIsRecoverable() {
        assertInstanceOf(DatabaseRuntimeException.class, new FspMetadataException("x"));
    }
}
