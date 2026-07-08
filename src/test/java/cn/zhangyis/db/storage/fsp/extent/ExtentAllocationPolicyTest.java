package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.exception.NoFreeSpaceException;


import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * ExtentAllocationPolicy 值表与 NoFreeSpaceException 可恢复分类。
 */
class ExtentAllocationPolicyTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @Test
    void noDirectionOrNonLeafAcquiresOneExtentOnly() {
        ExtentAllocationPolicy p = new DefaultExtentAllocationPolicy();
        assertEquals(1, p.extentsToAcquire(request(SegmentPurpose.INDEX_LEAF,
                ExtentAllocationDirection.NO_DIRECTION, 10, 1)));
        assertEquals(1, p.extentsToAcquire(request(SegmentPurpose.INDEX_NON_LEAF,
                ExtentAllocationDirection.UP, 10, 1)));
        assertEquals(1, p.extentsToAcquire(request(SegmentPurpose.UNDO,
                ExtentAllocationDirection.DOWN, 10, 1)));
    }

    @Test
    void leafDirectionalGrowthClampsOwnedExtentCountToFour() {
        ExtentAllocationPolicy p = new DefaultExtentAllocationPolicy();
        assertEquals(1, p.extentsToAcquire(request(SegmentPurpose.INDEX_LEAF,
                ExtentAllocationDirection.UP, 0, 1)));
        assertEquals(1, p.extentsToAcquire(request(SegmentPurpose.INDEX_LEAF,
                ExtentAllocationDirection.UP, 1, 1)));
        assertEquals(2, p.extentsToAcquire(request(SegmentPurpose.INDEX_LEAF,
                ExtentAllocationDirection.UP, 2, 1)));
        assertEquals(3, p.extentsToAcquire(request(SegmentPurpose.INDEX_LEAF,
                ExtentAllocationDirection.DOWN, 3, 1)));
        assertEquals(4, p.extentsToAcquire(request(SegmentPurpose.INDEX_LEAF,
                ExtentAllocationDirection.DOWN, 10, 1)));
    }

    @Test
    void pagesNeededRaisesLowerBoundButStillCapsAtFour() {
        ExtentAllocationPolicy p = new DefaultExtentAllocationPolicy();
        assertEquals(2, p.extentsToAcquire(request(SegmentPurpose.INDEX_LEAF,
                ExtentAllocationDirection.UP, 1, PS.pagesPerExtent() + 1L)));
        assertEquals(4, p.extentsToAcquire(request(SegmentPurpose.INDEX_LEAF,
                ExtentAllocationDirection.UP, 10, PS.pagesPerExtent() * 10L)));
    }

    @Test
    void directionalRequestRequiresHintPage() {
        assertThrows(DatabaseValidationException.class, () -> new ExtentAllocationRequest(
                SegmentPurpose.INDEX_LEAF,
                Optional.empty(),
                ExtentAllocationDirection.UP,
                1L,
                1L,
                PS.pagesPerExtent(),
                PageNo.of(1024),
                1.0d,
                PS));
    }

    @Test
    void noFreeSpaceIsRecoverable() {
        assertInstanceOf(DatabaseRuntimeException.class, new NoFreeSpaceException("x"));
    }

    private static ExtentAllocationRequest request(SegmentPurpose purpose, ExtentAllocationDirection direction,
                                                   long ownedExtentCount, long pagesNeeded) {
        return new ExtentAllocationRequest(
                purpose,
                Optional.of(PageNo.of(128)),
                direction,
                pagesNeeded,
                ownedExtentCount,
                ownedExtentCount * PS.pagesPerExtent(),
                PageNo.of(1024),
                1.0d,
                PS);
    }
}
