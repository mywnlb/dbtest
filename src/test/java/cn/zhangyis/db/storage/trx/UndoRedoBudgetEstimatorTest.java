package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Undo workload 根据首写建段与真实 drop plan 缩放，不再为所有终结固定预留 32 个页 image。 */
class UndoRedoBudgetEstimatorTest {

    /**
     * 验证 {@code firstAppendCostsMoreThanExistingSegmentAppend} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void firstAppendCostsMoreThanExistingSegmentAppend() {
        assertTrue(UndoRedoBudgetEstimator.append(true).pageImageEquivalents()
                > UndoRedoBudgetEstimator.append(false).pageImageEquivalents());
        assertEquals(10L, UndoRedoBudgetEstimator.append(UndoSegmentAcquisition.REUSE_FREE)
                .pageImageEquivalents());
    }

    /**
     * 验证 {@code finalizationScalesWithFragmentsAndExtents} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void finalizationScalesWithFragmentsAndExtents() {
        RedoBudgetWorkload empty = UndoRedoBudgetEstimator.finalization(new UndoSegmentDropPlan(0, 0, 0), true);
        RedoBudgetWorkload fragments = UndoRedoBudgetEstimator.finalization(
                new UndoSegmentDropPlan(3, 0, 3), true);
        RedoBudgetWorkload extents = UndoRedoBudgetEstimator.finalization(
                new UndoSegmentDropPlan(3, 2, 20), true);
        RedoBudgetWorkload purge = UndoRedoBudgetEstimator.finalization(
                new UndoSegmentDropPlan(3, 2, 20), false);

        assertEquals(8, empty.pageImageEquivalents());
        assertEquals(14, fragments.pageImageEquivalents());
        assertEquals(22, extents.pageImageEquivalents());
        assertEquals(21, purge.pageImageEquivalents());
        assertTrue(fragments.pageImageEquivalents() > empty.pageImageEquivalents());
        assertTrue(extents.pageImageEquivalents() > fragments.pageImageEquivalents());
    }

    /**
     * 验证 {@code reusableFinalizationBudgetsCacheAndFreeSeparately} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void reusableFinalizationBudgetsCacheAndFreeSeparately() {
        assertEquals(11L, UndoRedoBudgetEstimator.finalization(java.util.List.of(), 1, 0, false)
                .pageImageEquivalents());
        assertEquals(13L, UndoRedoBudgetEstimator.finalization(java.util.List.of(), 0, 1, false)
                .pageImageEquivalents());
    }
}
