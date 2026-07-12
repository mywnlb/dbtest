package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** B+Tree workload 只依赖 begin 前已有 rootLevel，深树最坏 split/merge 预算必须单调增长。 */
class BTreeRedoBudgetEstimatorTest {

    @Test
    void insertAndStructuralDeleteScaleWithTreeHeight() {
        RedoBudgetWorkload shallowInsert = BTreeRedoBudgetEstimator.insert(0);
        RedoBudgetWorkload deepInsert = BTreeRedoBudgetEstimator.insert(3);
        RedoBudgetWorkload shallowDelete = BTreeRedoBudgetEstimator.structuralDelete(0);
        RedoBudgetWorkload deepDelete = BTreeRedoBudgetEstimator.structuralDelete(3);

        assertEquals(10, shallowInsert.pageImageEquivalents());
        assertEquals(28, deepInsert.pageImageEquivalents());
        assertEquals(12, shallowDelete.pageImageEquivalents());
        assertEquals(30, deepDelete.pageImageEquivalents());
        assertTrue(deepInsert.pageImageEquivalents() > shallowInsert.pageImageEquivalents());
        assertTrue(deepDelete.pageImageEquivalents() > shallowDelete.pageImageEquivalents());
        assertEquals(4, BTreeRedoBudgetEstimator.pointRewrite().pageImageEquivalents());
    }

    @Test
    void negativeRootLevelIsRejectedBeforeMtrBegin() {
        assertThrows(DatabaseValidationException.class, () -> BTreeRedoBudgetEstimator.insert(-1));
    }
}
