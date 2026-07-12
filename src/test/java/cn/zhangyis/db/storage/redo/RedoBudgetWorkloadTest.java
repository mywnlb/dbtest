package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 领域 workload 只表达编码工作量，组合必须 checked，不能把溢出预算带入 MTR admission。 */
class RedoBudgetWorkloadTest {

    @Test
    void workloadsCombinePageImagesAndLogicalBytes() {
        RedoBudgetWorkload combined = RedoBudgetWorkload.pageImages(2)
                .plus(new RedoBudgetWorkload(3, 17));

        assertEquals(5, combined.pageImageEquivalents());
        assertEquals(17, combined.extraLogicalBytes());
    }

    @Test
    void workloadRejectsNegativeAndOverflowingValues() {
        assertThrows(DatabaseValidationException.class, () -> new RedoBudgetWorkload(-1, 0));
        assertThrows(DatabaseValidationException.class,
                () -> new RedoBudgetWorkload(Long.MAX_VALUE, 0).plus(RedoBudgetWorkload.pageImages(1)));
    }
}
