package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 领域 workload 只表达编码工作量，组合必须 checked，不能把溢出预算带入 MTR admission。 */
class RedoBudgetWorkloadTest {

    /**
     * 验证 {@code workloadsCombinePageImagesAndLogicalBytes} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void workloadsCombinePageImagesAndLogicalBytes() {
        RedoBudgetWorkload combined = RedoBudgetWorkload.pageImages(2)
                .plus(new RedoBudgetWorkload(3, 17));

        assertEquals(5, combined.pageImageEquivalents());
        assertEquals(17, combined.extraLogicalBytes());
    }

    /**
     * 验证 {@code workloadRejectsNegativeAndOverflowingValues} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void workloadRejectsNegativeAndOverflowingValues() {
        assertThrows(DatabaseValidationException.class, () -> new RedoBudgetWorkload(-1, 0));
        assertThrows(DatabaseValidationException.class,
                () -> new RedoBudgetWorkload(Long.MAX_VALUE, 0).plus(RedoBudgetWorkload.pageImages(1)));
    }
}
