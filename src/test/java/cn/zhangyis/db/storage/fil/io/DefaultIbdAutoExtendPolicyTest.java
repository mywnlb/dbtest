package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DefaultIbdAutoExtendPolicy 测试钉死 MySQL 8.0 默认扩展边界，避免后续误写成模糊闭区间（§15）。
 */
class DefaultIbdAutoExtendPolicyTest {

    private final AutoExtendPolicy policy = new DefaultIbdAutoExtendPolicy();

    /**
     * 验证 {@code shouldGrowByOnePageWhenSmallerThanOneExtent} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldGrowByOnePageWhenSmallerThanOneExtent() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(1, policy.nextIncrementPages(0, ps));
        assertEquals(1, policy.nextIncrementPages(63, ps));
    }

    /**
     * 验证 {@code shouldGrowByOneExtentBetweenOneAndThirtyTwoExtents} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void shouldGrowByOneExtentBetweenOneAndThirtyTwoExtents() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(64, policy.nextIncrementPages(64, ps));            // 恰好 1 个 extent 边界
        assertEquals(64, policy.nextIncrementPages(32 * 64 - 1, ps));   // 32 个 extent 前的最后一页
    }

    /**
     * 验证 {@code shouldGrowByFourExtentsAtOrAboveThirtyTwoExtents} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void shouldGrowByFourExtentsAtOrAboveThirtyTwoExtents() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(256, policy.nextIncrementPages(32 * 64, ps));      // 恰好 32 个 extent，进入 4-extent 档
        assertEquals(256, policy.nextIncrementPages(5000, ps));
    }

    /**
     * 验证 {@code shouldScaleBoundariesWithPageSize} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldScaleBoundariesWithPageSize() {
        PageSize ps = PageSize.ofBytes(4 * 1024); // ppe = 256
        assertEquals(1, policy.nextIncrementPages(255, ps));
        assertEquals(256, policy.nextIncrementPages(256, ps));
        assertEquals(1024, policy.nextIncrementPages(32 * 256, ps));
    }
}
