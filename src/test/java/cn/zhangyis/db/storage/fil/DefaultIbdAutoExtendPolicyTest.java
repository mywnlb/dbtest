package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.domain.PageSize;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DefaultIbdAutoExtendPolicy 测试钉死 MySQL 8.0 默认扩展边界，避免后续误写成模糊闭区间（§15）。
 */
class DefaultIbdAutoExtendPolicyTest {

    private final AutoExtendPolicy policy = new DefaultIbdAutoExtendPolicy();

    @Test
    void shouldGrowByOnePageWhenSmallerThanOneExtent() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(1, policy.nextIncrementPages(0, ps));
        assertEquals(1, policy.nextIncrementPages(63, ps));
    }

    @Test
    void shouldGrowByOneExtentBetweenOneAndThirtyTwoExtents() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(64, policy.nextIncrementPages(64, ps));            // 恰好 1 个 extent 边界
        assertEquals(64, policy.nextIncrementPages(32 * 64 - 1, ps));   // 32 个 extent 前的最后一页
    }

    @Test
    void shouldGrowByFourExtentsAtOrAboveThirtyTwoExtents() {
        PageSize ps = PageSize.ofBytes(16 * 1024); // ppe = 64
        assertEquals(256, policy.nextIncrementPages(32 * 64, ps));      // 恰好 32 个 extent，进入 4-extent 档
        assertEquals(256, policy.nextIncrementPages(5000, ps));
    }

    @Test
    void shouldScaleBoundariesWithPageSize() {
        PageSize ps = PageSize.ofBytes(4 * 1024); // ppe = 256
        assertEquals(1, policy.nextIncrementPages(255, ps));
        assertEquals(256, policy.nextIncrementPages(256, ps));
        assertEquals(1024, policy.nextIncrementPages(32 * 256, ps));
    }
}
