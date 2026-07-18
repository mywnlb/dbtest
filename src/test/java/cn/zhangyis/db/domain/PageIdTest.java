package cn.zhangyis.db.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PageId 测试固定物理页定位公式：文件偏移只能由 pageNo * pageSize 推导。
 */
class PageIdTest {

    /**
     * 验证 {@code shouldCreatePageIdFromSpaceAndPageNo} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldCreatePageIdFromSpaceAndPageNo() {
        PageId pageId = PageId.of(SpaceId.of(7), PageNo.of(42));

        assertEquals(SpaceId.of(7), pageId.spaceId());
        assertEquals(PageNo.of(42), pageId.pageNo());
    }

    /**
     * 验证 {@code shouldCalculatePageOffset} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldCalculatePageOffset() {
        PageId pageId = PageId.of(SpaceId.of(1), PageNo.of(3));

        assertEquals(3L * 16 * 1024, pageId.offset(PageSize.ofBytes(16 * 1024)));
    }

    /**
     * 验证 {@code shouldRejectNullParts} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectNullParts() {
        assertThrows(NullPointerException.class, () -> PageId.of(null, PageNo.of(1)));
        assertThrows(NullPointerException.class, () -> PageId.of(SpaceId.of(1), null));
    }
}
