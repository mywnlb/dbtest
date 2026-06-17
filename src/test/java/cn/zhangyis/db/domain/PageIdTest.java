package cn.zhangyis.db.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PageId 测试固定物理页定位公式：文件偏移只能由 pageNo * pageSize 推导。
 */
class PageIdTest {

    @Test
    void shouldCreatePageIdFromSpaceAndPageNo() {
        PageId pageId = PageId.of(SpaceId.of(7), PageNo.of(42));

        assertEquals(SpaceId.of(7), pageId.spaceId());
        assertEquals(PageNo.of(42), pageId.pageNo());
    }

    @Test
    void shouldCalculatePageOffset() {
        PageId pageId = PageId.of(SpaceId.of(1), PageNo.of(3));

        assertEquals(3L * 16 * 1024, pageId.offset(PageSize.ofBytes(16 * 1024)));
    }

    @Test
    void shouldRejectNullParts() {
        assertThrows(NullPointerException.class, () -> PageId.of(null, PageNo.of(1)));
        assertThrows(NullPointerException.class, () -> PageId.of(SpaceId.of(1), null));
    }
}
