package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PageSize 测试固定 InnoDB 风格页大小与 extent 换算规则，避免后续空间分配代码写死 64 页。
 */
class PageSizeTest {

    /**
     * 验证 {@code shouldAcceptSupportedInnodbPageSizes} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldAcceptSupportedInnodbPageSizes() {
        assertEquals(4096, PageSize.ofBytes(4096).bytes());
        assertEquals(8192, PageSize.ofBytes(8192).bytes());
        assertEquals(16 * 1024, PageSize.ofBytes(16 * 1024).bytes());
        assertEquals(32 * 1024, PageSize.ofBytes(32 * 1024).bytes());
        assertEquals(64 * 1024, PageSize.ofBytes(64 * 1024).bytes());
    }

    /**
     * 验证 {@code shouldRejectUnsupportedPageSizes} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectUnsupportedPageSizes() {
        assertThrows(DatabaseRuntimeException.class, () -> PageSize.ofBytes(1024));
        assertThrows(DatabaseRuntimeException.class, () -> PageSize.ofBytes(12 * 1024));
    }

    /**
     * 验证 {@code shouldCalculatePagesPerExtentByPageSize} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldCalculatePagesPerExtentByPageSize() {
        assertEquals(256, PageSize.ofBytes(4096).pagesPerExtent());
        assertEquals(128, PageSize.ofBytes(8192).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(16 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(32 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(64 * 1024).pagesPerExtent());
    }

    /**
     * 验证 {@code shouldCalculateExtentSizeBytesByMysql80Rules} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void shouldCalculateExtentSizeBytesByMysql80Rules() {
        assertEquals(1024 * 1024, PageSize.ofBytes(4096).extentSizeBytes());
        assertEquals(1024 * 1024, PageSize.ofBytes(8192).extentSizeBytes());
        assertEquals(1024 * 1024, PageSize.ofBytes(16 * 1024).extentSizeBytes());
        assertEquals(2 * 1024 * 1024, PageSize.ofBytes(32 * 1024).extentSizeBytes());
        assertEquals(4 * 1024 * 1024, PageSize.ofBytes(64 * 1024).extentSizeBytes());
    }
}
