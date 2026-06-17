package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PageSize 测试固定 InnoDB 风格页大小与 extent 换算规则，避免后续空间分配代码写死 64 页。
 */
class PageSizeTest {

    @Test
    void shouldAcceptSupportedInnodbPageSizes() {
        assertEquals(4096, PageSize.ofBytes(4096).bytes());
        assertEquals(8192, PageSize.ofBytes(8192).bytes());
        assertEquals(16 * 1024, PageSize.ofBytes(16 * 1024).bytes());
        assertEquals(32 * 1024, PageSize.ofBytes(32 * 1024).bytes());
        assertEquals(64 * 1024, PageSize.ofBytes(64 * 1024).bytes());
    }

    @Test
    void shouldRejectUnsupportedPageSizes() {
        assertThrows(DatabaseRuntimeException.class, () -> PageSize.ofBytes(1024));
        assertThrows(DatabaseRuntimeException.class, () -> PageSize.ofBytes(12 * 1024));
    }

    @Test
    void shouldCalculatePagesPerExtentByPageSize() {
        assertEquals(256, PageSize.ofBytes(4096).pagesPerExtent());
        assertEquals(128, PageSize.ofBytes(8192).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(16 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(32 * 1024).pagesPerExtent());
        assertEquals(64, PageSize.ofBytes(64 * 1024).pagesPerExtent());
    }

    @Test
    void shouldCalculateExtentSizeBytesByMysql80Rules() {
        assertEquals(1024 * 1024, PageSize.ofBytes(4096).extentSizeBytes());
        assertEquals(1024 * 1024, PageSize.ofBytes(8192).extentSizeBytes());
        assertEquals(1024 * 1024, PageSize.ofBytes(16 * 1024).extentSizeBytes());
        assertEquals(2 * 1024 * 1024, PageSize.ofBytes(32 * 1024).extentSizeBytes());
        assertEquals(4 * 1024 * 1024, PageSize.ofBytes(64 * 1024).extentSizeBytes());
    }
}
