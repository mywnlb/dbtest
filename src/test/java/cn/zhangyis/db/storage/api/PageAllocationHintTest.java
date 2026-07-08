package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.15 DiskSpaceManager 页分配 hint 的公开 API 测试。API 层只表达方向和邻近页，不暴露 FSP 内部 request。
 */
class PageAllocationHintTest {

    @Test
    void noneKeepsLegacyNoDirectionDefaults() {
        PageAllocationHint hint = PageAllocationHint.none();
        assertEquals(PageAllocationHint.Direction.NO_DIRECTION, hint.direction());
        assertTrue(hint.hintPageNo().isEmpty());
        assertEquals(1L, hint.pagesNeeded());
    }

    @Test
    void directionalFactoriesRequireHintAndPositivePagesNeeded() {
        PageAllocationHint up = PageAllocationHint.up(PageNo.of(128), 2L);
        assertEquals(PageAllocationHint.Direction.UP, up.direction());
        assertEquals(PageNo.of(128), up.hintPageNo().orElseThrow());
        assertEquals(2L, up.pagesNeeded());

        PageAllocationHint down = PageAllocationHint.down(PageNo.of(64), 1L);
        assertEquals(PageAllocationHint.Direction.DOWN, down.direction());

        assertThrows(DatabaseValidationException.class, () -> PageAllocationHint.up(null, 1L));
        assertThrows(DatabaseValidationException.class, () -> PageAllocationHint.down(PageNo.of(64), 0L));
    }
}
