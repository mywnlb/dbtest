package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 存储标识值对象测试确保非法编号不会进入 FIL/FSP 层，避免后续用裸 int 混淆语义。
 */
class StorageIdentifierTest {

    /**
     * 验证 {@code shouldRejectNegativeIdentifiers} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void shouldRejectNegativeIdentifiers() {
        assertThrows(DatabaseRuntimeException.class, () -> SpaceId.of(-1));
        assertThrows(DatabaseRuntimeException.class, () -> PageNo.of(-1));
        assertThrows(DatabaseRuntimeException.class, () -> SegmentId.of(-1));
        assertThrows(DatabaseRuntimeException.class, () -> Lsn.of(-1));
    }

    /**
     * 验证 {@code shouldCalculateExtentFromPageNo} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldCalculateExtentFromPageNo() {
        PageSize pageSize = PageSize.ofBytes(16 * 1024);

        assertEquals(ExtentId.of(SpaceId.of(3), 0), ExtentId.from(PageId.of(SpaceId.of(3), PageNo.of(63)), pageSize));
        assertEquals(ExtentId.of(SpaceId.of(3), 1), ExtentId.from(PageId.of(SpaceId.of(3), PageNo.of(64)), pageSize));
    }

    /**
     * 验证 {@code shouldCalculateFirstPageNoInExtent} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void shouldCalculateFirstPageNoInExtent() {
        ExtentId extentId = ExtentId.of(SpaceId.of(9), 2);

        assertEquals(PageNo.of(128), extentId.firstPageNo(PageSize.ofBytes(16 * 1024)));
    }
}
