package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * T1.3b undo segment 定位值对象：验证页链端点、inode 槽与 segment id 的形状约束。
 */
class UndoSegmentHandleTest {

    private static final SpaceId SPACE = SpaceId.of(77);

    private static PageId page(long no) {
        return PageId.of(SPACE, PageNo.of(no));
    }

    /**
     * 验证 {@code buildsAndExposesFields} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void buildsAndExposesFields() {
        UndoSegmentHandle h = new UndoSegmentHandle(SPACE, 3, SegmentId.of(9), page(10), page(10));
        assertEquals(SPACE, h.spaceId());
        assertEquals(3, h.inodeSlot());
        assertEquals(9L, h.segmentId().value());
        assertEquals(page(10), h.firstPageId());
        assertEquals(page(10), h.lastPageId());
    }

    /**
     * 验证 {@code withLastPageReturnsNewInstanceOnlyChangingLast} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void withLastPageReturnsNewInstanceOnlyChangingLast() {
        UndoSegmentHandle h = new UndoSegmentHandle(SPACE, 3, SegmentId.of(9), page(10), page(10));
        UndoSegmentHandle h2 = h.withLastPage(page(20));
        assertEquals(page(20), h2.lastPageId());
        assertEquals(page(10), h2.firstPageId());
        assertEquals(h.spaceId(), h2.spaceId());
        assertEquals(h.inodeSlot(), h2.inodeSlot());
        assertEquals(h.segmentId(), h2.segmentId());
        assertEquals(page(10), h.lastPageId());
    }

    /**
     * 验证 {@code rejectsNegativeSlot} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsNegativeSlot() {
        assertThrows(DatabaseValidationException.class,
                () -> new UndoSegmentHandle(SPACE, -1, SegmentId.of(9), page(10), page(10)));
    }

    /**
     * 验证 {@code rejectsNonPositiveSegmentId} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsNonPositiveSegmentId() {
        assertThrows(DatabaseValidationException.class,
                () -> new UndoSegmentHandle(SPACE, 0, SegmentId.of(0), page(10), page(10)));
    }

    /**
     * 验证 {@code rejectsPageSpaceMismatch} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsPageSpaceMismatch() {
        PageId other = PageId.of(SpaceId.of(88), PageNo.of(10));
        assertThrows(DatabaseValidationException.class,
                () -> new UndoSegmentHandle(SPACE, 0, SegmentId.of(9), other, page(10)));
    }
}
