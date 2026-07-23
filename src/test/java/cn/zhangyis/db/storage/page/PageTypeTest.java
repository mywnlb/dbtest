package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** PageType code 钉死（落盘依赖）+ fromCode 往返 + 未知 code 拒绝。 */
class PageTypeTest {

    /**
     * 验证 {@code codesAreStable} 对应的物理页信封行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void codesAreStable() {
        assertEquals(0, PageType.ALLOCATED.code());
        assertEquals(1, PageType.FSP_HDR.code());
        assertEquals(2, PageType.IBUF_BITMAP.code());
        assertEquals(3, PageType.INODE.code());
        assertEquals(4, PageType.SDI.code());
        assertEquals(5, PageType.INDEX.code());
        assertEquals(6, PageType.UNDO.code());
        assertEquals(7, PageType.RSEG_HEADER.code());
        assertEquals(8, PageType.BLOB.code());
        assertEquals(9, PageType.UNDO_PAYLOAD.code());
        assertEquals(10, PageType.DDL_DESCRIPTOR.code());
        assertEquals(11, PageType.IBUF_HEADER.code());
        assertEquals(12, PageType.IBUF_INDEX.code());
    }

    /**
     * 验证 {@code fromCodeRoundTrips} 对应的物理页信封行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void fromCodeRoundTrips() {
        for (PageType t : PageType.values()) {
            assertEquals(t, PageType.fromCode(t.code()));
        }
    }

    /**
     * 验证 {@code fromCodeRejectsUnknown} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void fromCodeRejectsUnknown() {
        assertThrows(DatabaseValidationException.class, () -> PageType.fromCode(99));
    }
}
