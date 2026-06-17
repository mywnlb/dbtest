package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** PageType code 钉死（落盘依赖）+ fromCode 往返 + 未知 code 拒绝。 */
class PageTypeTest {

    @Test
    void codesAreStable() {
        assertEquals(0, PageType.ALLOCATED.code());
        assertEquals(1, PageType.FSP_HDR.code());
        assertEquals(2, PageType.IBUF_BITMAP.code());
        assertEquals(3, PageType.INODE.code());
        assertEquals(4, PageType.SDI.code());
        assertEquals(5, PageType.INDEX.code());
    }

    @Test
    void fromCodeRoundTrips() {
        for (PageType t : PageType.values()) {
            assertEquals(t, PageType.fromCode(t.code()));
        }
    }

    @Test
    void fromCodeRejectsUnknown() {
        assertThrows(DatabaseValidationException.class, () -> PageType.fromCode(99));
    }
}
