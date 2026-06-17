package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** RecordRef 取值与构造校验。 */
class RecordRefTest {

    private static final PageId PAGE = PageId.of(SpaceId.of(1), PageNo.of(3));

    @Test
    void holdsValues() {
        RecordRef ref = new RecordRef(PAGE, 5, 98, 1L, 7L);
        assertEquals(PAGE, ref.pageId());
        assertEquals(5, ref.heapNo());
        assertEquals(98, ref.pageOffset());
        assertEquals(1L, ref.schemaVersion());
        assertEquals(7L, ref.indexId());
    }

    @Test
    void rejectsInvalid() {
        assertThrows(DatabaseValidationException.class, () -> new RecordRef(null, 0, 0, 0, 0));
        assertThrows(DatabaseValidationException.class, () -> new RecordRef(PAGE, -1, 0, 0, 0));
        assertThrows(DatabaseValidationException.class, () -> new RecordRef(PAGE, 0, -1, 0, 0));
        assertThrows(DatabaseValidationException.class, () -> new RecordRef(PAGE, 0, 0, -1, 0));
        assertThrows(DatabaseValidationException.class, () -> new RecordRef(PAGE, 0, 0, 0, -1));
    }
}
