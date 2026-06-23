package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * TablespaceType 稳定落盘 code 测试：page-0 spaceFlags 依赖这些 code，不能随 enum 顺序漂移。
 */
class TablespaceTypeTest {

    @Test
    void codesAreStable() {
        assertEquals(0, TablespaceType.SYSTEM.code());
        assertEquals(1, TablespaceType.FILE_PER_TABLE.code());
        assertEquals(2, TablespaceType.GENERAL.code());
        assertEquals(3, TablespaceType.UNDO.code());
        assertEquals(4, TablespaceType.TEMPORARY.code());
    }

    @Test
    void fromCodeRoundTrips() {
        for (TablespaceType type : TablespaceType.values()) {
            assertEquals(type, TablespaceType.fromCode(type.code()));
        }
    }

    @Test
    void fromCodeRejectsUnknown() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceType.fromCode(7));
    }
}
