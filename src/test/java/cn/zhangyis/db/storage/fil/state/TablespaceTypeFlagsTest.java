package cn.zhangyis.db.storage.fil.state;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * type 与 page-0 spaceFlags 的编解码测试：低 3 位保存 type code，高位保留给后续压缩/加密等 flags。
 */
class TablespaceTypeFlagsTest {

    @Test
    void roundTripsAllTypes() {
        for (TablespaceType type : TablespaceType.values()) {
            assertEquals(type, TablespaceTypeFlags.decode(TablespaceTypeFlags.encode(type)));
        }
    }

    @Test
    void highReservedBitsDoNotAffectType() {
        int withHighBits = TablespaceTypeFlags.encode(TablespaceType.UNDO) | 0x100;
        assertEquals(TablespaceType.UNDO, TablespaceTypeFlags.decode(withHighBits));
    }

    @Test
    void decodeRejectsUnknownTypeCode() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceTypeFlags.decode(5));
    }

    @Test
    void encodeRejectsNull() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceTypeFlags.encode(null));
    }
}
