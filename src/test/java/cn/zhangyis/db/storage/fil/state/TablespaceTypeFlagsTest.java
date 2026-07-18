package cn.zhangyis.db.storage.fil.state;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * type 与 page-0 spaceFlags 的编解码测试：低 3 位保存 type code，高位保留给后续压缩/加密等 flags。
 */
class TablespaceTypeFlagsTest {

    /**
     * 验证 {@code roundTripsAllTypes} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void roundTripsAllTypes() {
        for (TablespaceType type : TablespaceType.values()) {
            assertEquals(type, TablespaceTypeFlags.decode(TablespaceTypeFlags.encode(type)));
        }
    }

    /**
     * 验证 {@code highReservedBitsDoNotAffectType} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void highReservedBitsDoNotAffectType() {
        int withHighBits = TablespaceTypeFlags.encode(TablespaceType.UNDO) | 0x100;
        assertEquals(TablespaceType.UNDO, TablespaceTypeFlags.decode(withHighBits));
    }

    /**
     * 验证 {@code decodeRejectsUnknownTypeCode} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void decodeRejectsUnknownTypeCode() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceTypeFlags.decode(5));
    }

    /**
     * 验证 {@code encodeRejectsNull} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void encodeRejectsNull() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceTypeFlags.encode(null));
    }
}
