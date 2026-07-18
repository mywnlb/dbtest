package cn.zhangyis.db.storage.fil.state;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * TablespaceType 稳定落盘 code 测试：page-0 spaceFlags 依赖这些 code，不能随 enum 顺序漂移。
 */
class TablespaceTypeTest {

    /**
     * 验证 {@code codesAreStable} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void codesAreStable() {
        assertEquals(0, TablespaceType.SYSTEM.code());
        assertEquals(1, TablespaceType.FILE_PER_TABLE.code());
        assertEquals(2, TablespaceType.GENERAL.code());
        assertEquals(3, TablespaceType.UNDO.code());
        assertEquals(4, TablespaceType.TEMPORARY.code());
    }

    /**
     * 验证 {@code fromCodeRoundTrips} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void fromCodeRoundTrips() {
        for (TablespaceType type : TablespaceType.values()) {
            assertEquals(type, TablespaceType.fromCode(type.code()));
        }
    }

    /**
     * 验证 {@code fromCodeRejectsUnknown} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void fromCodeRejectsUnknown() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceType.fromCode(7));
    }
}
