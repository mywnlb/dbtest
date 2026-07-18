package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 字符集与排序规则 stable id 测试：持久标识不得依赖 enum ordinal，未知标识必须 fail-closed。 */
class CharsetCollationIdTest {

    /**
     * 验证 {@code charsetStableIdsAreUniqueAndReversible} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void charsetStableIdsAreUniqueAndReversible() {
        HashSet<Integer> ids = new HashSet<>();
        for (CharsetId charset : CharsetId.values()) {
            ids.add(charset.stableId());
            assertEquals(charset, CharsetId.fromStableId(charset.stableId()));
        }
        assertEquals(CharsetId.values().length, ids.size());
        assertThrows(DatabaseValidationException.class, () -> CharsetId.fromStableId(255));
    }

    /**
     * 验证 {@code collationStableIdsAreUniqueAndReversible} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void collationStableIdsAreUniqueAndReversible() {
        assertEquals(4, CollationId.UTF8_UNICODE_CI_V1.stableId());
        HashSet<Integer> ids = new HashSet<>();
        for (CollationId collation : CollationId.values()) {
            ids.add(collation.stableId());
            assertEquals(collation, CollationId.fromStableId(collation.stableId()));
        }
        assertEquals(CollationId.values().length, ids.size());
        assertThrows(DatabaseValidationException.class, () -> CollationId.fromStableId(255));
    }
}
