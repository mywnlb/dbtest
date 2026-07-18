package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** IndexKeyDef 复合 key part、ASC/DESC、prefix。 */
class IndexKeyDefTest {

    /**
     * 验证 {@code compositeKey} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void compositeKey() {
        IndexKeyDef k = new IndexKeyDef(7L, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(1), KeyOrder.DESC, 4)));
        assertEquals(2, k.parts().size());
        assertEquals(KeyOrder.DESC, k.parts().get(1).order());
        assertEquals(4, k.parts().get(1).prefixBytes());
    }

    /**
     * 验证 {@code rejectsEmptyParts} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsEmptyParts() {
        assertThrows(DatabaseValidationException.class, () -> new IndexKeyDef(1L, List.of()));
    }

    /**
     * 验证 {@code rejectsNegativePrefix} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void rejectsNegativePrefix() {
        assertThrows(DatabaseValidationException.class,
                () -> new KeyPartDef(new ColumnId(0), KeyOrder.ASC, -1));
    }
}
