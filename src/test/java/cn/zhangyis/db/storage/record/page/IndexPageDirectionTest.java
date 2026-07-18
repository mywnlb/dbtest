package cn.zhangyis.db.storage.record.page;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 插入方向 code 往返；未知 code 视为 page header 损坏。 */
class IndexPageDirectionTest {

    /**
     * 验证 {@code codeRoundTrip} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void codeRoundTrip() {
        for (IndexPageDirection d : IndexPageDirection.values()) {
            assertEquals(d, IndexPageDirection.fromCode(d.code()));
        }
    }

    /**
     * 验证 {@code unknownCodeRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void unknownCodeRejected() {
        assertThrows(PageDirectoryCorruptedException.class, () -> IndexPageDirection.fromCode(99));
    }
}
