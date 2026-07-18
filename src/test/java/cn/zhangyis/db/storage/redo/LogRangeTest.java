package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** LogRange：end>=start，endLsn 取 end。 */
class LogRangeTest {

    /**
     * 验证 {@code holdsRangeAndRejectsInverted} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void holdsRangeAndRejectsInverted() {
        LogRange r = new LogRange(Lsn.of(10), Lsn.of(27));
        assertEquals(Lsn.of(10), r.start());
        assertEquals(Lsn.of(27), r.end());
        assertThrows(DatabaseValidationException.class, () -> new LogRange(Lsn.of(27), Lsn.of(10)));
    }
}
