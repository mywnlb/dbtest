package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 Change Buffer 模式集合和资源配置边界，防止配置名与实际可缓冲操作发生漂移。 */
class ChangeBufferModeTest {

    /** 固定 MySQL 8.0 六种模式对 INSERT、delete-mark 和 purge-delete 的精确包含关系。 */
    @Test
    void exposesExactOperationSets() {
        assertFalse(ChangeBufferMode.NONE.allows(ChangeBufferOperation.INSERT));
        assertTrue(ChangeBufferMode.INSERTS.allows(ChangeBufferOperation.INSERT));
        assertFalse(ChangeBufferMode.INSERTS.allows(ChangeBufferOperation.DELETE_MARK));
        assertTrue(ChangeBufferMode.DELETES.allows(ChangeBufferOperation.DELETE_MARK));
        assertFalse(ChangeBufferMode.DELETES.allows(ChangeBufferOperation.DELETE));
        assertTrue(ChangeBufferMode.CHANGES.allows(ChangeBufferOperation.INSERT));
        assertTrue(ChangeBufferMode.CHANGES.allows(ChangeBufferOperation.DELETE_MARK));
        assertFalse(ChangeBufferMode.CHANGES.allows(ChangeBufferOperation.DELETE));
        assertTrue(ChangeBufferMode.PURGES.allows(ChangeBufferOperation.DELETE));
        assertFalse(ChangeBufferMode.PURGES.allows(ChangeBufferOperation.INSERT));
        assertTrue(ChangeBufferMode.ALL.allows(ChangeBufferOperation.INSERT));
        assertTrue(ChangeBufferMode.ALL.allows(ChangeBufferOperation.DELETE_MARK));
        assertTrue(ChangeBufferMode.ALL.allows(ChangeBufferOperation.DELETE));
    }

    /** 资源配置必须有界，尤其禁止超过 InnoDB 公开上限 50% 或使用无界等待。 */
    @Test
    void rejectsUnsafeResourceBounds() {
        assertThrows(DatabaseValidationException.class, () -> new ChangeBufferConfig(
                ChangeBufferMode.ALL, 51, Duration.ofSeconds(1), 8,
                Duration.ofSeconds(1), Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class, () -> new ChangeBufferConfig(
                ChangeBufferMode.ALL, 25, Duration.ZERO, 8,
                Duration.ofSeconds(1), Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class, () -> new ChangeBufferConfig(
                ChangeBufferMode.ALL, 25, Duration.ofSeconds(1), 0,
                Duration.ofSeconds(1), Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class, () -> new ChangeBufferConfig(
                ChangeBufferMode.ALL, 25, Duration.ofSeconds(1),
                ChangeBufferConfig.MAX_MERGE_BATCH_PAGES + 1,
                Duration.ofSeconds(1), Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class, () -> new ChangeBufferConfig(
                ChangeBufferMode.ALL, 25, Duration.ofSeconds(1), 8,
                Duration.ZERO, Duration.ofSeconds(1)));
    }

    /** max-size 必须按 Buffer Pool frame 数计算；不足一个页等价量时仍保留一个可用槽位。 */
    @Test
    void derivesConservativeCapacityFromBufferPoolFrames() {
        ChangeBufferConfig config = ChangeBufferConfig.defaults();

        assertEquals(24L, config.maxBufferedPageEquivalents(96));
        assertEquals(1L, config.maxBufferedPageEquivalents(3));
        assertThrows(DatabaseValidationException.class,
                () -> config.maxBufferedPageEquivalents(0));
    }
}
