package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 自动 undo truncate 配置测试：固定默认启用、extent 阈值和冷却边界。 */
class UndoTruncationConfigTest {

    /** 默认策略必须与生产设计一致，避免兼容构造器静默关闭自动回收。 */
    @Test
    void defaultsEnableOneExtentWithThirtySecondCooldown() {
        UndoTruncationConfig config = UndoTruncationConfig.defaults();

        assertTrue(config.enabled());
        assertEquals(1, config.minReclaimableExtents());
        assertEquals(Duration.ofSeconds(30), config.checkInterval());
    }

    /** extent 阈值和冷却必须严格为正，禁用配置也不能携带未来启用时无效的边界。 */
    @Test
    void rejectsInvalidThresholdAndInterval() {
        assertThrows(DatabaseValidationException.class,
                () -> new UndoTruncationConfig(true, 0, Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class,
                () -> new UndoTruncationConfig(true, 1, Duration.ZERO));
        assertThrows(DatabaseValidationException.class,
                () -> new UndoTruncationConfig(false, 1, null));
    }
}
