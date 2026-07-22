package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 多 worker purge 的资源边界必须在启动线程前完成校验。 */
class PurgeConfigTest {

    /** 默认启用四个 worker，并为 history speculation 与批次等待设置确定上限。 */
    @Test
    void defaultsEnableFourBoundedWorkers() {
        PurgeConfig config = PurgeConfig.defaults();

        assertEquals(4, config.workerCount());
        assertEquals(16, config.maxInFlightLogs());
        assertEquals(Duration.ofSeconds(5), config.batchTimeout());
    }

    /** 非法线程数、队列容量或等待时间必须在任何线程创建前失败。 */
    @Test
    void rejectsInvalidResourceBounds() {
        assertThrows(DatabaseValidationException.class,
                () -> new PurgeConfig(0, 16, Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class,
                () -> new PurgeConfig(33, 16, Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class,
                () -> new PurgeConfig(4, 0, Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class,
                () -> new PurgeConfig(4, 4097, Duration.ofSeconds(1)));
        assertThrows(DatabaseValidationException.class,
                () -> new PurgeConfig(4, 16, Duration.ZERO));
        assertThrows(DatabaseValidationException.class,
                () -> new PurgeConfig(4, 16, Duration.ofNanos(-1)));
        assertThrows(DatabaseValidationException.class,
                () -> new PurgeConfig(4, 16, null));
    }
}
