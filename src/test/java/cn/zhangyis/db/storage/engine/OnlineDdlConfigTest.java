package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Online DDL 配置边界测试。旧 EngineConfig 构造器必须继续得到稳定默认值，避免配置扩展破坏既有调用方。
 */
class OnlineDdlConfigTest {

    @TempDir
    Path directory;

    /** 默认容量、扫描批次和 abort reserve 必须与 design 固定值一致。 */
    @Test
    void shouldExposeStableDefaults() {
        OnlineDdlConfig defaults = OnlineDdlConfig.defaults();

        assertEquals(128L * 1024 * 1024, defaults.maxRowLogBytes());
        assertEquals(256, defaults.scanBatchRows());
        assertEquals(4 * 1024, defaults.abortReserveBytes());
    }

    /** 旧九参构造器无需修改即可获得默认 Online DDL 配置和受控日志目录。 */
    @Test
    void shouldKeepLegacyEngineConfigConstructorCompatible() {
        EngineConfig config = new EngineConfig(directory, PageSize.ofBytes(16 * 1024), 64,
                SpaceId.of(90), PageNo.of(64), 16, 32,
                Duration.ofSeconds(2), 1024 * 1024);

        assertEquals(OnlineDdlConfig.defaults(), config.onlineDdlConfig());
        assertEquals(directory.resolve("online-ddl"), config.onlineDdlDirectory());
    }

    /** 容量必须给 terminal abort frame 留出独立空间，批次也不能退化为空循环。 */
    @Test
    void shouldRejectInvalidBounds() {
        assertThrows(DatabaseValidationException.class,
                () -> new OnlineDdlConfig(4096, 256, 4096));
        assertThrows(DatabaseValidationException.class,
                () -> new OnlineDdlConfig(8192, 0, 4096));
        assertThrows(DatabaseValidationException.class,
                () -> new OnlineDdlConfig(8192, 256, 0));
        assertThrows(DatabaseValidationException.class,
                () -> new OnlineDdlConfig(8192, 256, 64));
    }
}
