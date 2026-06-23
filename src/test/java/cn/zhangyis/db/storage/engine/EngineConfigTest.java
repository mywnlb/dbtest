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

/** E1-T1 EngineConfig：字段校验 + 固定文件布局（redo.log / redo-control / undo_<id>.ibu）。 */
class EngineConfigTest {

    @TempDir
    Path dir;

    private EngineConfig valid() {
        return new EngineConfig(dir, PageSize.ofBytes(16 * 1024), 256, SpaceId.of(5),
                PageNo.of(64), 64, 100, Duration.ofSeconds(5), 64L * 1024 * 1024);
    }

    @Test
    void fileLayoutUnderBaseDir() {
        EngineConfig c = valid();
        assertEquals(dir.resolve("redo.log"), c.redoFile());
        assertEquals(dir.resolve("redo-control"), c.redoControlFile());
        assertEquals(dir.resolve("undo_5.ibu"), c.undoFile());
    }

    @Test
    void rejectsNullAndNonPositiveFields() {
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(null, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                0, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(0), 64, 100, Duration.ofSeconds(5), 1L));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 0, 100, Duration.ofSeconds(5), 1L));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 0, Duration.ofSeconds(5), 1L));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ZERO, 1L));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 0L));
    }
}
