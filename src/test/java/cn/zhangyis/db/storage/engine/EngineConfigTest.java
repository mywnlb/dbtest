package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertEquals(List.of(), c.recoveryTablespaces());
        assertTrue(c.backgroundFlushEnabled());
        assertEquals(4, c.pageCleanerQueueCapacity());
        assertEquals(Duration.ofSeconds(1), c.backgroundFlushInterval());
        assertEquals(c.bufferPoolCapacityFrames(), c.backgroundFlushMaxPages());
        assertEquals(c.flushTimeout(), c.backgroundFlushStopTimeout());
    }

    @Test
    void recoveryTablespacesAreValidatedAndSnapshotted() {
        Path path = dir.resolve("data.ibd");
        EngineTablespaceConfig tablespace = new EngineTablespaceConfig(SpaceId.of(10), path);
        EngineConfig c = new EngineConfig(dir, PageSize.ofBytes(16 * 1024), 256, SpaceId.of(5),
                PageNo.of(64), 64, 100, Duration.ofSeconds(5), 64L * 1024 * 1024,
                List.of(tablespace));

        assertEquals(List.of(tablespace), c.recoveryTablespaces());
        assertThrows(UnsupportedOperationException.class,
                () -> c.recoveryTablespaces().add(new EngineTablespaceConfig(SpaceId.of(11), dir.resolve("x.ibd"))));
        assertThrows(DatabaseValidationException.class, () -> new EngineTablespaceConfig(null, path));
        assertThrows(DatabaseValidationException.class, () -> new EngineTablespaceConfig(SpaceId.of(10), null));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L, null));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L,
                List.of(tablespace, new EngineTablespaceConfig(SpaceId.of(10), dir.resolve("same.ibd")))));
    }

    @Test
    void backgroundFlushSettingsAreValidatedAndSnapshotted() {
        EngineConfig c = new EngineConfig(dir, PageSize.ofBytes(16 * 1024), 256, SpaceId.of(5),
                PageNo.of(64), 64, 100, Duration.ofSeconds(5), 64L * 1024 * 1024,
                List.of(), false, 8, Duration.ofMillis(25), 0, Duration.ofSeconds(2));

        assertFalse(c.backgroundFlushEnabled());
        assertEquals(8, c.pageCleanerQueueCapacity());
        assertEquals(Duration.ofMillis(25), c.backgroundFlushInterval());
        assertEquals(0, c.backgroundFlushMaxPages());
        assertEquals(Duration.ofSeconds(2), c.backgroundFlushStopTimeout());

        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L,
                List.of(), true, 0, Duration.ofMillis(25), 1, Duration.ofSeconds(2)));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L,
                List.of(), true, 4, Duration.ZERO, 1, Duration.ofSeconds(2)));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L,
                List.of(), true, 4, Duration.ofMillis(25), -1, Duration.ofSeconds(2)));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L,
                List.of(), true, 4, Duration.ofMillis(25), 1, Duration.ZERO));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L,
                List.of(), true, 4, null, 1, Duration.ofSeconds(2)));
        assertThrows(DatabaseValidationException.class, () -> new EngineConfig(dir, PageSize.ofBytes(16 * 1024),
                256, SpaceId.of(5), PageNo.of(64), 64, 100, Duration.ofSeconds(5), 1L,
                List.of(), true, 4, Duration.ofMillis(25), 1, null));
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
