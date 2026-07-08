package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.recovery.RecoveryMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;

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
        assertEquals(dir.resolve("recovery-progress.jsonl"), c.recoveryProgressFile());
        assertEquals(List.of(), c.recoveryTablespaces());
        assertTrue(c.backgroundFlushEnabled());
        assertEquals(4, c.pageCleanerQueueCapacity());
        assertEquals(Duration.ofSeconds(1), c.backgroundFlushInterval());
        assertEquals(c.bufferPoolCapacityFrames(), c.backgroundFlushMaxPages());
        assertEquals(c.flushTimeout(), c.backgroundFlushStopTimeout());
    }

    @Test
    void defaultConfigEnablesRedoRotation() {
        // 0.18 收口：默认 redo 后端改为有界文件环（不再单 append-only 文件）。
        EngineConfig c = valid();
        assertTrue(c.redoRotationEnabled(), "默认配置应启用 redo 文件环");
        assertEquals(RedoRotationConfig.defaults(), c.redoRotation());
        assertEquals(dir.resolve("redo"), c.redoDir());
        // 显式 opt-out 到单文件仍可用（canonical 构造器传 null）。
        EngineConfig single = c.withSingleFileRedo();
        assertFalse(single.redoRotationEnabled(), "withSingleFileRedo 显式回退单文件");
    }

    @Test
    void bufferPoolInstanceCountDefaultsToOneAndIsConfigurable() {
        EngineConfig c = valid();
        assertEquals(1, c.bufferPoolInstanceCount(), "默认单实例池（生产保守 N=1）");

        EngineConfig sharded = c.withBufferPoolInstanceCount(4);
        assertEquals(4, sharded.bufferPoolInstanceCount(), "wither 配置 N>1");
        assertEquals(1, c.bufferPoolInstanceCount(), "wither 不改原配置（不可变）");

        assertThrows(DatabaseValidationException.class, () -> c.withBufferPoolInstanceCount(0),
                "分片数须 >=1");
        // valid() 容量 256 帧；分片数不能超过帧数（否则有分片分到 0 帧）。
        assertThrows(DatabaseValidationException.class, () -> c.withBufferPoolInstanceCount(257),
                "分片数须 <= bufferPoolCapacityFrames");
    }

    @Test
    void recoveryModeDefaultsToNormalAndIsConfigurable() {
        EngineConfig c = valid();
        assertEquals(RecoveryMode.NORMAL, c.recoveryMode(),
                "默认启动路径保持普通 crash recovery，不改变既有生产行为");

        EngineConfig readOnly = c.withRecoveryMode(RecoveryMode.READ_ONLY_VALIDATE);
        assertEquals(RecoveryMode.READ_ONLY_VALIDATE, readOnly.recoveryMode());
        assertEquals(RecoveryMode.NORMAL, c.recoveryMode(), "wither 保持 EngineConfig 不可变");
        assertEquals(c.bufferPoolInstanceCount(), readOnly.bufferPoolInstanceCount());
        assertEquals(c.redoRotation(), readOnly.redoRotation());

        assertThrows(DatabaseValidationException.class, () -> c.withRecoveryMode(null),
                "recovery mode 必须显式非空，避免启动时落入未定义恢复语义");
    }

    @Test
    void forceSkippedSpacesAreSnapshottedAndCanEnableForceSkipMode() {
        EngineConfig c = valid();
        EngineConfig withSkipped = c.withForceSkippedSpaces(Set.of(SpaceId.of(10)));
        assertEquals(Set.of(SpaceId.of(10)), withSkipped.forceSkippedSpaces());
        assertEquals(Set.of(), c.forceSkippedSpaces(), "wither 保持 EngineConfig 不可变");
        assertThrows(UnsupportedOperationException.class,
                () -> withSkipped.forceSkippedSpaces().add(SpaceId.of(11)));

        EngineConfig force = c.withForceSkipRecovery(Set.of(SpaceId.of(10)));
        assertEquals(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE, force.recoveryMode());
        assertEquals(Set.of(SpaceId.of(10)), force.forceSkippedSpaces());
        assertThrows(DatabaseValidationException.class, () -> c.withForceSkippedSpaces(null));
        assertThrows(DatabaseValidationException.class, () -> c.withForceSkippedSpaces(
                new java.util.HashSet<>(java.util.Arrays.asList(SpaceId.of(10), null))));
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
