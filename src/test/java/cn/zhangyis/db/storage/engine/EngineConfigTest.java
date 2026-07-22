package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.recovery.RecoveryMode;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteMode;
import cn.zhangyis.db.storage.api.undotruncate.UndoTruncationConfig;
import cn.zhangyis.db.storage.trx.PurgeConfig;
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

    /**
     * 验证 {@code fileLayoutUnderBaseDir} 对应的数据库引擎组合根行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
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
        assertEquals(16, c.maxExternalUndoPayloadPages(),
                "外置 undo 默认上限兼顾宽行教学场景与损坏链的读取边界");
        assertEquals(8, c.undoCachedSegmentsPerKind(), "INSERT/UPDATE 默认各保留八个 cached segment");
        assertEquals(Duration.ofSeconds(5), c.undoHistoryTransitionTimeout());
        assertEquals(DoublewriteMode.DETECT_AND_RECOVER, c.doublewriteMode());
        assertEquals(PurgeConfig.defaults(), c.purgeConfig());
        assertEquals(UndoTruncationConfig.defaults(), c.undoTruncationConfig());
        assertEquals(dir.resolve("doublewrite-flush-list.dwb"), c.flushListDoublewriteFile());
        assertEquals(dir.resolve("doublewrite-lru.dwb"), c.lruDoublewriteFile());
    }

    /**
     * 验证 {@code historyTransitionTimeoutIsIndependentAndValidated} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void historyTransitionTimeoutIsIndependentAndValidated() {
        EngineConfig original = valid();
        EngineConfig configured = original.withUndoHistoryTransitionTimeout(Duration.ofMillis(75));
        assertEquals(Duration.ofMillis(75), configured.undoHistoryTransitionTimeout());
        assertEquals(Duration.ofSeconds(5), original.undoHistoryTransitionTimeout());
        assertThrows(DatabaseValidationException.class,
                () -> original.withUndoHistoryTransitionTimeout(Duration.ZERO));
        assertThrows(DatabaseValidationException.class,
                () -> original.withUndoHistoryTransitionTimeout(null));
    }

    /**
     * 验证 {@code undoCacheCapacityCanBeDisabledAndMustFitRollbackHeaderPage} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void undoCacheCapacityCanBeDisabledAndMustFitRollbackHeaderPage() {
        EngineConfig config = valid();
        assertEquals(0, config.withUndoCachedSegmentsPerKind(0).undoCachedSegmentsPerKind(),
                "0 是显式禁用，终结路径退回物理 drop");
        assertEquals(3, config.withUndoCachedSegmentsPerKind(3).undoCachedSegmentsPerKind());
        assertThrows(DatabaseValidationException.class,
                () -> config.withUndoCachedSegmentsPerKind(-1));
        assertThrows(DatabaseValidationException.class,
                () -> config.withUndoCachedSegmentsPerKind(2_000),
                "slot array 与两个 cache array 的组合布局不能越过 FIL trailer");
    }

    /**
     * 验证 {@code defaultConfigEnablesRedoRotation} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
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

    /** LogBlock ring 的容量单位是完整 512B block，配置不能留下永远不可用的碎片尾部。 */
    @Test
    void redoRotationCapacityMustBePositive512ByteMultiple() {
        assertThrows(DatabaseValidationException.class, () -> new RedoRotationConfig(2, 511));
        assertThrows(DatabaseValidationException.class, () -> new RedoRotationConfig(2, 513));
        assertThrows(DatabaseValidationException.class,
                () -> new RedoRotationConfig(2, (long) Integer.MAX_VALUE + 1));
        assertEquals(512, new RedoRotationConfig(2, 512).fileBytes());
    }

    /**
     * 验证 {@code bufferPoolInstanceCountDefaultsToOneAndIsConfigurable} 对应的数据库引擎组合根行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
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

    /** 外置 undo 链上限必须能被任一 buffer pool 分片容纳，避免单次 MTR 固定页超过分片容量。 */
    @Test
    void externalUndoPayloadPageLimitIsConfigurableAndBoundedBySmallestPoolInstance() {
        EngineConfig c = valid();
        EngineConfig configured = c.withMaxExternalUndoPayloadPages(32);
        assertEquals(32, configured.maxExternalUndoPayloadPages());
        assertEquals(16, c.maxExternalUndoPayloadPages(), "wither 保持原配置不可变");

        EngineConfig sharded = c.withBufferPoolInstanceCount(4);
        assertEquals(16, sharded.maxExternalUndoPayloadPages(),
                "既有上限在最小分片容量内时应原样保留");
        assertThrows(DatabaseValidationException.class,
                () -> sharded.withMaxExternalUndoPayloadPages(65),
                "256 帧分成 4 片后，单条外置链不能超过任一 64 帧分片");
        assertThrows(DatabaseValidationException.class,
                () -> c.withMaxExternalUndoPayloadPages(0));
    }

    /**
     * 验证 {@code recoveryModeDefaultsToNormalAndIsConfigurable} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
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

    /**
     * 验证 {@code doublewriteModeIsImmutableAndConfigurable} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void doublewriteModeIsImmutableAndConfigurable() {
        EngineConfig c = valid();
        EngineConfig detectOnly = c.withDoublewriteMode(DoublewriteMode.DETECT_ONLY);
        assertEquals(DoublewriteMode.DETECT_ONLY, detectOnly.doublewriteMode());
        assertEquals(DoublewriteMode.DETECT_AND_RECOVER, c.doublewriteMode());
        assertThrows(DatabaseValidationException.class, () -> c.withDoublewriteMode(null));
    }

    /** purge worker 边界通过不可变 wither 接入组合根，且空配置在构造线程池前被领域校验拒绝。 */
    @Test
    void purgeConfigDefaultsAndIsImmutableAndConfigurable() {
        EngineConfig original = valid();
        PurgeConfig custom = new PurgeConfig(2, 8, Duration.ofMillis(250));

        EngineConfig configured = original.withPurgeConfig(custom);

        assertEquals(custom, configured.purgeConfig());
        assertEquals(PurgeConfig.defaults(), original.purgeConfig());
        assertThrows(DatabaseValidationException.class, () -> original.withPurgeConfig(null));
    }

    /** 自动 undo 截断策略通过不可变 wither 接入组合根，且其它后台维护策略必须原样保留。 */
    @Test
    void undoTruncationConfigDefaultsAndIsImmutableAndConfigurable() {
        EngineConfig original = valid();
        UndoTruncationConfig custom = new UndoTruncationConfig(false, 4, Duration.ofSeconds(7));

        EngineConfig configured = original.withUndoTruncationConfig(custom);

        assertEquals(custom, configured.undoTruncationConfig());
        assertEquals(UndoTruncationConfig.defaults(), original.undoTruncationConfig());
        assertEquals(original.purgeConfig(), configured.purgeConfig(),
                "undo 截断策略不能意外改变 purge worker 资源边界");
        assertEquals(custom, configured.withPurgeConfig(new PurgeConfig(
                2, 8, Duration.ofMillis(250))).undoTruncationConfig(),
                "其它 wither 必须保留已经选择的自动截断策略");
        assertThrows(DatabaseValidationException.class,
                () -> original.withUndoTruncationConfig(null));
    }

    /**
     * 验证 {@code forceSkippedSpacesAreSnapshottedAndCanEnableForceSkipMode} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
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

    /**
     * 验证 {@code recoveryTablespacesAreValidatedAndSnapshotted} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
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

    /**
     * 验证 {@code backgroundFlushSettingsAreValidatedAndSnapshotted} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     */
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

    /**
     * 验证 {@code rejectsNullAndNonPositiveFields} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
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
