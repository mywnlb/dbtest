package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link StorageEngine} 启动配置（E1/E2/E3a）。固定文件布局在 {@code baseDir} 下：redo 日志 {@code redo.log}、
 * redo control（checkpoint label）{@code redo-control}、系统 undo 表空间 {@code undo_<undoSpaceId>.ibu}；
 * 数据表空间文件路径由调用方建/开（本片无 data dictionary）。
 *
 * @param baseDir                  引擎数据目录（redo/control/undo 文件所在）。
 * @param pageSize                 实例页大小。
 * @param bufferPoolCapacityFrames buffer pool 帧数；E1 假设 &gt; 工作集（不发生脏页淘汰，见 slice WAL 限制）。
 * @param undoSpaceId              系统 undo 表空间编号（引擎 open 时建/开）。
 * @param undoSpaceInitialPages    undo 表空间初始页数。
 * @param slotCapacity             rollback segment slot 容量（{@code RollbackSegmentSlotManager}）。
 * @param maxVersionHops           MVCC 版本链最大跳数（{@code MvccReader} 防环）。
 * @param flushTimeout             {@code FlushService.flushThrough} 的等待超时。
 * @param redoCapacityBytes        redo 容量策略字节数（{@code RedoCapacityPolicy.fixed}）。
 * @param recoveryTablespaces      E2 启动恢复期显式打开的数据表空间；无 DD discovery 前只能恢复这些空间。
 * @param backgroundFlushEnabled   E3a 是否在 open 成功后启动后台 page cleaner；禁用仅用于定向测试或故障隔离。
 * @param pageCleanerQueueCapacity 显式 flush 请求队列容量，防止后台线程停滞时无限积压。
 * @param backgroundFlushInterval  后台 page cleaner 空闲 tick 间隔；tick 会评估 redo capacity 并尝试推进 checkpoint。
 * @param backgroundFlushMaxPages  后台 tick 每轮最多刷出的页数；0 表示只推进 checkpoint，不主动刷脏。
 * @param backgroundFlushStopTimeout close 时等待后台线程退出的边界，超时会作为 close 错误上报。
 * @param redoRotation             redo 文件环配置（0.18b）；{@code null}=单 append-only redo 文件，非空=启用文件环 + checkpoint 回收。
 * @param bufferPoolInstanceCount  buffer pool 分片数（0.10d，≥1 且 ≤ bufferPoolCapacityFrames）；默认 1（生产保守，对齐
 *                                 单实例池行为），由 {@link StorageEngine} 经本访问器构造 {@code LruBufferPool}。
 */
public record EngineConfig(Path baseDir, PageSize pageSize, int bufferPoolCapacityFrames,
                           SpaceId undoSpaceId, PageNo undoSpaceInitialPages, int slotCapacity,
                           int maxVersionHops, Duration flushTimeout, long redoCapacityBytes,
                           List<EngineTablespaceConfig> recoveryTablespaces,
                           boolean backgroundFlushEnabled, int pageCleanerQueueCapacity,
                           Duration backgroundFlushInterval, int backgroundFlushMaxPages,
                           Duration backgroundFlushStopTimeout, RedoRotationConfig redoRotation,
                           int bufferPoolInstanceCount) {

    /** 默认启动后台 page cleaner，使 engine open 后具备持续 checkpoint tick 能力。 */
    private static final boolean DEFAULT_BACKGROUND_FLUSH_ENABLED = true;
    /** 默认后台请求队列容量；当前只有内部 tick 和少量显式请求，不需要无限队列。 */
    private static final int DEFAULT_PAGE_CLEANER_QUEUE_CAPACITY = 4;
    /** 默认后台 tick 间隔；教学实现优先稳定性，避免测试/开发环境频繁空转。 */
    private static final Duration DEFAULT_BACKGROUND_FLUSH_INTERVAL = Duration.ofSeconds(1);
    /** 默认 buffer pool 分片数；生产保守 1（对齐 MySQL 单实例默认 + 本片"机制完整、生产 N=1"基调）。 */
    private static final int DEFAULT_BUFFER_POOL_INSTANCE_COUNT = 1;

    public EngineConfig(Path baseDir, PageSize pageSize, int bufferPoolCapacityFrames,
                        SpaceId undoSpaceId, PageNo undoSpaceInitialPages, int slotCapacity,
                        int maxVersionHops, Duration flushTimeout, long redoCapacityBytes) {
        this(baseDir, pageSize, bufferPoolCapacityFrames, undoSpaceId, undoSpaceInitialPages,
                slotCapacity, maxVersionHops, flushTimeout, redoCapacityBytes, List.of());
    }

    public EngineConfig(Path baseDir, PageSize pageSize, int bufferPoolCapacityFrames,
                        SpaceId undoSpaceId, PageNo undoSpaceInitialPages, int slotCapacity,
                        int maxVersionHops, Duration flushTimeout, long redoCapacityBytes,
                        List<EngineTablespaceConfig> recoveryTablespaces) {
        this(baseDir, pageSize, bufferPoolCapacityFrames, undoSpaceId, undoSpaceInitialPages,
                slotCapacity, maxVersionHops, flushTimeout, redoCapacityBytes, recoveryTablespaces,
                DEFAULT_BACKGROUND_FLUSH_ENABLED, DEFAULT_PAGE_CLEANER_QUEUE_CAPACITY,
                DEFAULT_BACKGROUND_FLUSH_INTERVAL, bufferPoolCapacityFrames, flushTimeout);
    }

    /**
     * 兼容旧签名的便利构造器：让既有 15 参调用点零改动继续编译。0.18 收口后默认 redo 后端为有界文件环
     * （{@link RedoRotationConfig#defaults()}）；如需单 append-only 文件，用 {@link #withSingleFileRedo()} 显式回退。
     */
    public EngineConfig(Path baseDir, PageSize pageSize, int bufferPoolCapacityFrames,
                        SpaceId undoSpaceId, PageNo undoSpaceInitialPages, int slotCapacity,
                        int maxVersionHops, Duration flushTimeout, long redoCapacityBytes,
                        List<EngineTablespaceConfig> recoveryTablespaces,
                        boolean backgroundFlushEnabled, int pageCleanerQueueCapacity,
                        Duration backgroundFlushInterval, int backgroundFlushMaxPages,
                        Duration backgroundFlushStopTimeout) {
        this(baseDir, pageSize, bufferPoolCapacityFrames, undoSpaceId, undoSpaceInitialPages,
                slotCapacity, maxVersionHops, flushTimeout, redoCapacityBytes, recoveryTablespaces,
                backgroundFlushEnabled, pageCleanerQueueCapacity, backgroundFlushInterval,
                backgroundFlushMaxPages, backgroundFlushStopTimeout, RedoRotationConfig.defaults(),
                DEFAULT_BUFFER_POOL_INSTANCE_COUNT);
    }

    public EngineConfig {
        if (baseDir == null || pageSize == null || undoSpaceId == null
                || undoSpaceInitialPages == null || flushTimeout == null || recoveryTablespaces == null
                || backgroundFlushInterval == null || backgroundFlushStopTimeout == null) {
            throw new DatabaseValidationException("engine config object fields must not be null");
        }
        if (bufferPoolCapacityFrames <= 0) {
            throw new DatabaseValidationException("bufferPoolCapacityFrames must be positive: " + bufferPoolCapacityFrames);
        }
        if (undoSpaceInitialPages.value() <= 0) {
            throw new DatabaseValidationException("undoSpaceInitialPages must be positive: " + undoSpaceInitialPages.value());
        }
        if (slotCapacity <= 0) {
            throw new DatabaseValidationException("slotCapacity must be positive: " + slotCapacity);
        }
        if (maxVersionHops <= 0) {
            throw new DatabaseValidationException("maxVersionHops must be positive: " + maxVersionHops);
        }
        if (flushTimeout.isZero() || flushTimeout.isNegative()) {
            throw new DatabaseValidationException("flushTimeout must be positive: " + flushTimeout);
        }
        if (redoCapacityBytes <= 0) {
            throw new DatabaseValidationException("redoCapacityBytes must be positive: " + redoCapacityBytes);
        }
        if (pageCleanerQueueCapacity <= 0) {
            throw new DatabaseValidationException("pageCleanerQueueCapacity must be positive: "
                    + pageCleanerQueueCapacity);
        }
        if (backgroundFlushInterval.isZero() || backgroundFlushInterval.isNegative()) {
            throw new DatabaseValidationException("backgroundFlushInterval must be positive: "
                    + backgroundFlushInterval);
        }
        if (backgroundFlushMaxPages < 0) {
            throw new DatabaseValidationException("backgroundFlushMaxPages must not be negative: "
                    + backgroundFlushMaxPages);
        }
        if (backgroundFlushStopTimeout.isZero() || backgroundFlushStopTimeout.isNegative()) {
            throw new DatabaseValidationException("backgroundFlushStopTimeout must be positive: "
                    + backgroundFlushStopTimeout);
        }
        if (bufferPoolInstanceCount < 1) {
            throw new DatabaseValidationException("bufferPoolInstanceCount must be >= 1: " + bufferPoolInstanceCount);
        }
        // 每个分片至少分到 1 帧，否则容量切分非法（与 LruBufferPool.buildInstances 一致，提前在 config 层给清晰错误）。
        if (bufferPoolInstanceCount > bufferPoolCapacityFrames) {
            throw new DatabaseValidationException("bufferPoolInstanceCount must be <= bufferPoolCapacityFrames: count="
                    + bufferPoolInstanceCount + " frames=" + bufferPoolCapacityFrames);
        }
        validateRecoveryTablespaces(undoSpaceId, recoveryTablespaces);
        recoveryTablespaces = List.copyOf(recoveryTablespaces);
    }

    /** redo 日志文件路径（单文件模式）。 */
    public Path redoFile() {
        return baseDir.resolve("redo.log");
    }

    /** redo 文件环目录（0.18b 文件环模式），环内文件命名为 {@code redo-NNNNNN.log}。 */
    public Path redoDir() {
        return baseDir.resolve("redo");
    }

    /** 是否启用 redo 文件环。 */
    public boolean redoRotationEnabled() {
        return redoRotation != null;
    }

    /**
     * 派生一个使用自定义 redo 文件环的配置副本（其余字段不变）。便于在不重复罗列全部组件的情况下指定文件数/容量。
     *
     * @param fileCount 文件数（≥2）。
     * @param fileBytes 单文件帧容量上限（不含文件头）。
     * @return 使用指定文件环的新配置。
     */
    public EngineConfig withRedoRotation(int fileCount, long fileBytes) {
        return withRedoRotation(new RedoRotationConfig(fileCount, fileBytes));
    }

    /**
     * 派生一个显式回退到单 append-only redo 文件的配置副本（其余字段不变）。0.18 收口后默认是文件环，
     * 该方法供确实需要单文件语义的场景（如直接检视单 redo 文件的测试）显式 opt-out。
     *
     * @return 使用单文件 redo 的新配置。
     */
    public EngineConfig withSingleFileRedo() {
        return withRedoRotation((RedoRotationConfig) null);
    }

    private EngineConfig withRedoRotation(RedoRotationConfig rotation) {
        return new EngineConfig(baseDir, pageSize, bufferPoolCapacityFrames, undoSpaceId, undoSpaceInitialPages,
                slotCapacity, maxVersionHops, flushTimeout, redoCapacityBytes, recoveryTablespaces,
                backgroundFlushEnabled, pageCleanerQueueCapacity, backgroundFlushInterval,
                backgroundFlushMaxPages, backgroundFlushStopTimeout, rotation, bufferPoolInstanceCount);
    }

    /**
     * 派生一个使用指定 buffer pool 分片数的配置副本（其余字段不变）。生产默认 1（单实例池），测试可经此显式配置 N&gt;1
     * 验证多 instance 行为，无需重复罗列全部组件。
     *
     * @param instanceCount 分片数（≥1 且 ≤ bufferPoolCapacityFrames）。
     * @return 使用指定分片数的新配置。
     */
    public EngineConfig withBufferPoolInstanceCount(int instanceCount) {
        return new EngineConfig(baseDir, pageSize, bufferPoolCapacityFrames, undoSpaceId, undoSpaceInitialPages,
                slotCapacity, maxVersionHops, flushTimeout, redoCapacityBytes, recoveryTablespaces,
                backgroundFlushEnabled, pageCleanerQueueCapacity, backgroundFlushInterval,
                backgroundFlushMaxPages, backgroundFlushStopTimeout, redoRotation, instanceCount);
    }

    /** redo control（checkpoint label）文件路径。 */
    public Path redoControlFile() {
        return baseDir.resolve("redo-control");
    }

    /** 系统 undo 表空间数据文件路径。 */
    public Path undoFile() {
        return baseDir.resolve("undo_" + undoSpaceId.value() + ".ibu");
    }

    /** doublewrite buffer 文件路径（崩溃后修复 torn data page 的整页副本）。 */
    public Path doublewriteFile() {
        return baseDir.resolve("doublewrite.dwb");
    }

    /** buffer pool warmup dump 文件路径（close 保存热页定位、open 预取）。不参与 crash recovery，损坏可丢弃。 */
    public Path bufferPoolDumpFile() {
        return baseDir.resolve("buffer-pool.dump");
    }

    /**
     * 后台 redo flusher（{@code RedoFlushWorker}）的周期间隔。当前派生自 {@link #backgroundFlushInterval()}
     * （与 page cleaner 同后台节奏），以命名 hook 形式暴露：将来若 redo 刷盘需要独立节奏，可升级为独立配置组件
     * 而不改本访问器的调用方。
     */
    public Duration redoFlushInterval() {
        return backgroundFlushInterval;
    }

    /**
     * 校验恢复期显式表空间集合。重复 SpaceId 会导致同一 {@code PageStore} 句柄被打开两次，破坏 registry
     * 中“一个 SpaceId 一个运行时状态”的不变量；系统 undo 由 {@link #undoFile()} 单独管理，也不能再作为
     * 普通数据表空间重复传入。
     */
    private static void validateRecoveryTablespaces(
            SpaceId undoSpaceId,
            List<EngineTablespaceConfig> recoveryTablespaces) {
        Set<SpaceId> seen = new HashSet<>();
        for (EngineTablespaceConfig tablespace : recoveryTablespaces) {
            if (tablespace == null) {
                throw new DatabaseValidationException("engine recovery tablespace entry must not be null");
            }
            if (tablespace.spaceId().equals(undoSpaceId)) {
                throw new DatabaseValidationException("system undo tablespace is managed by engine config: "
                        + undoSpaceId.value());
            }
            if (!seen.add(tablespace.spaceId())) {
                throw new DatabaseValidationException("duplicate engine recovery tablespace space id: "
                        + tablespace.spaceId().value());
            }
        }
    }
}
