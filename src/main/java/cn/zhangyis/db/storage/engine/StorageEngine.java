package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.tablespace.PageZeroTablespaceMetadataLoader;
import cn.zhangyis.db.storage.api.undotruncate.UndoTablespaceTruncationRecovery;
import cn.zhangyis.db.storage.api.undotruncate.UndoTablespaceTruncationService;
import cn.zhangyis.db.storage.api.undotruncate.UndoTruncationFaultInjector;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.meta.CachingTablespaceRegistry;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import cn.zhangyis.db.storage.flush.CoordinatedDirtyVictimFlusher;
import cn.zhangyis.db.storage.flush.FlushCoordinator;
import cn.zhangyis.db.storage.flush.FlushCycleResult;
import cn.zhangyis.db.storage.flush.FlushService;
import cn.zhangyis.db.storage.flush.cleaner.PageCleanerState;
import cn.zhangyis.db.storage.flush.cleaner.PageCleanerWorker;
import cn.zhangyis.db.storage.buf.ReadAheadService;
import cn.zhangyis.db.storage.buf.BufferPoolWarmupService;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteFileRepository;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.flush.doublewrite.RecoverableDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.recovery.CrashRecoveryService;
import cn.zhangyis.db.storage.recovery.RecoveryReport;
import cn.zhangyis.db.storage.recovery.RecoveryRequest;
import cn.zhangyis.db.storage.recovery.RecoveryState;
import cn.zhangyis.db.storage.recovery.RecoveryTrafficGate;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCapacityPolicy;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoFlushWorker;
import cn.zhangyis.db.storage.redo.RedoFlushWorkerState;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoReclaimBoundary;
import cn.zhangyis.db.storage.redo.RotatingRedoLogRepository;
import cn.zhangyis.db.storage.redo.RedoLogManagerFlushTarget;
import cn.zhangyis.db.storage.trx.HistoryEntry;
import cn.zhangyis.db.storage.trx.HistoryList;
import cn.zhangyis.db.storage.trx.PurgeCoordinator;
import cn.zhangyis.db.storage.trx.PurgeDriverWorker;
import cn.zhangyis.db.storage.trx.MvccReader;
import cn.zhangyis.db.storage.trx.RollbackSegmentSlotManager;
import cn.zhangyis.db.storage.trx.RollbackService;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.TransactionSystem;
import cn.zhangyis.db.storage.trx.UndoLogManager;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 存储引擎组合根（engine bootstrap E1/E2/E3a，设计 §3/§13）。把此前仅在测试中构造的存储组件接线成一个生产实例：
 * 共享单一 {@link TablespaceAccessController}，redo 用 durable {@link RedoLogManager}，使 **WAL 顺序在生产
 * flush/checkpoint 路径与 commit 之间成立**（redo durable 必先于 data page 写数据文件）。
 *
 * <p><b>生命周期</b>：{@link #open()}（fresh 建 redo/系统 undo 表空间；existing 打开配置表空间并运行
 * {@link CrashRecoveryService}：doublewrite repair、redo replay、安装 redo 边界、UNDO truncate 续作、
 * SPACE_FILE_RECONCILE）→ {@link #checkpoint()}/{@link #close()} 经 {@link FlushService#flushThrough} 按 WAL
 * 顺序持久（先 redo.flush 再刷脏页再持久 checkpoint），close 末关闭 AutoCloseable 句柄。
 *
 * <p><b>当前限制</b>：启动后已有后台 redo flusher、单线程 page cleaner；配置单聚簇索引时会启动
 * {@code PurgeCoordinator} + purge driver。后台 worker 失败仍无 supervisor 重启；事务 UNDO_ROLLBACK / RESUME_PURGE
 * 目前是 engine 后恢复步并依赖显式单聚簇索引，尚未成为正式 recovery stage；DDL_RECOVERY 仍未接入。
 * doublewrite 已常开为 {@link RecoverableDoublewriteStrategy}，但恢复页列表来自 doublewrite 文件有效 slot 并过滤到
 * recovery 已打开空间，仍没有全空间 checksum discovery。
 *
 * <p>访问器暴露已接线的事务/disk/btree/undo/mvcc/rollback 服务，供测试与未来 DML facade（E4）驱动；本类不含 DML 逻辑。
 */
public final class StorageEngine {

    private final EngineConfig config;
    private EngineState state = EngineState.NEW;

    // AutoCloseable 句柄（close 时关闭）
    private PageStore store;
    private RedoLogFileRepository redoRepo;
    private RedoCheckpointStore checkpointStore;
    /** doublewrite buffer 文件仓储；前向刷脏写整页副本，恢复期供 scanner 修复 torn data page。 */
    private DoublewriteFileRepository doublewriteRepo;
    private BufferPool pool;
    /** E2 起由 engine 显式持有的运行时表空间 registry，确保 disk facade 与 undo recovery 共享同一状态视图。 */
    private TablespaceRegistry registry;
    /** 每个表空间的 operation lease 控制器；同一实例注入 MTR、page0 loader、flush 和 undo truncate recovery。 */
    private TablespaceAccessController accessController;
    /** 启动恢复流量门控；fresh open 直接打开，existing open 由 {@link CrashRecoveryService} 驱动状态转换。 */
    private final RecoveryTrafficGate recoveryGate = new RecoveryTrafficGate();

    // 接线的服务
    private RedoLogManager redo;
    private MiniTransactionManager miniTransactionManager;
    private DiskSpaceManager diskSpaceManager;
    private TransactionManager transactionManager;
    private UndoLogManager undoLogManager;
    private SplitCapableBTreeIndexService btreeService;
    private IndexPageAccess indexPageAccess;
    private MvccReader mvccReader;
    private RollbackService rollbackService;
    /** 内存 rseg slot 目录；0.3 起 claim/release 持久化到 page3，恢复期扫描重建。 */
    private RollbackSegmentSlotManager rollbackSlots;
    /** 持久 rseg header 仓储（page3）；fresh 格式化、claim/release 持久、恢复扫描共用。 */
    private RollbackSegmentHeaderRepository rsegHeaderRepo;
    /** undo 段物理设施；恢复期读 undo 段 state、回滚 ACTIVE 段用。 */
    private UndoLogSegmentAccess undoAccess;
    /** 单显式配置聚簇索引（无 DD，open 前 set）：同时服务恢复期回滚（R 1.2）与后台 purge（0.4）；null 则都跳过。 */
    private BTreeIndex clusteredIndex;
    /** 事务系统（id/no 分配 + active 表 + purge low water）；purge boundary 来源。 */
    private TransactionSystem txnSystem;
    /** 已提交 undo 的 history list；undoLogManager 写入、purge 读出，须同一实例。 */
    private HistoryList history;
    /** undo 段分配/回收端口；purge dropUndoSegment 用。 */
    private DiskSpaceUndoAllocator undoAllocator;
    /** E3a 后台 purge driver（0.4）；配置聚簇索引时启动，周期驱动 PurgeCoordinator.runBatch。 */
    private PurgeDriverWorker purgeDriverWorker;
    private FlushService flushService;
    /** E3a 后台 page cleaner；只由 engine 生命周期启动/停止，所有刷脏仍走 {@link FlushService}。 */
    private PageCleanerWorker pageCleanerWorker;
    /** 0.10a 后台 linear read-ahead 服务；只由 engine 生命周期启动/停止，预取经 {@code BufferPool.prefetch}（不 fix）。 */
    private ReadAheadService readAheadService;
    /** linear read-ahead 触发阈值（同一 extent 连续访问页数，对齐 InnoDB 默认 56）。当前固定常量，按需再升为 config。 */
    private static final int READ_AHEAD_THRESHOLD = 56;
    /**
     * random read-ahead 触发阈值（0.10c）：0=禁用，对齐 MySQL 默认 {@code innodb_random_read_ahead=OFF}。
     * 禁用时 {@code recordAccess} 不查 residentCountInRange，普通路径无额外开销。生产启用留 config（延后）。
     */
    private static final int RANDOM_READ_AHEAD_THRESHOLD = 0;
    /** read-ahead 预取请求队列容量；队满丢弃，绝不挤占前台需求读。 */
    private static final int READ_AHEAD_QUEUE_CAPACITY = 64;
    /** E3a 后台 redo flusher；周期驱动 redo.flush() 使 durable LSN 自动前进，解淘汰/flush 的 WAL gate 卡顿。 */
    private RedoFlushWorker redoFlushWorker;
    /** E2 启动恢复门面；仅 existing open 调用，失败时保持 gate fail-closed。 */
    private CrashRecoveryService crashRecoveryService;
    /** 最近一次 existing-open 恢复报告；fresh open 为 null，避免把无恢复路径误报成已恢复。 */
    private RecoveryReport lastRecoveryReport;

    public StorageEngine(EngineConfig config) {
        if (config == null) {
            throw new DatabaseValidationException("engine config must not be null");
        }
        this.config = config;
    }

    /**
     * 打开引擎。fresh（baseDir 无 redo.log）建 redo 文件 + 系统 undo 表空间；existing 先打开系统 undo 和
     * {@link EngineConfig#recoveryTablespaces()} 中列出的数据表空间，再由 {@link CrashRecoveryService} 执行
     * checkpoint-aware redo replay、安装恢复边界、UNDO TRUNCATING 续作和 SPACE_FILE_RECONCILE。恢复/建库完成后先
     * 启动后台 page cleaner，再发布 OPEN。
     */
    public void open() {
        if (state != EngineState.NEW) {
            throw new EngineStateException("open requires NEW state: " + state);
        }
        try {
            Files.createDirectories(config.baseDir());
        } catch (IOException e) {
            throw new DatabaseRuntimeException("create engine baseDir failed: " + config.baseDir(), e);
        }
        // redo 后端：默认单 append-only 文件；config 启用文件环时改用 RotatingRedoLogRepository（0.18b），
        // 并把环作为回收边界端口交给 CheckpointCoordinator。fresh 判定按各自存在性：单文件看 redo.log，文件环看 redo 目录。
        RedoReclaimBoundary redoReclaim;
        boolean fresh;
        if (config.redoRotationEnabled()) {
            fresh = !Files.exists(config.redoDir());
            RotatingRedoLogRepository ring = RedoLogFileRepository.openRing(
                    config.redoDir(), config.redoRotation().fileCount(), config.redoRotation().fileBytes());
            this.redoRepo = ring;
            redoReclaim = ring;
        } else {
            fresh = !Files.exists(config.redoFile());
            this.redoRepo = RedoLogFileRepository.open(config.redoFile());
            redoReclaim = null;
        }
        this.redo = RedoLogManager.durable(redoRepo);
        this.checkpointStore = RedoCheckpointStore.open(config.redoControlFile());
        // doublewrite 文件早于 FlushCoordinator/recovery 打开、跨进程持久：恢复枚举到的是上一进程的整页副本。
        this.doublewriteRepo = DoublewriteFileRepository.open(config.doublewriteFile(), config.pageSize());
        this.accessController = new TablespaceAccessController();
        this.store = new FileChannelPageStore();
        // 0.10d：按 config 分片数构造 buffer pool（默认 1=单实例池，生产保守；测试经 withBufferPoolInstanceCount 配 N>1）。
        LruBufferPool lruPool = new LruBufferPool(store, config.pageSize(), config.bufferPoolCapacityFrames(),
                config.bufferPoolInstanceCount());
        this.pool = lruPool;
        this.registry = new CachingTablespaceRegistry(
                new PageZeroTablespaceMetadataLoader(store, config.pageSize(), accessController));
        this.miniTransactionManager = new MiniTransactionManager(accessController, redo);
        this.diskSpaceManager = new DiskSpaceManager(pool, store, config.pageSize(), registry);

        TypeCodecRegistry typeRegistry = new TypeCodecRegistry();
        this.txnSystem = new TransactionSystem();
        this.transactionManager = new TransactionManager(txnSystem);
        this.rollbackSlots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), config.slotCapacity());
        this.rsegHeaderRepo = new RollbackSegmentHeaderRepository(pool, config.pageSize());
        this.history = new HistoryList();
        this.undoAllocator = new DiskSpaceUndoAllocator(diskSpaceManager);
        this.undoAccess = new UndoLogSegmentAccess(pool, config.pageSize(), undoAllocator, typeRegistry);
        this.undoLogManager = new UndoLogManager(undoAccess, rollbackSlots, config.undoSpaceId(), history,
                rsegHeaderRepo, miniTransactionManager);
        this.indexPageAccess = new IndexPageAccess(pool, config.pageSize());
        this.btreeService = new SplitCapableBTreeIndexService(indexPageAccess, diskSpaceManager, typeRegistry);
        this.mvccReader = new MvccReader(miniTransactionManager, btreeService, undoAccess,
                config.undoSpaceId(), config.maxVersionHops());
        this.rollbackService = new RollbackService(btreeService, undoAccess, rollbackSlots, transactionManager,
                miniTransactionManager);

        FlushCoordinator flushCoordinator = new FlushCoordinator(pool, store, redo, config.pageSize(),
                new RecoverableDoublewriteStrategy(doublewriteRepo), config.flushTimeout(), accessController);
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinator(pool, redo, checkpointStore, redoReclaim);
        this.flushService = new FlushService(pool, flushCoordinator, checkpointCoordinator, redo,
                RedoCapacityPolicy.fixed(config.redoCapacityBytes()),
                AdaptiveFlushPolicy.adaptive(1, 1, config.bufferPoolCapacityFrames()));
        // WAL 安全淘汰：注入淘汰刷盘端口，使脏页淘汰复用 FlushCoordinator 的 WAL gate + checksum + doublewrite
        // 管线。必须在 FlushCoordinator 就绪后、任何可能触发淘汰的 page access（fresh 建系统 undo / recover）之前注入。
        lruPool.attachVictimFlusher(new CoordinatedDirtyVictimFlusher(flushCoordinator));
        this.crashRecoveryService = new CrashRecoveryService(recoveryGate);

        if (fresh) {
            // 建系统 undo 表空间（page0/inode 经 MTR 写入，redo 累积，close 时随 flushThrough durable）
            MiniTransaction boot = miniTransactionManager.begin();
            diskSpaceManager.createTablespace(boot, config.undoSpaceId(), config.undoFile(),
                    config.undoSpaceInitialPages(), TablespaceType.UNDO);
            // 0.3：同 boot MTR 格式化 page3 rseg header（空 slot 目录，redo 保护），供 claim/release 持久与恢复扫描。
            rsegHeaderRepo.format(boot, config.undoSpaceId(), rollbackSlots.rollbackSegmentId(), config.slotCapacity());
            miniTransactionManager.commit(boot);
            lastRecoveryReport = null;
        } else {
            recoverExisting();
        }
        // redo flusher 早于 page cleaner 启动：先让 durable LSN 能自动前进，page cleaner 的 WAL gate 才不空等。
        startBackgroundRedoFlusher();
        startBackgroundPageCleaner();
        startBackgroundPurgeDriver();
        // read-ahead 在 bootstrap/recover 之后启动并接钩子：建库/恢复的 page access 不触发预取；之后普通 getPage 顺序访问才驱动。
        startBackgroundReadAhead(lruPool);
        // warmup load：上次 close 的热页定位预取回池（缺失/损坏 dump no-op；未打开空间的页由 prefetch 跳过）。最佳努力，不阻断 open。
        new BufferPoolWarmupService().load(pool, config.bufferPoolDumpFile());
        if (fresh) {
            recoveryGate.openForUserTraffic();
        }
        this.state = EngineState.OPEN;
    }

    /**
     * E2 existing-open 恢复入口。数据流为：先按 recovery 准入打开系统 undo 与显式配置的数据表空间，使
     * {@code PageStore} 拥有 redo apply 所需的物理句柄；再构造不可变 {@link RecoveryRequest}，由
     * {@link CrashRecoveryService} 负责关闭 gate、redo replay、安装 recoveredToLsn、续作 UNDO TRUNCATING、
     * SPACE_FILE_RECONCILE 和开放 gate。
     *
     * <p>简化点：当前引擎已使用 {@link RecoverableDoublewriteStrategy} 并携带
     * {@link DoublewriteRecoveryScanner}/page 列表，但 page 列表只来自 doublewrite 文件中的有效 slot，且仅覆盖启动恢复
     * 已打开的系统 undo 与显式配置数据表空间；没有 data dictionary / 全空间 checksum discovery。事务 UNDO_ROLLBACK
     * 目前是恢复服务之后的 engine 后恢复步并依赖显式单聚簇索引，PURGE_RESUME、DDL_RECOVERY 仍未接入。
     */
    private void recoverExisting() {
        diskSpaceManager.openTablespaceForRecovery(config.undoSpaceId(), config.undoFile());
        for (EngineTablespaceConfig tablespace : config.recoveryTablespaces()) {
            diskSpaceManager.openTablespaceForRecovery(tablespace.spaceId(), tablespace.path());
        }

        // doublewrite repair：scanner 用同一 dwRepo（含上一进程整页副本）。待检查页**只取恢复期已打开的空间**
        // （系统 undo + 配置的 recoveryTablespaces）——没有 DD discovery 前恢复只能触达这些空间，对未打开空间的页
        // 调 scanner 会触发 TablespaceNotOpenException；其它空间的 torn 页留待该空间被显式打开/未来 discovery 时修复。
        List<SpaceId> recoverySpaces = recoverySpaceIds();
        DoublewriteRecoveryScanner doublewriteScanner =
                new DoublewriteRecoveryScanner(doublewriteRepo, store, config.pageSize());
        List<PageId> doublewritePages = new ArrayList<>();
        for (PageId pageId : doublewriteRepo.pageIds()) {
            if (recoverySpaces.contains(pageId.spaceId())) {
                doublewritePages.add(pageId);
            }
        }
        RecoveryRequest request = RecoveryRequest.normal(checkpointStore, redoRepo,
                        RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, config.pageSize()))
                .withDoublewriteRepair(doublewriteScanner, doublewritePages)
                .withRedoBoundaryInstall(redo)
                .withUndoTablespaceRecovery(buildUndoTablespaceRecovery())
                .withSpaceFileReconcile(recoverySpaces);
        lastRecoveryReport = crashRecoveryService.recover(request);
        restoreRollbackSegmentSlots();
    }

    /**
     * 0.3/R 1.2/R 1.3：redo 重放使 page3 物理一致后，扫描 rseg header 重建内存 slot 目录，再逐 slot 读取
     * undo first 页 header。ACTIVE 段在配置单聚簇索引时执行恢复期 rollback；COMMITTED 段按 {@code COMMIT_NO}
     * 重建 committed history，并复位事务 id/no 高水位，供启动后的后台 purge driver 续作。
     *
     * <p>简化点：正式 {@code UndoRecoveryService} / {@code RecoveryStageName.UNDO_ROLLBACK} /
     * {@code RecoveryStageName.RESUME_PURGE} stage 留后续；未配置聚簇索引时 active rollback 和 purge driver 都跳过，
     * 但 committed history 与计数器仍会从持久 undo header 重建。
     */
    private void restoreRollbackSegmentSlots() {
        MiniTransaction scan = miniTransactionManager.begin();
        RollbackSegmentHeaderSnapshot snapshot = rsegHeaderRepo.read(scan, config.undoSpaceId(),
                rollbackSlots.rollbackSegmentId(), config.slotCapacity());
        miniTransactionManager.commit(scan);
        snapshot.occupiedSlots().forEach(rollbackSlots::restore);
        recoverRollbackSegmentTransactions(snapshot.occupiedSlots());
    }

    /**
     * R 1.2/R 1.3 恢复 rseg slot 的事务状态。数据流：每个 page3 restored slot 只读打开 undo first 页，读取
     * {@code STATE}/{@code TRANSACTION_ID}/{@code COMMIT_NO} 后立即提交 MTR 释放 undo latch；随后 ACTIVE 才进入
     * rollback，COMMITTED 只重建 history，不在恢复线程中执行 purge。这样不会在 undo latch 下访问 B+Tree，也避免
     * recovery 抢跑后台 purge 的批次边界。
     *
     * @param occupiedSlots page3 扫描得到的 slot->undo first page 映射。
     */
    private void recoverRollbackSegmentTransactions(Map<UndoSlotId, PageId> occupiedSlots) {
        List<HistoryEntry> committed = new ArrayList<>();
        long nextTransactionId = 1;
        long nextTransactionNo = 1;

        for (Map.Entry<UndoSlotId, PageId> entry : occupiedSlots.entrySet()) {
            RecoveredUndoSlot recovered = readRecoveredUndoSlot(entry.getKey(), entry.getValue());
            if (!recovered.creatorTrxId().isNone()) {
                nextTransactionId = Math.max(nextTransactionId, recovered.creatorTrxId().value() + 1);
            }

            if (recovered.active()) {
                if (clusteredIndex != null) {
                    rollbackService.rollbackRecovered(recovered.firstPageId(), clusteredIndex);
                    rollbackSlots.release(recovered.slotId());
                }
                continue;
            }

            if (recovered.creatorTrxId().isNone() || recovered.commitNo().isNone()) {
                throw new UndoLogFormatException("committed undo slot " + recovered.slotId()
                        + " has invalid creator/commit header: creator=" + recovered.creatorTrxId().value()
                        + ", commitNo=" + recovered.commitNo().value());
            }
            nextTransactionNo = Math.max(nextTransactionNo, recovered.commitNo().value() + 1);
            committed.add(new HistoryEntry(recovered.commitNo(), recovered.creatorTrxId(),
                    recovered.firstPageId().spaceId(), recovered.firstPageId(), recovered.slotId()));
        }

        txnSystem.restoreCounters(nextTransactionId, nextTransactionNo);
        committed.stream()
                .sorted(Comparator.comparingLong(entry -> entry.transactionNo().value()))
                .forEach(history::submitCommitted);
    }

    /**
     * 只读读取一个 restored undo slot 的恢复 header。异常时回滚该只读 MTR，避免损坏 undo 页导致 guard/lease 泄漏。
     * 返回值只含内存快照，不持有 page latch；调用方可安全地在之后进入 B+Tree rollback 或 history 入队。
     */
    private RecoveredUndoSlot readRecoveredUndoSlot(UndoSlotId slotId, PageId firstPageId) {
        MiniTransaction stateMtr = miniTransactionManager.begin();
        try {
            UndoLogSegment segment = undoAccess.open(stateMtr, firstPageId, PageLatchMode.SHARED);
            boolean active = segment.isActive();
            boolean committed = segment.isCommitted();
            if (!active && !committed) {
                throw new UndoLogFormatException("undo slot " + slotId + " first page " + firstPageId
                        + " has unknown state " + segment.state());
            }
            RecoveredUndoSlot recovered = new RecoveredUndoSlot(slotId, firstPageId, active,
                    segment.creatorTransactionId(), segment.committedTransactionNo());
            miniTransactionManager.commit(stateMtr);
            return recovered;
        } catch (RuntimeException e) {
            miniTransactionManager.rollbackUncommitted(stateMtr);
            throw e;
        }
    }

    /**
     * 恢复期从 undo first 页读取出的 slot header 快照。它刻意不保存 {@link UndoLogSegment} 句柄，确保读取 header
     * 的 MTR 已释放后，后续 rollback/purge history 重建才进入 B+Tree 或 history list。
     */
    private record RecoveredUndoSlot(UndoSlotId slotId, PageId firstPageId, boolean active,
                                     TransactionId creatorTrxId, TransactionNo commitNo) {
    }

    /**
     * 构造 UNDO truncate 恢复参与者。它必须共享 engine 的 {@link TablespaceAccessController}、registry、redo 和
     * flushService：恢复续作会在同一空间上获取 X operation lease、写 marker/rebuild redo、走 WAL flush barrier，
     * 最后发布 registry 状态；若这些依赖不是同一实例，普通准入看到的生命周期状态会与恢复写入脱节。
     */
    private UndoTablespaceTruncationRecovery buildUndoTablespaceRecovery() {
        UndoTablespaceTruncationService truncationService = new UndoTablespaceTruncationService(
                pool, store, config.pageSize(), registry, accessController, miniTransactionManager,
                flushService, config.flushTimeout(), UndoTruncationFaultInjector.none());
        return new UndoTablespaceTruncationRecovery(Set.of(config.undoSpaceId()), store, config.pageSize(),
                registry, redo, truncationService);
    }

    /**
     * SPACE_FILE_RECONCILE 输入集合。系统 undo 由引擎固定加入；数据表空间仅限配置显式列出的集合。使用
     * {@link LinkedHashSet} 保持诊断顺序稳定，同时防御未来构造器变化导致的重复 SpaceId。
     */
    private List<SpaceId> recoverySpaceIds() {
        LinkedHashSet<SpaceId> spaces = new LinkedHashSet<>();
        spaces.add(config.undoSpaceId());
        for (EngineTablespaceConfig tablespace : config.recoveryTablespaces()) {
            spaces.add(tablespace.spaceId());
        }
        return List.copyOf(spaces);
    }

    /**
     * 启动后台 page cleaner。它必须晚于 fresh bootstrap MTR 或 existing recovery：worker tick 会读取 redo/checkpoint
     * 和 Buffer Pool dirty view；若在恢复/建系统表空间前启动，会看到尚未稳定的文件句柄和 page0 状态。
     */
    private void startBackgroundPageCleaner() {
        if (!config.backgroundFlushEnabled()) {
            return;
        }
        pageCleanerWorker = new PageCleanerWorker(flushService, config.pageCleanerQueueCapacity(),
                config.backgroundFlushInterval(), config.backgroundFlushMaxPages());
        pageCleanerWorker.start();
    }

    private void startBackgroundRedoFlusher() {
        if (!config.backgroundFlushEnabled()) {
            return;
        }
        redoFlushWorker = new RedoFlushWorker(new RedoLogManagerFlushTarget(redo), config.redoFlushInterval());
        redoFlushWorker.start();
    }

    /**
     * 0.4：启动后台 purge driver。仅在后台启用且已配置单聚簇索引时构造 {@link PurgeCoordinator}（无 DD，单表）+ driver。
     * 未配置索引（既有 engine 测试）则不启动，purge 不跑、行为不变。
     */
    private void startBackgroundPurgeDriver() {
        if (!config.backgroundFlushEnabled() || clusteredIndex == null) {
            return;
        }
        PurgeCoordinator coordinator = new PurgeCoordinator(miniTransactionManager, txnSystem, history,
                undoAccess, undoAllocator, rollbackSlots, btreeService, clusteredIndex);
        purgeDriverWorker = new PurgeDriverWorker(coordinator, config.slotCapacity(), config.backgroundFlushInterval());
        purgeDriverWorker.start();
    }

    /**
     * 0.10a/0.10c：启动后台 read-ahead 服务并接 Buffer Pool 钩子。仅在后台启用时启动；linear 阈值取 InnoDB 默认 56，
     * random 阈值取 {@link #RANDOM_READ_AHEAD_THRESHOLD}=0（禁用，对齐 MySQL OFF），故一般负载（含既有测试）不触发
     * 预取、行为不变。必须晚于 bootstrap/recover，使其只跟踪普通 getPage 访问。
     */
    private void startBackgroundReadAhead(LruBufferPool lruPool) {
        if (!config.backgroundFlushEnabled()) {
            return;
        }
        readAheadService = new ReadAheadService(pool, READ_AHEAD_THRESHOLD, RANDOM_READ_AHEAD_THRESHOLD,
                READ_AHEAD_QUEUE_CAPACITY);
        readAheadService.start();
        lruPool.attachReadAheadHook(readAheadService);
    }

    /**
     * 显式 checkpoint：按 WAL 顺序把 redo 刷盘、刷出全部 dirty page（oldest≤currentLsn）、持久化 checkpoint label。
     */
    public void checkpoint() {
        requireOpen();
        flushService.flushThrough(redo.currentLsn(), config.flushTimeout());
    }

    /**
     * 关闭引擎。先 {@link FlushService#flushThrough}（WAL 顺序持久 + 清空 dirty），再依次关闭 pool（此时 dirty 已空，
     * legacy flushAll 为 no-op，不绕 WAL gate）、store、redoRepo、checkpointStore。幂等：CLOSED 再 close 为 no-op。
     */
    public void close() {
        if (state == EngineState.CLOSED) {
            return;
        }
        if (state != EngineState.OPEN) {
            throw new EngineStateException("close requires OPEN state: " + state);
        }
        stopBackgroundReadAhead();
        stopBackgroundPageCleaner();
        stopBackgroundRedoFlusher();
        stopBackgroundPurgeDriver();
        flushService.flushThrough(redo.currentLsn(), config.flushTimeout());
        // warmup dump：后台 worker 已停、dirty 已刷，residentMap 稳定，保存热页定位供下次 open 预取。最佳努力（IO 失败不抛）。
        new BufferPoolWarmupService().dump(pool, config.bufferPoolDumpFile());
        List<RuntimeException> errors = new ArrayList<>();
        closeQuietly(pool, errors);
        closeQuietly(store, errors);
        closeQuietly(redoRepo, errors);
        closeQuietly(checkpointStore, errors);
        closeQuietly(doublewriteRepo, errors);
        state = EngineState.CLOSED;
        if (!errors.isEmpty()) {
            DatabaseRuntimeException aggregate =
                    new DatabaseRuntimeException("engine close failed to release " + errors.size() + " handle(s)",
                            errors.get(0));
            errors.subList(1, errors.size()).forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    /**
     * 停止后台 page cleaner 后再进入 final flush。若 worker 仍在执行 flush cycle，stop 会等待该轮完成；
     * 超时不继续关闭 page store/buffer pool，避免后台 IO 与句柄释放并发。
     */
    private void stopBackgroundPageCleaner() {
        if (pageCleanerWorker == null) {
            return;
        }
        boolean stopped = pageCleanerWorker.stop(config.backgroundFlushStopTimeout());
        if (!stopped) {
            throw new DatabaseRuntimeException("page cleaner did not stop within "
                    + config.backgroundFlushStopTimeout());
        }
    }

    /** 在 final flush / 关 store 之前停止后台 read-ahead，避免预取盘 IO 与句柄释放并发；超时则中止关闭。 */
    private void stopBackgroundReadAhead() {
        if (readAheadService == null) {
            return;
        }
        boolean stopped = readAheadService.stop(config.backgroundFlushStopTimeout());
        if (!stopped) {
            throw new DatabaseRuntimeException("read-ahead service did not stop within "
                    + config.backgroundFlushStopTimeout());
        }
    }

    /**
     * 在 final flush 前停止后台 redo flusher：worker 若正在 flush 会等待该轮 fsync 完成，超时则中止关闭，
     * 避免后台 redo IO 与 redoRepo 句柄释放并发。
     */
    private void stopBackgroundRedoFlusher() {
        if (redoFlushWorker == null) {
            return;
        }
        boolean stopped = redoFlushWorker.stop(config.backgroundFlushStopTimeout());
        if (!stopped) {
            throw new DatabaseRuntimeException("redo flush worker did not stop within "
                    + config.backgroundFlushStopTimeout());
        }
    }

    /** 在 final flush 前停止后台 purge driver；worker 正跑批次会等其完成，超时中止关闭。 */
    private void stopBackgroundPurgeDriver() {
        if (purgeDriverWorker == null) {
            return;
        }
        boolean stopped = purgeDriverWorker.stop(config.backgroundFlushStopTimeout());
        if (!stopped) {
            throw new DatabaseRuntimeException("purge driver did not stop within "
                    + config.backgroundFlushStopTimeout());
        }
    }

    public MiniTransactionManager miniTransactionManager() {
        requireOpen();
        return miniTransactionManager;
    }

    public TransactionManager transactionManager() {
        requireOpen();
        return transactionManager;
    }

    public DiskSpaceManager diskSpaceManager() {
        requireOpen();
        return diskSpaceManager;
    }

    public SplitCapableBTreeIndexService btreeService() {
        requireOpen();
        return btreeService;
    }

    /** 索引页格式化入口（建聚簇/二级索引 root 页用）。E4 DML/DDL 接线前供测试与上层格式化索引页。 */
    public IndexPageAccess indexPageAccess() {
        requireOpen();
        return indexPageAccess;
    }

    public UndoLogManager undoLogManager() {
        requireOpen();
        return undoLogManager;
    }

    /** 内存 rseg slot 目录（0.3：claim/release 持久到 page3，恢复期由 page3 重建）。 */
    public RollbackSegmentSlotManager rollbackSegmentSlotManager() {
        requireOpen();
        return rollbackSlots;
    }

    /**
     * 配置本 engine 的单聚簇索引（无 DD 前由测试/上层显式注入），**必须在 {@link #open()} 之前**调用。
     * 同时服务恢复期回滚（R 1.2）与后台 purge driver（0.4）。未配置时跳过恢复回滚、不启动 purge driver
     * （既有 engine 测试行为不变）。
     */
    public void configureClusteredIndex(BTreeIndex clusteredIndex) {
        if (state == EngineState.OPEN) {
            throw new EngineStateException("clustered index must be configured before open(): " + state);
        }
        this.clusteredIndex = clusteredIndex;
    }

    public MvccReader mvccReader() {
        requireOpen();
        return mvccReader;
    }

    public RollbackService rollbackService() {
        requireOpen();
        return rollbackService;
    }

    /** 当前生命周期状态。 */
    public EngineState state() {
        return state;
    }

    /**
     * 当前恢复流量门控状态。该方法不要求引擎处于 OPEN：如果 {@link #open()} 在恢复阶段失败，上层仍可读取
     * FAILED 状态和失败根因，确认普通请求没有被错误放行。
     */
    public RecoveryState recoveryState() {
        return recoveryGate.state();
    }

    /**
     * 最近一次 existing-open 启动恢复报告。fresh open 没有恢复报告；恢复失败时由 {@link CrashRecoveryService}
     * 保持 fail-closed 并抛出 {@code RecoveryStartupException}，调用方可通过 {@link #recoveryState()} 诊断。
     */
    public Optional<RecoveryReport> lastRecoveryReport() {
        return Optional.ofNullable(lastRecoveryReport);
    }

    /**
     * 后台 page cleaner 当前状态。该查询不要求 engine OPEN：close 后测试/诊断需要确认 worker 已 STOPPED；
     * 如果配置禁用了后台 worker，则返回 NEW 表示未构造线程。
     */
    public PageCleanerState pageCleanerState() {
        return pageCleanerWorker == null ? PageCleanerState.NEW : pageCleanerWorker.state();
    }

    /** 后台 redo flusher 状态（诊断用）；未启动返回 NEW。 */
    public RedoFlushWorkerState redoFlushWorkerState() {
        return redoFlushWorker == null ? RedoFlushWorkerState.NEW : redoFlushWorker.state();
    }

    /** 最近一轮后台 flush/checkpoint tick 结果；禁用后台 worker 或尚未执行过 tick 时为空。 */
    public Optional<FlushCycleResult> lastBackgroundFlushCycle() {
        return pageCleanerWorker == null ? Optional.empty() : pageCleanerWorker.lastCycle();
    }

    /**
     * 等待后台 worker 清空显式请求并离开正在执行的 cycle。禁用后台 worker 时直接返回 true；超时语义和
     * {@link PageCleanerWorker#awaitIdle(Duration)} 一致。
     */
    public boolean awaitBackgroundFlushIdle(Duration timeout) {
        if (timeout == null) {
            throw new DatabaseValidationException("background flush await timeout must not be null");
        }
        return pageCleanerWorker == null || pageCleanerWorker.awaitIdle(timeout);
    }

    private void requireOpen() {
        if (state != EngineState.OPEN) {
            throw new EngineStateException("engine not OPEN: " + state);
        }
    }

    private static void closeQuietly(AutoCloseable handle, List<RuntimeException> errors) {
        if (handle == null) {
            return;
        }
        try {
            handle.close();
        } catch (RuntimeException e) {
            errors.add(e);
        } catch (Exception e) {
            errors.add(new DatabaseRuntimeException("close handle failed", e));
        }
    }
}
