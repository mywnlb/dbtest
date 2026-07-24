package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.server.lockobs.api.DefaultLockObservationService;
import cn.zhangyis.db.server.lockobs.api.LockObservationService;
import cn.zhangyis.db.server.lockobs.api.SnapshotRequest;
import cn.zhangyis.db.server.lockobs.snapshot.LockDiagnosticSnapshot;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.trx.PreparedTransactionService;
import cn.zhangyis.db.storage.api.DiskSpaceUndoAllocator;
import cn.zhangyis.db.storage.api.TablePurgeBarrier;
import cn.zhangyis.db.storage.api.autoincrement.AutoIncrementService;
import cn.zhangyis.db.storage.api.IndexRetirementHistoryBarrier;
import cn.zhangyis.db.storage.api.ddl.TableDdlStorageService;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTableGate;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.api.lob.LobStorage;
import cn.zhangyis.db.storage.api.dml.ClusteredDmlService;
import cn.zhangyis.db.storage.api.dml.TableDmlService;
import cn.zhangyis.db.storage.api.tablespace.PageZeroTablespaceMetadataLoader;
import cn.zhangyis.db.storage.api.undotruncate.PurgeDrivenUndoTruncationScheduler;
import cn.zhangyis.db.storage.api.undotruncate.UndoTablespaceTruncationRecovery;
import cn.zhangyis.db.storage.api.undotruncate.UndoTablespaceTruncationService;
import cn.zhangyis.db.storage.api.undotruncate.UndoTruncationFaultInjector;
import cn.zhangyis.db.storage.api.undotruncate.UndoTruncationMetricsSnapshot;
import cn.zhangyis.db.storage.api.undotruncate.UndoReusableSegmentTruncationCoordinator;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadService;
import cn.zhangyis.db.storage.btree.BTreeRootSnapshotService;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferBitmapRepository;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferBootstrap;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferConfig;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferCounters;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferDdlBarrier;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferHeaderRepository;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferHeaderSnapshot;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferMergeWorker;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferMetadataCatalog;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferMetadataResolver;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferPageGate;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferPageMergeInterceptor;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferPageMerger;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferRecoveryValidator;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferStore;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferStateException;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferSnapshot;
import cn.zhangyis.db.storage.changebuffer.SecondaryIndexMutationCoordinator;
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
import cn.zhangyis.db.storage.flush.cleaner.PageCleanerMetricsSnapshot;
import cn.zhangyis.db.storage.flush.cleaner.PageCleanerState;
import cn.zhangyis.db.storage.flush.cleaner.PageCleanerSupervisor;
import cn.zhangyis.db.storage.flush.cleaner.PageCleanerWorker;
import cn.zhangyis.db.storage.buf.ReadAheadService;
import cn.zhangyis.db.storage.buf.BufferPoolWarmupService;
import cn.zhangyis.db.storage.flush.checkpoint.CheckpointCoordinator;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteFileRepository;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteChannel;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteMode;
import cn.zhangyis.db.storage.flush.doublewrite.DetectOnlyDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.doublewrite.NoDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.flush.doublewrite.RecoverableDoublewriteStrategy;
import cn.zhangyis.db.storage.flush.policy.AdaptiveFlushPolicy;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.recovery.CrashRecoveryService;
import cn.zhangyis.db.storage.recovery.RecoveryMode;
import cn.zhangyis.db.storage.recovery.RecoveredTransactionSnapshot;
import cn.zhangyis.db.storage.recovery.RecoveredTransactionReconciliation;
import cn.zhangyis.db.storage.recovery.RecoveredTransactionReconciler;
import cn.zhangyis.db.storage.recovery.RecoveredUndoSlotEvidence;
import cn.zhangyis.db.storage.recovery.RecoveryDiagnosticsSnapshot;
import cn.zhangyis.db.storage.recovery.RecoveryProgressJournal;
import cn.zhangyis.db.storage.recovery.PersistentHistoryRecovery;
import cn.zhangyis.db.storage.recovery.PreparedTransactionDecision;
import cn.zhangyis.db.storage.recovery.PreparedTransactionDecisionProvider;
import cn.zhangyis.db.storage.recovery.RecoveryReport;
import cn.zhangyis.db.storage.recovery.RecoveryRequest;
import cn.zhangyis.db.storage.recovery.RecoverySpaceExclusionPolicy;
import cn.zhangyis.db.storage.recovery.RecoveryState;
import cn.zhangyis.db.storage.recovery.RecoveryTrafficGate;
import cn.zhangyis.db.storage.recovery.TransactionUndoRecoveryResult;
import cn.zhangyis.db.storage.recovery.TransactionUndoRecoveryParticipant;
import cn.zhangyis.db.storage.recovery.TransactionRecoveryCheckpoint;
import cn.zhangyis.db.storage.recovery.TransactionRecoveryCheckpointStore;
import cn.zhangyis.db.storage.recovery.TransactionRecoveryCheckpointSource;
import cn.zhangyis.db.storage.recovery.TransactionRecoveryContext;
import cn.zhangyis.db.storage.recovery.TransactionRecoveryException;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoAppendBudget;
import cn.zhangyis.db.storage.redo.RedoCapacityThrottle;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
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
import cn.zhangyis.db.storage.trx.HistoryTablePurgeBarrier;
import cn.zhangyis.db.storage.trx.HistoryIndexRetirementBarrier;
import cn.zhangyis.db.storage.trx.PurgeCoordinator;
import cn.zhangyis.db.storage.trx.PurgeDriverWorker;
import cn.zhangyis.db.storage.trx.PurgeSummary;
import cn.zhangyis.db.storage.trx.SecondaryPurgeSafetyChecker;
import cn.zhangyis.db.storage.trx.MvccReader;
import cn.zhangyis.db.storage.trx.SecondaryMvccReader;
import cn.zhangyis.db.storage.trx.SecondaryCurrentReadService;
import cn.zhangyis.db.storage.trx.RollbackSegmentSlotManager;
import cn.zhangyis.db.storage.trx.RollbackService;
import cn.zhangyis.db.storage.trx.RollbackSummary;
import cn.zhangyis.db.storage.trx.RecoveredUndoLogIdentity;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.TransactionCounterSnapshot;
import cn.zhangyis.db.storage.trx.TransactionSystem;
import cn.zhangyis.db.storage.trx.UndoTargetMetadataResolver;
import cn.zhangyis.db.storage.trx.UndoLogManager;
import cn.zhangyis.db.storage.trx.UndoLogBinding;
import cn.zhangyis.db.storage.trx.UndoSegmentFinalizer;
import cn.zhangyis.db.storage.trx.UndoSegmentReuseDirectory;
import cn.zhangyis.db.storage.trx.lock.LockManager;
import cn.zhangyis.db.storage.trx.PurgeDmlRowGuardManager;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderSnapshot;
import cn.zhangyis.db.storage.undo.RollbackSegmentHistoryBase;
import cn.zhangyis.db.storage.undo.CachedUndoSegmentRef;
import cn.zhangyis.db.storage.undo.FreeUndoSegmentRef;
import cn.zhangyis.db.storage.undo.RollbackSegmentFreeListBase;
import cn.zhangyis.db.storage.undo.UndoFreeListNodeSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogSegment;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoHistoryNodeSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordIdentity;
import cn.zhangyis.db.storage.undo.UndoRecordType;
import cn.zhangyis.db.storage.undo.UndoSegmentDropPlan;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 存储引擎组合根（engine bootstrap E1/E2/E3a，设计 §3/§13）。把此前仅在测试中构造的存储组件接线成一个生产实例：
 * 共享单一 {@link TablespaceAccessController}，redo 用 durable {@link RedoLogManager}，使 **WAL 顺序在生产
 * flush/checkpoint 路径与 commit 之间成立**（redo durable 必先于 data page 写数据文件）。
 *
 * <p><b>生命周期</b>：{@link #open()}（fresh 建 redo、system.ibd Change Buffer 与系统 undo 表空间；existing
 * 打开配置表空间并运行 {@link CrashRecoveryService}：doublewrite repair、redo replay、安装 redo 边界、
 * CHANGE_BUFFER_RECOVER、UNDO truncate 续作、SPACE_FILE_RECONCILE、UNDO_ROLLBACK、RESUME_PURGE）→
 * {@link #checkpoint()}/{@link #close()} 经
 * {@link FlushService#flushThrough} 按 WAL 顺序持久（先 redo.flush 再刷脏页再持久 checkpoint），close 末关闭
 * AutoCloseable 句柄。
 *
 * <p><b>当前限制</b>：启动后已有后台 redo flusher、单线程 page cleaner；配置 legacy 单聚簇索引或 DD resolver 时会启动
 * {@code PurgeCoordinator} + purge driver。事务 UNDO_ROLLBACK / RESUME_PURGE 已作为正式 recovery stage 接入；
 * DD 表级 resolver 模式支持多索引 rollback、secondary purge、Change Buffer exact-version merge、
 * affected-table barrier 与 prepared transaction 外部决议恢复；DDL 收敛由上层
 * DatabaseEngine 在本存储恢复成功后执行。doublewrite 已常开为 {@link RecoverableDoublewriteStrategy}，但恢复页列表来自 doublewrite 文件有效 slot 并
 * 过滤到 recovery 已打开空间，仍没有全空间 checksum discovery。
 *
 * <p>访问器暴露已接线的事务/disk/btree/undo/mvcc/rollback/DML facade 服务；本类只负责组合根与生命周期，
 * 单聚簇物理 anchor 由 {@link ClusteredDmlService} 编排，表级多索引顺序由 {@link TableDmlService} 编排。
 */
@Slf4j
public final class StorageEngine {

    /**
     * 构造时冻结的 {@code config} 配置快照；已完成范围和组合校验，运行期策略读取它但不得就地修改。
     */
    private final EngineConfig config;
    /** 管理员本次声明与 DD durable 隔离的恢复排除并集；所有恢复 IO 阶段共享同一不可变实例。 */
    private final RecoverySpaceExclusionPolicy recoveryExclusionPolicy;
    /** DatabaseEngine 注入的 DDL/manifest 完成钩子；在 worker 与用户写准入发布前同线程执行。 */
    private final Runnable recoveryCompletionHook;
    /** 写 MTR 的唯一实例级准入；构造即为 RECOVERY_INTERNAL，完成钩子后单向封存。 */
    private final StorageWriteAdmission writeAdmission = StorageWriteAdmission.recoveryInternal();
    /**
     * 后台存储 fatal 向公共组合根发布的可替换回调；只能在 NEW 期配置，worker 调用时不得持页、MTR 或生命周期锁。
     */
    private Consumer<DatabaseFatalException> backgroundFatalFailureHandler = ignored -> { };
    /** 恢复 PREPARED 的外部权威决议源；默认 unresolved，绝不由存储层猜测全局事务结果。 */
    private final PreparedTransactionDecisionProvider preparedDecisionProvider;
    /**
     * 本对象的权威状态机字段 {@code state}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
     */
    private EngineState state = EngineState.NEW;

    // AutoCloseable 句柄（close 时关闭）
    /**
     * 本对象持有的 {@code store} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private PageStore store;
    /**
     * 本对象持有的 {@code redoRepo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private RedoLogFileRepository redoRepo;
    /**
     * 本对象持有的 {@code checkpointStore} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private RedoCheckpointStore checkpointStore;
    /**
     * fuzzy checkpoint 的事务 id/no 高水位 sidecar；NORMAL/FORCE/fresh 打开，READ_ONLY_VALIDATE 不创建或写入。
     */
    private TransactionRecoveryCheckpointStore transactionRecoveryCheckpointStore;
    /** FlushList/LRU 双物理 doublewrite 通道；OFF 模式为空。 */
    private DoublewriteChannel doublewriteChannel;
    /** 旧版单文件副本，仅作为恢复输入兼容，不再作为前向写入目标。 */
    private DoublewriteFileRepository legacyDoublewriteRepo;
    /**
     * 本对象持有的 {@code pool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private BufferPool pool;
    /** E2 起由 engine 显式持有的运行时表空间 registry，确保 disk facade 与 undo recovery 共享同一状态视图。 */
    private TablespaceRegistry registry;
    /** 每个表空间的 operation lease 控制器；同一实例注入 MTR、page0 loader、flush 和 undo truncate recovery。 */
    private TablespaceAccessController accessController;
    /** 启动恢复流量门控；fresh open 直接打开，existing open 由 {@link CrashRecoveryService} 驱动状态转换。 */
    private final RecoveryTrafficGate recoveryGate;
    /** 本进程内 recovery 阶段进度 journal；fresh open 为空，existing open 由 CrashRecoveryService 写入。 */
    private final RecoveryProgressJournal recoveryProgressJournal;

    // 接线的服务
    /**
     * 本对象持有的 {@code redo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private RedoLogManager redo;
    /**
     * 本对象持有的 {@code miniTransactionManager} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private MiniTransactionManager miniTransactionManager;
    /** 页 0 high-water 的唯一运行时分配服务。 */
    private AutoIncrementService autoIncrementService;
    /**
     * 本对象持有的 {@code diskSpaceManager} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private DiskSpaceManager diskSpaceManager;
    /** 0.21h off-page TEXT/BLOB/JSON 门面；与 B+Tree/Undo 共享同一 type registry、pool 与 FSP facade。 */
    private LobStorage lobStorage;
    /**
     * 本对象持有的 {@code transactionManager} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private TransactionManager transactionManager;
    /**
     * 本对象持有的 {@code undoLogManager} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private UndoLogManager undoLogManager;
    /**
     * 本对象持有的 {@code btreeService} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private SplitCapableBTreeIndexService btreeService;
    /** 0.17 事务锁内核；聚簇 current-read、secondary logical-prefix S/X 与事务终态共享这一实例。 */
    private LockManager lockManager;
    /** server.lockobs row-lock 观测服务；只读消费 LockManager 事实，不参与授锁或 rollback。 */
    private LockObservationService lockObservationService;
    /** B+Tree current-read 点查/unique-check 协调器；授予的锁由 DML/上层事务结束入口释放。 */
    private BTreeCurrentReadService btreeCurrentReadService;
    /** 聚簇 DML 内核兼容 facade；表级服务复用其 undo anchor、事务终态、LOB 与锁释放。 */
    private ClusteredDmlService dmlService;
    /** 表级多索引 DML facade；与聚簇 facade 共享事务、锁、redo 和 undo 组合根。 */
    private TableDmlService tableDmlService;
    /** 结构写前读取 root 页头 level 的共享服务；DML、rollback 与 purge 不得各自相信过期 DD level。 */
    private BTreeRootSnapshotService btreeRootSnapshots;
    /** 表级 DML、rollback 与 purge 共用的短物理行协调器，保证同一主键的跨树变更不会互相穿插。 */
    private PurgeDmlRowGuardManager purgeDmlRowGuards;
    /** 组合根共享的类型/排序 registry，保证 DD、record 与 secondary comparator 使用同一语义。 */
    private TypeCodecRegistry typeRegistry;
    /** 组合根唯一 Online DDL table gate；DML、XA 和 DDL coordinator 不得各自创建旁路实例。 */
    private OnlineDdlTableGate onlineDdlTableGate;
    /**
     * 本对象持有的 {@code indexPageAccess} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private IndexPageAccess indexPageAccess;
    /** system.ibd 存在时的 page3 header 仓储；legacy existing 实例为空且 effective mode=NONE。 */
    private ChangeBufferHeaderRepository changeBufferHeaders;
    /** 用户空间 4-bit/page bitmap 仓储；只在 system.ibd 格式可用时接线。 */
    private ChangeBufferBitmapRepository changeBufferBitmaps;
    /** 全局 IBUF_INDEX B+Tree store。 */
    private ChangeBufferStore changeBufferStore;
    /** 前台 append、发布前 merge 与 DDL discard 共用的 per-target gate。 */
    private ChangeBufferPageGate changeBufferPageGate;
    /** exact-version 进程目录与持久 DD resolver 组合。 */
    private ChangeBufferMetadataCatalog changeBufferMetadataCatalog;
    /** DML/rollback/purge 统一 buffer-or-direct 决策点。 */
    private SecondaryIndexMutationCoordinator secondaryIndexMutations;
    /** DROP INDEX/TABLE 回收物理资源前的全局 mutation 屏障。 */
    private ChangeBufferDdlBarrier changeBufferDdlBarrier;
    /** LOADING frame 发布前唯一 merge 拦截器。 */
    private ChangeBufferPageMergeInterceptor changeBufferLoadInterceptor;
    /** redo 后 header/tree 一致性恢复参与者。 */
    private ChangeBufferRecoveryValidator changeBufferRecoveryValidator;
    /** 普通可写模式下的低优先级主动 merge worker。 */
    private ChangeBufferMergeWorker changeBufferMergeWorker;
    /** 本次 open 是否发现/创建了 system.ibd Change Buffer 格式；legacy existing 保持 false。 */
    private boolean changeBufferAvailable;
    /** append、merge、DDL 与 fallback 共用的进程级统计 owner；不参与任何持久裁决。 */
    private final ChangeBufferCounters changeBufferCounters = new ChangeBufferCounters();
    /** 结合 system.ibd 与持久 exact-version resolver 后的运行期策略；可能从 configured mode 降级为 NONE。 */
    private ChangeBufferConfig effectiveChangeBufferConfig;
    /** resolver 能否在重启后按 table/schema/index 三元组重建二级布局。 */
    private boolean persistentChangeBufferMetadataAvailable;
    /** DD/DDL 上层唯一可调用的物理 CREATE/DROP TABLE facade。 */
    private TableDdlStorageService tableDdlStorageService;
    /**
     * 本对象持有的 {@code mvccReader} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private MvccReader mvccReader;
    /** unique point/non-unique logical-prefix 候选回表并复核聚簇可见版本的 MVCC 读服务。 */
    private SecondaryMvccReader secondaryMvccReader;
    /** non-unique logical-prefix 锁定读；predicate 与 clustered 锁保持到事务终态。 */
    private SecondaryCurrentReadService secondaryCurrentReadService;
    /**
     * 本对象持有的 {@code rollbackService} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private RollbackService rollbackService;
    /** phase-one/phase-two 强持久边界与事务锁收尾的稳定 storage resource-manager facade。 */
    private PreparedTransactionService preparedTransactionService;
    /** 内存 rseg slot 目录；0.3 起 claim/release 持久化到 page3，恢复期扫描重建。 */
    private RollbackSegmentSlotManager rollbackSlots;
    /** page3 cache/free owner 的统一运行期投影。 */
    private UndoSegmentReuseDirectory undoSegmentReuse;
    /** 持久 rseg header 仓储（page3）；fresh 格式化、claim/release 持久、恢复扫描共用。 */
    private RollbackSegmentHeaderRepository rsegHeaderRepo;
    /** undo 段物理设施；恢复期读 undo 段 state、回滚 ACTIVE 段用。 */
    private UndoLogSegmentAccess undoAccess;
    /** commit、live/recovery rollback 与 purge 共享的单/双段原子终结协调器。 */
    private UndoSegmentFinalizer undoSegmentFinalizer;
    /**
     * 兼容模式的单显式聚簇索引：在不使用公共 {@code DatabaseEngine}/DD 组合根的低层测试中，
     * 同时服务恢复期回滚（R 1.2）与后台 purge（0.4）。DD 模式优先使用
     * {@link #indexMetadataResolver}逐条定位索引；两者都为 null 时不启动 purge，existing-open 发现 ACTIVE undo 则
     * fail-closed，不能跳过 rollback 后开放流量。
     */
    private BTreeIndex clusteredIndex;
    /** DD 模式按 undo tableId/indexId 解析索引；在 open 前注入，优先于 legacy 单索引。 */
    private IndexMetadataResolver indexMetadataResolver;
    /** 事务系统（id/no 分配 + active 表 + purge low water）；purge boundary 来源。 */
    private TransactionSystem txnSystem;
    /** 已提交 undo 的 history list；undoLogManager 写入、purge 读出，须同一实例。 */
    private HistoryList history;
    /** DD DROP 只依赖的稳定表级 barrier；计数权威态仍由 history 在同一锁内维护。 */
    private TablePurgeBarrier tablePurgeBarrier;
    /** Online DROP使用的提交号高水位与有限history屏障；与transaction system/history共享唯一owner。 */
    private IndexRetirementHistoryBarrier indexRetirementHistoryBarrier;
    /** undo 段分配/回收端口；purge dropUndoSegment 用。 */
    private DiskSpaceUndoAllocator undoAllocator;
    /** E3a 后台 purge driver（0.4）；配置聚簇索引时启动，周期驱动 PurgeCoordinator.runBatch。 */
    private PurgeDriverWorker purgeDriverWorker;
    /** 前台测试、后台 driver 与 recovery RESUME_PURGE 共用的单线程协调器；无索引 metadata 时为空。 */
    private PurgeCoordinator purgeCoordinator;
    /** recovery 与 live 自动截断共享的 cache/free drain 协调器，避免各自维护不同的 reuse owner 视图。 */
    private UndoReusableSegmentTruncationCoordinator undoReusableSegmentTruncationCoordinator;
    /** recovery 续作与 purge cycle 尝试共享的 crash-safe truncate service。 */
    private UndoTablespaceTruncationService undoTablespaceTruncationService;
    /** 复用 purge driver 线程的自动截断调度器；只保存内存 cooldown 与原子观测快照。 */
    private PurgeDrivenUndoTruncationScheduler undoTruncationScheduler;
    /**
     * 本对象持有的 {@code flushService} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private FlushService flushService;
    /** E3a 后台 page cleaner；只由 engine 生命周期启动/停止，所有刷脏仍走 {@link FlushService}。 */
    private PageCleanerSupervisor pageCleanerSupervisor;
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

    /**
     * 创建 {@code StorageEngine}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param config 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    public StorageEngine(EngineConfig config) {
        this(config, PreparedTransactionDecisionProvider.unresolved());
    }

    /**
     * 构造带 prepared recovery 决议源的存储引擎。provider 只在 existing-open 且发现 PREPARED owner 时调用。
     *
     * @param config 引擎物理与恢复配置
     * @param preparedDecisionProvider 上层协调器持久决议查询端口；不能为 null
     */
    public StorageEngine(EngineConfig config,
                         PreparedTransactionDecisionProvider preparedDecisionProvider) {
        this(config, new RecoveryTrafficGate(), defaultRecoveryProgressJournal(config),
                preparedDecisionProvider, configuredExclusionPolicy(config), () -> { });
    }

    /**
     * 构造携带对象级 DD 隔离证据的存储引擎；公共 DatabaseEngine 在用户表空间 discovery 前使用该入口。
     *
     * @param config 引擎物理与恢复配置
     * @param preparedDecisionProvider PREPARED 外部决议源
     * @param exclusionPolicy 已由 DD 规划器证明的管理员/DD 排除集合
     */
    public StorageEngine(EngineConfig config,
                         PreparedTransactionDecisionProvider preparedDecisionProvider,
                         RecoverySpaceExclusionPolicy exclusionPolicy) {
        this(config, new RecoveryTrafficGate(), defaultRecoveryProgressJournal(config),
                preparedDecisionProvider, exclusionPolicy, () -> { });
    }

    /**
     * 构造带 DatabaseEngine 恢复完成钩子的存储引擎。钩子只能执行 DDL recovery/clean publish，
     * 返回前用户入口尚未发布且后台 worker 尚未启动。
     *
     * @param config 引擎配置
     * @param preparedDecisionProvider PREPARED 决议源
     * @param exclusionPolicy 对象级排除证据
     * @param recoveryCompletionHook storage crash recovery 后、服务发布前的同线程完成动作
     */
    public StorageEngine(EngineConfig config,
                         PreparedTransactionDecisionProvider preparedDecisionProvider,
                         RecoverySpaceExclusionPolicy exclusionPolicy,
                         Runnable recoveryCompletionHook) {
        this(config, new RecoveryTrafficGate(), defaultRecoveryProgressJournal(config),
                preparedDecisionProvider, exclusionPolicy, recoveryCompletionHook);
    }

    /**
     * 创建 {@code StorageEngine}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param config 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param recoveryGate 由组合根提供的 {@code RecoveryTrafficGate} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param recoveryProgressJournal 由组合根提供的 {@code RecoveryProgressJournal} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    StorageEngine(EngineConfig config, RecoveryTrafficGate recoveryGate, RecoveryProgressJournal recoveryProgressJournal) {
        this(config, recoveryGate, recoveryProgressJournal,
                PreparedTransactionDecisionProvider.unresolved(), configuredExclusionPolicy(config), () -> { });
    }

    /**
     * 创建 {@code StorageEngine}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param config 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param recoveryGate 由组合根提供的 {@code RecoveryTrafficGate} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param recoveryProgressJournal 由组合根提供的 {@code RecoveryProgressJournal} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param preparedDecisionProvider 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    StorageEngine(EngineConfig config, RecoveryTrafficGate recoveryGate,
                  RecoveryProgressJournal recoveryProgressJournal,
                  PreparedTransactionDecisionProvider preparedDecisionProvider) {
        this(config, recoveryGate, recoveryProgressJournal, preparedDecisionProvider,
                configuredExclusionPolicy(config), () -> { });
    }

    /** 统一校验并冻结启动恢复依赖；package 构造器仍默认只携带旧配置中的管理员集合。 */
    private StorageEngine(EngineConfig config, RecoveryTrafficGate recoveryGate,
                          RecoveryProgressJournal recoveryProgressJournal,
                          PreparedTransactionDecisionProvider preparedDecisionProvider,
                          RecoverySpaceExclusionPolicy exclusionPolicy,
                          Runnable recoveryCompletionHook) {
        if (config == null) {
            throw new DatabaseValidationException("engine config must not be null");
        }
        if (recoveryGate == null || recoveryProgressJournal == null || preparedDecisionProvider == null
                || exclusionPolicy == null || recoveryCompletionHook == null) {
            throw new DatabaseValidationException(
                    "engine recovery gate/journal/prepared decision provider/exclusion policy must not be null");
        }
        this.config = config;
        this.recoveryExclusionPolicy = exclusionPolicy;
        this.recoveryCompletionHook = recoveryCompletionHook;
        this.recoveryGate = recoveryGate;
        this.recoveryProgressJournal = recoveryProgressJournal;
        this.preparedDecisionProvider = preparedDecisionProvider;
    }

    /** 旧低层构造只把 config 的 force 集合解释为管理员证据；DD 证据只能由上层显式注入。 */
    private static RecoverySpaceExclusionPolicy configuredExclusionPolicy(EngineConfig config) {
        if (config == null) {
            throw new DatabaseValidationException("engine config must not be null");
        }
        return RecoverySpaceExclusionPolicy.of(config.forceSkippedSpaces(), Set.of());
    }

    private static RecoveryProgressJournal defaultRecoveryProgressJournal(EngineConfig config) {
        if (config == null) {
            throw new DatabaseValidationException("engine config must not be null");
        }
        return RecoveryProgressJournal.persistent(config.recoveryProgressFile());
    }

    /**
     * 打开引擎。fresh（baseDir 无 redo.log）在 boot MTR 创建 redo、SpaceId 0 {@code system.ibd}、Change Buffer
     * page3/page4 与系统 undo；existing 先打开 system/undo/配置数据空间，再由 {@link CrashRecoveryService} 执行
     * doublewrite、checkpoint-aware redo、恢复边界、CHANGE_BUFFER_RECOVER、undo/事务/purge 与文件 reconcile。
     * 正常恢复完成后安装发布前 merge 拦截器，并依次启动 redo flusher、page cleaner、purge、Change Buffer worker、
     * read-ahead，最后才发布 OPEN；只读校验模式不安装写拦截器或后台写线程。
     *
     * @throws EngineStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void open() {
        if (state != EngineState.NEW) {
            throw new EngineStateException("open requires NEW state: " + state);
        }
        try {
            openInternal();
        } catch (RuntimeException failure) {
            cleanupAfterFailedOpen(failure);
            throw failure;
        }
    }

    /** 执行实际 bootstrap/recovery；任何异常由 {@link #open()} 统一停止 worker 并关闭部分初始化句柄。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
    private void openInternal() {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        try {
            Files.createDirectories(config.baseDir());
        } catch (IOException e) {
            throw new DatabaseRuntimeException("create engine baseDir failed: " + config.baseDir(), e);
        }
        // redo 后端：默认文件环，也可显式回退单 append-only 文件。文件环 fresh 判定只看稳定命名的 ring 文件；
        // 无关诊断文件不算 existing，而任意 partial ring 都必须进入完整性校验、不能被当作 fresh 覆盖。
        // READ_ONLY_VALIDATE 使用只读 data/control channel，启动扫描不能创建、预分配、截断或修复 redo 输入。
        RedoReclaimBoundary redoReclaim;
        boolean fresh;
        boolean readOnlyValidate = config.recoveryMode() == RecoveryMode.READ_ONLY_VALIDATE;
        if (config.redoRotationEnabled()) {
            fresh = !RotatingRedoLogRepository.hasAnyRingFiles(config.redoDir());
            rejectFreshRecoveryMode(fresh);
            validateRecoverySkipConfiguration();
            RotatingRedoLogRepository ring = readOnlyValidate
                    ? RedoLogFileRepository.openRingReadOnly(
                            config.redoDir(), config.redoRotation().fileCount(), config.redoRotation().fileBytes())
                    : RedoLogFileRepository.openRing(
                            config.redoDir(), config.redoRotation().fileCount(), config.redoRotation().fileBytes());
            this.redoRepo = ring;
            redoReclaim = ring;
        } else {
            fresh = !Files.exists(config.redoFile());
            rejectFreshRecoveryMode(fresh);
            validateRecoverySkipConfiguration();
            this.redoRepo = readOnlyValidate
                    ? RedoLogFileRepository.openReadOnly(config.redoFile())
                    : RedoLogFileRepository.open(config.redoFile());
            redoReclaim = null;
        }
        // existing 实例绝不隐式创建 system.ibd；只有 fresh bootstrap 或真实文件存在时才接线持久格式。
        this.changeBufferAvailable = fresh || Files.exists(config.systemTablespaceFile());
        this.redo = RedoLogManager.durable(redoRepo);
        this.checkpointStore = readOnlyValidate
                ? RedoCheckpointStore.openReadOnly(config.redoControlFile())
                : RedoCheckpointStore.open(config.redoControlFile());
        if (readOnlyValidate) {
            if (Files.exists(config.transactionRecoveryCheckpointFile())) {
                this.transactionRecoveryCheckpointStore = TransactionRecoveryCheckpointStore.openReadOnly(
                        config.transactionRecoveryCheckpointFile());
            }
        } else {
            this.transactionRecoveryCheckpointStore = TransactionRecoveryCheckpointStore.open(
                    config.transactionRecoveryCheckpointFile());
        }
        // 双物理文件早于 FlushCoordinator/recovery 打开；OFF 不创建文件，旧单文件只读作恢复兼容输入。
        if (config.doublewriteMode() != DoublewriteMode.OFF) {
            this.doublewriteChannel = readOnlyValidate
                    ? DoublewriteChannel.openReadOnly(config.flushListDoublewriteFile(),
                            config.lruDoublewriteFile(), config.pageSize())
                    : DoublewriteChannel.open(config.flushListDoublewriteFile(),
                            config.lruDoublewriteFile(), config.pageSize());
            if (Files.exists(config.doublewriteFile())) {
                this.legacyDoublewriteRepo = readOnlyValidate
                        ? DoublewriteFileRepository.openReadOnlyIfExists(config.doublewriteFile(), config.pageSize())
                                .orElse(null)
                        : DoublewriteFileRepository.open(config.doublewriteFile(), config.pageSize());
            }
        }
        this.accessController = new TablespaceAccessController();
        this.store = new FileChannelPageStore();
        // 0.10d：按 config 分片数构造 buffer pool（默认 1=单实例池，生产保守；测试经 withBufferPoolInstanceCount 配 N>1）。
        LruBufferPool lruPool = new LruBufferPool(store, config.pageSize(), config.bufferPoolCapacityFrames(),
                config.bufferPoolInstanceCount());
        this.pool = lruPool;
        this.registry = new CachingTablespaceRegistry(
                new PageZeroTablespaceMetadataLoader(store, config.pageSize(), accessController));
        this.diskSpaceManager = new DiskSpaceManager(pool, store, config.pageSize(), registry);
        // TransactionSystem 必须早于 CheckpointCoordinator：checkpoint participant 在 redo label 前短锁读取其 next-counter。
        this.txnSystem = new TransactionSystem();

        var doublewriteStrategy = switch (config.doublewriteMode()) {
            case OFF -> new NoDoublewriteStrategy();
            case DETECT_ONLY -> new DetectOnlyDoublewriteStrategy(doublewriteChannel);
            case DETECT_AND_RECOVER -> new RecoverableDoublewriteStrategy(doublewriteChannel);
        };
        FlushCoordinator flushCoordinator = new FlushCoordinator(pool, store, redo, config.pageSize(),
                doublewriteStrategy, config.flushTimeout(), accessController);
        CheckpointCoordinator checkpointCoordinator =
                transactionRecoveryCheckpointStore == null
                        ? new CheckpointCoordinator(pool, redo, checkpointStore, redoReclaim)
                        : new CheckpointCoordinator(pool, redo, checkpointStore,
                                this::persistTransactionRecoveryCheckpoint, redoReclaim);
        this.flushService = new FlushService(pool, flushCoordinator, checkpointCoordinator, redo,
                RedoCapacityPolicy.fixed(config.redoCapacityBytes()),
                AdaptiveFlushPolicy.adaptive(1, 1, config.bufferPoolCapacityFrames()));
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        RedoCapacityThrottle redoCapacityThrottle = new RedoCapacityThrottle(
                RedoCapacityPolicy.fixed(config.redoCapacityBytes()),
                redo::currentLsn,
                checkpointCoordinator::lastCheckpointLsn,
                () -> requestBackgroundFlush(config.backgroundFlushMaxPages()),
                () -> {
                    // 前台容量反压在进入 redo append 之前执行：先推进 redo durable 边界，再让 FlushService
                    // 通过 WAL gate 刷脏并持久 checkpoint。这里不持 page latch/frame lock/FSP lease。
                    // 不能复用 backgroundFlushMaxPages：该值允许为 0，表示后台 tick 不主动刷脏；前台
                    // SYNC/HARD 等待必须有自己的刷页预算，否则 dirty page 会永久挡住 checkpoint。
                    redo.flush();
                    flushService.flushForCapacity(foregroundCapacityFlushMaxPages());
                },
                config.flushTimeout(),
                config.redoRotationEnabled() ? config.redoRotation().fileBytes() : Long.MAX_VALUE);
        this.miniTransactionManager = new MiniTransactionManager(
                accessController, redo, redoCapacityThrottle, config.pageSize(), writeAdmission);
        this.autoIncrementService = new AutoIncrementService(
                miniTransactionManager, pool, flushService);

        this.typeRegistry = new TypeCodecRegistry();
        this.onlineDdlTableGate = new OnlineDdlTableGate();
        this.lobStorage = new LobStorage(diskSpaceManager, pool, config.pageSize(), typeRegistry);
        this.transactionManager = new TransactionManager(txnSystem);
        this.rollbackSlots = new RollbackSegmentSlotManager(RollbackSegmentId.of(0), config.slotCapacity());
        this.undoSegmentReuse = new UndoSegmentReuseDirectory(config.undoCachedSegmentsPerKind());
        this.rsegHeaderRepo = new RollbackSegmentHeaderRepository(pool, config.pageSize());
        this.history = new HistoryList(config.undoHistoryTransitionTimeout());
        this.tablePurgeBarrier = new HistoryTablePurgeBarrier(history);
        this.indexRetirementHistoryBarrier = new HistoryIndexRetirementBarrier(txnSystem, history);
        this.undoAllocator = new DiskSpaceUndoAllocator(diskSpaceManager);
        this.undoAccess = new UndoLogSegmentAccess(pool, config.pageSize(), undoAllocator, typeRegistry, registry,
                config.maxExternalUndoPayloadPages());
        this.undoSegmentFinalizer = new UndoSegmentFinalizer(miniTransactionManager, undoAccess, undoAllocator,
                rsegHeaderRepo, rollbackSlots, undoSegmentReuse);
        this.undoReusableSegmentTruncationCoordinator = new UndoReusableSegmentTruncationCoordinator(
                miniTransactionManager, undoAccess, undoAllocator, rsegHeaderRepo,
                rollbackSlots.rollbackSegmentId(), config.slotCapacity(),
                config.undoCachedSegmentsPerKind(), undoSegmentReuse);
        this.undoTablespaceTruncationService = new UndoTablespaceTruncationService(
                pool, store, config.pageSize(), registry, accessController, miniTransactionManager,
                flushService, config.flushTimeout(), UndoTruncationFaultInjector.none(),
                undoReusableSegmentTruncationCoordinator);
        this.undoTruncationScheduler = new PurgeDrivenUndoTruncationScheduler(
                config.undoTruncationConfig(), config.undoSpaceId(), undoTablespaceTruncationService);
        this.undoLogManager = new UndoLogManager(undoAccess, rollbackSlots, config.undoSpaceId(), history,
                rsegHeaderRepo, undoSegmentFinalizer, undoSegmentReuse);
        this.indexPageAccess = new IndexPageAccess(pool, config.pageSize(), registry);
        this.btreeService = new SplitCapableBTreeIndexService(indexPageAccess, diskSpaceManager, typeRegistry);
        this.btreeRootSnapshots = new BTreeRootSnapshotService(indexPageAccess);
        this.purgeDmlRowGuards = new PurgeDmlRowGuardManager();
        initializeChangeBufferComponents();
        this.tableDdlStorageService = new TableDdlStorageService(miniTransactionManager, diskSpaceManager,
                indexPageAccess, pool, store, flushService, accessController, config.pageSize(),
                btreeService, btreeRootSnapshots, lobStorage, writeAdmission, changeBufferDdlBarrier);
        this.lockObservationService = new DefaultLockObservationService();
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        this.lockManager = new LockManager(lockObservationService);
        this.btreeCurrentReadService =
                new BTreeCurrentReadService(miniTransactionManager, btreeService, lockManager);
        this.mvccReader = new MvccReader(miniTransactionManager, btreeService, undoAccess,
                config.undoSpaceId(), config.maxVersionHops());
        this.secondaryMvccReader = new SecondaryMvccReader(
                miniTransactionManager, btreeService, mvccReader, typeRegistry);
        this.secondaryCurrentReadService = new SecondaryCurrentReadService(
                transactionManager, miniTransactionManager, btreeService,
                btreeCurrentReadService, lockManager, typeRegistry);
        this.rollbackService = indexMetadataResolver instanceof UndoTargetMetadataResolver targetResolver
                ? new RollbackService(btreeService, undoAccess, transactionManager,
                        miniTransactionManager, undoSegmentFinalizer, lobStorage, targetResolver,
                        btreeRootSnapshots, purgeDmlRowGuards, secondaryIndexMutations)
                : indexMetadataResolver == null
                        ? new RollbackService(btreeService, undoAccess, transactionManager,
                                miniTransactionManager, undoSegmentFinalizer)
                        : new RollbackService(btreeService, undoAccess, transactionManager,
                                miniTransactionManager, undoSegmentFinalizer, indexMetadataResolver);
        this.preparedTransactionService = new PreparedTransactionService(
                transactionManager, undoLogManager, rollbackService, redo, recoveryGate, lockManager,
                onlineDdlTableGate);
        this.dmlService = new ClusteredDmlService(transactionManager, undoLogManager, miniTransactionManager,
                btreeService, btreeCurrentReadService, rollbackService, lockManager, redo, recoveryGate,
                lobStorage, onlineDdlTableGate);
        this.tableDmlService = new TableDmlService(dmlService, transactionManager, miniTransactionManager,
                btreeService, btreeCurrentReadService, lockManager,
                btreeRootSnapshots, typeRegistry, purgeDmlRowGuards, redo, secondaryIndexMutations);
        this.purgeCoordinator = indexMetadataResolver instanceof UndoTargetMetadataResolver targetResolver
                ? new PurgeCoordinator(miniTransactionManager, txnSystem, history, undoAccess,
                        undoSegmentFinalizer, btreeService, targetResolver, btreeRootSnapshots,
                        purgeDmlRowGuards, new SecondaryPurgeSafetyChecker(miniTransactionManager,
                                btreeService, undoAccess, config.undoSpaceId(),
                                config.maxVersionHops(), typeRegistry), lobStorage, config.purgeConfig(),
                        secondaryIndexMutations)
                : indexMetadataResolver != null
                        ? new PurgeCoordinator(miniTransactionManager, txnSystem, history, undoAccess,
                                undoSegmentFinalizer, btreeService, indexMetadataResolver)
                        : clusteredIndex != null
                                ? new PurgeCoordinator(miniTransactionManager, txnSystem, history, undoAccess,
                                        undoSegmentFinalizer, btreeService, clusteredIndex)
                                : null;

        // WAL 安全淘汰：注入淘汰刷盘端口，使脏页淘汰复用 FlushCoordinator 的 WAL gate + checksum + doublewrite
        // 管线。必须在 FlushCoordinator 就绪后、任何可能触发淘汰的 page access（fresh 建系统 undo / recover）之前注入。
        lruPool.attachVictimFlusher(new CoordinatedDirtyVictimFlusher(flushCoordinator));
        this.crashRecoveryService = new CrashRecoveryService(recoveryGate, recoveryProgressJournal);

        if (fresh) {
            // 先建 SpaceId 0 system.ibd 及全局树，再建系统 undo；同一 MTR 的物理页顺序单调且统一受 redo 保护。
            MiniTransaction boot = miniTransactionManager.begin(
                    miniTransactionManager.budgetFor(RedoBudgetPurpose.ENGINE_BOOT));
            diskSpaceManager.createTablespace(boot, ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID,
                    config.systemTablespaceFile(), config.systemTablespaceInitialPages(), TablespaceType.SYSTEM);
            new ChangeBufferBootstrap(diskSpaceManager, indexPageAccess, changeBufferHeaders, config.pageSize())
                    .initialize(boot, config.changeBufferConfig().mode());
            diskSpaceManager.createTablespace(boot, config.undoSpaceId(), config.undoFile(),
                    config.undoSpaceInitialPages(), TablespaceType.UNDO);
            // 0.3：同 boot MTR 格式化 page3 rseg header（空 slot 目录，redo 保护），供 claim/release 持久与恢复扫描。
            rsegHeaderRepo.format(boot, config.undoSpaceId(), rollbackSlots.rollbackSegmentId(),
                    config.slotCapacity(), config.undoCachedSegmentsPerKind());
            miniTransactionManager.commit(boot);
            log.info("initialized system Change Buffer: file={} mode={}",
                    config.systemTablespaceFile(), config.changeBufferConfig().mode());
            attachChangeBufferLoadInterceptor();
            lastRecoveryReport = null;
        } else {
            recoverExisting();
        }
        if (recoveryGate.state() == RecoveryState.READ_ONLY) {
            // READ_ONLY_VALIDATE 是灾难诊断启动：前面已经打开文件句柄并扫描 recovery 输入，但没有应用 redo、
            // 没有修复 doublewrite、也没有安装可续写 redo 边界。此处不能启动后台 worker 或 warmup，因为它们会
            // 触发普通读写生命周期；发布 EngineState.READ_ONLY 后只允许读取恢复报告/gate 状态再 close 释放句柄。
            this.state = EngineState.READ_ONLY;
            return;
        }
        // DatabaseEngine 的 DDL/SDI/orphan 收敛必须仍处于 recovery-internal 写窗口；完成后强制刷清副作用。
        recoveryCompletionHook.run();
        flushService.flushThrough(redo.currentLsn(), config.flushTimeout());
        if (config.recoveryMode() == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE) {
            // FORCE 只用于导出健康对象：不启动任何会写文件或主动触页的后台 worker/warmup。
            writeAdmission.seal(StorageWriteAdmission.Mode.EXPORT_READ_ONLY);
            this.state = EngineState.OPEN;
            return;
        }
        writeAdmission.seal(StorageWriteAdmission.Mode.NORMAL);
        // redo flusher 早于 page cleaner 启动：先让 durable LSN 能自动前进，page cleaner 的 WAL gate 才不空等。
        startBackgroundRedoFlusher();
        startBackgroundPageCleaner();
        startBackgroundPurgeDriver();
        startBackgroundChangeBufferMerge();
        // read-ahead 在 bootstrap/recover 之后启动并接钩子：建库/恢复的 page access 不触发预取；之后普通 getPage 顺序访问才驱动。
        startBackgroundReadAhead(lruPool);
        // warmup load：上次 close 的热页定位预取回池（缺失/损坏 dump no-op；未打开空间的页由 prefetch 跳过）。最佳努力，不阻断 open。
        new BufferPoolWarmupService().load(pool, config.bufferPoolDumpFile());
        if (fresh) {
            recoveryGate.openForUserTraffic();
        }
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        this.state = EngineState.OPEN;
    }

    /**
     * 构造 Change Buffer 的单一生产协作图。此阶段只绑定对象，不读取 page 3，也不安装 Buffer Pool 拦截器；
     * fresh bootstrap 尚未创建系统页，existing recovery 也尚未完成 redo replay。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>根据 system.ibd 可用性与 resolver 能力计算 effective mode；缺持久 resolver 时禁止产生跨重启不可解析的新记录。</li>
     *     <li>创建共享 header、bitmap、global store、metadata catalog 与 per-page gate，所有协作者绑定同一 Buffer Pool/MTR。</li>
     *     <li>构造发布前 merger/interceptor、统一 secondary mutation coordinator 与 DDL barrier。</li>
     *     <li>构造恢复校验器和后台 worker；实际 attach/start 延后到 bootstrap 或 recovery 安全边界。</li>
     * </ol>
     */
    private void initializeChangeBufferComponents() {
        // 1、legacy existing 没有 system.ibd 时完全不构造持久协作者；上层继续使用原来的二级树直写路径。
        persistentChangeBufferMetadataAvailable = indexMetadataResolver instanceof ChangeBufferMetadataResolver;
        effectiveChangeBufferConfig = changeBufferAvailable && persistentChangeBufferMetadataAvailable
                ? config.changeBufferConfig()
                : withChangeBufferModeDisabled(config.changeBufferConfig());
        if (config.changeBufferConfig().mode()
                != cn.zhangyis.db.storage.changebuffer.ChangeBufferMode.NONE
                && effectiveChangeBufferConfig.mode()
                == cn.zhangyis.db.storage.changebuffer.ChangeBufferMode.NONE) {
            log.warn("Change Buffer mode downgraded to NONE: systemTablespaceAvailable={} "
                            + "persistentMetadataResolverAvailable={}",
                    changeBufferAvailable, persistentChangeBufferMetadataAvailable);
        }
        if (!changeBufferAvailable) {
            return;
        }

        // 2、所有持久仓储共享本引擎的页大小、Buffer Pool、B+Tree 与 redo/MTR 域，不创建旁路页缓存。
        ChangeBufferMetadataResolver persistentResolver = persistentChangeBufferMetadataAvailable
                ? (ChangeBufferMetadataResolver) indexMetadataResolver : null;
        this.changeBufferHeaders = new ChangeBufferHeaderRepository(pool, config.pageSize());
        this.changeBufferBitmaps = new ChangeBufferBitmapRepository(pool, config.pageSize());
        this.changeBufferStore = new ChangeBufferStore(changeBufferHeaders, btreeService, config.pageSize());
        this.changeBufferPageGate = new ChangeBufferPageGate(1024);
        this.changeBufferMetadataCatalog = persistentResolver == null
                ? new ChangeBufferMetadataCatalog()
                : new ChangeBufferMetadataCatalog(persistentResolver);

        // 3、append、demand merge 与 DROP discard 共用 gate/store/catalog，避免同一 target 存在两套串行化所有权。
        ChangeBufferPageMerger merger = new ChangeBufferPageMerger(
                changeBufferMetadataCatalog, typeRegistry, config.pageSize());
        this.changeBufferLoadInterceptor = new ChangeBufferPageMergeInterceptor(
                changeBufferBitmaps, changeBufferStore, merger, miniTransactionManager,
                changeBufferPageGate, effectiveChangeBufferConfig.pageGateTimeout(), config.pageSize(),
                changeBufferCounters);
        this.secondaryIndexMutations = new SecondaryIndexMutationCoordinator(
                effectiveChangeBufferConfig, pool, miniTransactionManager, btreeService,
                btreeRootSnapshots, indexPageAccess, changeBufferStore, changeBufferBitmaps,
                changeBufferPageGate, changeBufferMetadataCatalog,
                typeRegistry, config.pageSize(), changeBufferCounters);
        this.changeBufferDdlBarrier = new ChangeBufferDdlBarrier(
                changeBufferStore, changeBufferBitmaps, changeBufferPageGate,
                miniTransactionManager, changeBufferMetadataCatalog, changeBufferCounters);

        // 4、这里只创建生命周期对象；在持久格式被 redo 校验前启动或 attach 都会提前暴露未恢复页面。
        this.changeBufferRecoveryValidator = new ChangeBufferRecoveryValidator(
                changeBufferHeaders, changeBufferStore, changeBufferBitmaps,
                miniTransactionManager, config.pageSize());
        this.changeBufferMergeWorker = new ChangeBufferMergeWorker(
                effectiveChangeBufferConfig, changeBufferStore, miniTransactionManager, pool,
                this::failChangeBufferMergeWorker);
    }

    /**
     * 把后台 Change Buffer fatal 原子投影到存储层准入，再通知公共组合根发布 FAILED。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先永久关闭写 MTR 准入，使尚未进入物理临界区的新写在申请 redo/page 资源前失败。</li>
     *     <li>把 recovery traffic gate 置为 FAILED 并保留同一个 fatal，阻止所有稳定 storage facade 继续服务。</li>
     *     <li>最后在不持存储锁的 worker 线程通知外层组合根；回调失败由 worker 作为 suppressed 诊断保留。</li>
     * </ol>
     *
     * @param failure 后台目标加载或发布前 merge 的致命失败；不得为 {@code null}
     */
    private void failChangeBufferMergeWorker(DatabaseFatalException failure) {
        // 1、新写必须先于外层状态通知被截断，不能依赖 Session 下一次碰巧观察到 fatal。
        writeAdmission.close();
        // 2、所有 requireOpen 入口立即观察 FAILED，并可通过 recovery diagnostics 读取原始 cause。
        recoveryGate.failClosed(failure);
        // 3、外层 DatabaseEngine 独立发布生命周期 FAILED；此处不持页 latch、MTR 或 worker lifecycle lock。
        backgroundFatalFailureHandler.accept(failure);
    }

    /**
     * 保留用户配置的容量与 timeout，只把产生新 mutation 的模式降级为 NONE。
     *
     * @param configured 已由 EngineConfig 完整校验的配置
     * @return 资源边界不变、模式为 NONE 的运行期配置
     */
    private static ChangeBufferConfig withChangeBufferModeDisabled(ChangeBufferConfig configured) {
        return new ChangeBufferConfig(cn.zhangyis.db.storage.changebuffer.ChangeBufferMode.NONE,
                configured.maxSizePercent(), configured.mergeInterval(), configured.mergeBatchPages(),
                configured.pageGateTimeout(), configured.stopTimeout());
    }

    /**
     * 在 system.ibd 已格式化或 redo 后校验完成时 set-once 安装发布前拦截器。安装本身不触页；成功后任何
     * 用户 leaf 从 LOADING 变为可见前都必须先消费对应 mutation。
     */
    private void attachChangeBufferLoadInterceptor() {
        if (changeBufferAvailable) {
            ((LruBufferPool) pool).attachPageLoadInterceptor(changeBufferLoadInterceptor);
        }
    }

    /**
     * recoveryMode 只定义 existing-open 的恢复语义。fresh open 会格式化 redo、doublewrite 和系统 undo 文件，
     * 因此不能接受 READ_ONLY_VALIDATE / force recovery 这类诊断模式；在打开 redo repo 之前拒绝，避免留下半初始化文件。
     */
    private void rejectFreshRecoveryMode(boolean fresh) {
        if (fresh && config.recoveryMode() != RecoveryMode.NORMAL) {
            throw new DatabaseValidationException("recovery mode " + config.recoveryMode()
                    + " requires existing engine files");
        }
    }

    /**
     * 校验 force-skip 的组合根不变量。该校验必须早于任何 data/undo tablespace 打开：
     * <ul>
     *   <li>普通模式不能携带 skipped set，避免配置残留导致静默跳过数据；</li>
     *   <li>force-skip 必须显式非空，不能由引擎猜测损坏空间；</li>
     *   <li>系统 undo 不能跳过，否则 UNDO_ROLLBACK/RESUME_PURGE 没有安全语义；</li>
     *   <li>显式配置的 legacy 聚簇索引所在空间不能跳过；DD resolver 模式若解析到被跳过表空间，会在恢复访问时 fail-closed。</li>
     * </ul>
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private void validateRecoverySkipConfiguration() {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        Set<SpaceId> skipped = recoveryExclusionPolicy.excludedSpaces();
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        if (config.recoveryMode() != RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE
                && !recoveryExclusionPolicy.administrativeSpaces().isEmpty()) {
            throw new DatabaseValidationException(
                    "force skipped spaces are only allowed in FORCE_SKIP_CORRUPT_TABLESPACE mode");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        if (config.recoveryMode() == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE
                && recoveryExclusionPolicy.administrativeSpaces().isEmpty()) {
            throw new DatabaseValidationException("FORCE_SKIP_CORRUPT_TABLESPACE requires skipped spaces");
        }
        if (skipped.contains(config.undoSpaceId())) {
            throw new DatabaseValidationException("system undo tablespace cannot be force-skipped: "
                    + config.undoSpaceId().value());
        }
        if (skipped.contains(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)) {
            throw new DatabaseValidationException("system tablespace cannot be force-skipped: "
                    + ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID.value());
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        if (clusteredIndex != null && skipped.contains(clusteredIndex.rootPageId().spaceId())) {
            throw new DatabaseValidationException("configured clustered index space cannot be force-skipped: "
                    + clusteredIndex.rootPageId().spaceId().value());
        }
    }

    /**
     * E2 existing-open 恢复入口。数据流为：先按 recovery 准入打开系统 undo 与显式配置的数据表空间，使
     * {@code PageStore} 拥有 redo apply 所需的物理句柄；再构造不可变 {@link RecoveryRequest}，由
     * {@link CrashRecoveryService} 负责关闭 gate、redo replay、安装 recoveredToLsn、续作 UNDO TRUNCATING、
     * SPACE_FILE_RECONCILE、事务 UNDO_ROLLBACK/RESUME_PURGE 和开放 gate。
     *
     * <p>简化点：当前引擎已使用 {@link RecoverableDoublewriteStrategy} 并携带
     * {@link DoublewriteRecoveryScanner}/page 列表，但 page 列表只来自 doublewrite 文件中的有效 slot，且仅覆盖启动恢复
     * 已打开的系统 undo 与显式配置数据表空间；没有全空间 checksum discovery。事务 UNDO_ROLLBACK
     * / RESUME_PURGE 已进入恢复服务阶段链，索引可由 legacy 单索引或 DD resolver 提供；DDL 收敛属于 DatabaseEngine 上层阶段。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 recovery 模式打开系统 undo 和允许参与恢复的数据表空间；force-skip 空间不创建物理句柄。</li>
     *     <li>从已打开空间筛选 doublewrite 候选页，并构造 checkpoint/redo 事务恢复上下文。</li>
     *     <li>按恢复模式组装不可变请求；NORMAL/FORCE 注入 rollback 与真实 purge 两阶段参与者，READ_ONLY 只校验证据。</li>
     *     <li>交给恢复总控执行并保存报告；总控返回前已经 force 恢复写且开放 traffic gate。</li>
     * </ol>
     */
    private void recoverExisting() {
        // 1、只打开恢复准入范围内的物理空间，避免 redo/doublewrite 访问未发现文件。
        if (changeBufferAvailable) {
            diskSpaceManager.openTablespaceForRecovery(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID,
                    config.systemTablespaceFile());
        }
        diskSpaceManager.openTablespaceForRecovery(config.undoSpaceId(), config.undoFile());
        for (EngineTablespaceConfig tablespace : config.recoveryTablespaces()) {
            if (recoveryExclusionPolicy.shouldSkip(tablespace.spaceId())) {
                continue;
            }
            diskSpaceManager.openTablespaceForRecovery(tablespace.spaceId(), tablespace.path());
        }

        // 2、doublewrite repair：scanner 用同一 dwRepo（含上一进程整页副本）。待检查页**只取恢复期已打开的空间**
        // （系统 undo + 配置的 recoveryTablespaces）——没有 DD discovery 前恢复只能触达这些空间，对未打开空间的页
        // 调 scanner 会触发 TablespaceNotOpenException；其它空间的 torn 页留待该空间被显式打开/未来 discovery 时修复。
        List<SpaceId> recoverySpaces = recoverySpaceIds();
        DoublewriteRecoveryScanner doublewriteScanner = doublewriteChannel == null
                ? null
                : new DoublewriteRecoveryScanner(doublewriteChannel, legacyDoublewriteRepo, store, config.pageSize());
        List<PageId> doublewritePages = new ArrayList<>();
        if (doublewriteChannel != null) {
            for (PageId pageId : doublewriteChannel.pageIds()) {
                if (recoverySpaces.contains(pageId.spaceId())) {
                    doublewritePages.add(pageId);
                }
            }
        }
        if (legacyDoublewriteRepo != null) {
            for (PageId pageId : legacyDoublewriteRepo.pageIds()) {
                if (recoverySpaces.contains(pageId.spaceId()) && !doublewritePages.contains(pageId)) {
                    doublewritePages.add(pageId);
                }
            }
        }
        TransactionRecoveryCheckpointSource transactionRecoverySource =
                transactionRecoveryCheckpointStore == null
                        ? TransactionRecoveryCheckpointSource.empty()
                        : transactionRecoveryCheckpointStore;
        TransactionRecoveryContext transactionRecovery = TransactionRecoveryContext.using(transactionRecoverySource);

        // 3、写恢复模式注入 rollback+purge 参与者；只读诊断模式禁止任何 undo/history 写入。
        TransactionUndoRecoveryParticipant undoRecovery = transactionUndoRecoveryParticipant();
        RecoveryRequest request = (switch (config.recoveryMode()) {
            case NORMAL -> RecoveryRequest.normal(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(),
                            new RedoApplyContext(store, config.pageSize()))
                    .withDoublewriteRepair(doublewriteScanner, doublewritePages)
                    .withRedoBoundaryInstall(redo)
                    .withUndoTablespaceRecovery(buildUndoTablespaceRecovery())
                    .withSpaceFileReconcile(recoverySpaces)
                    .withTransactionRecovery(transactionRecovery, undoRecovery);
            case READ_ONLY_VALIDATE -> RecoveryRequest.readOnlyValidate(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, config.pageSize()))
                    .withDoublewriteRepair(doublewriteScanner, doublewritePages)
                    .withTransactionRecoveryValidation(transactionRecovery);
            case FORCE_SKIP_CORRUPT_TABLESPACE -> RecoveryRequest.forceSkip(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(),
                            new RedoApplyContext(store, config.pageSize()),
                            config.forceSkippedSpaces())
                    .withDoublewriteRepair(doublewriteScanner, doublewritePages)
                    .withRedoBoundaryInstall(redo)
                    .withUndoTablespaceRecovery(buildUndoTablespaceRecovery())
                    .withSpaceFileReconcile(recoverySpaces)
                    .withTransactionRecovery(transactionRecovery, undoRecovery);
        }).withSpaceExclusionPolicy(recoveryExclusionPolicy);
        if (changeBufferAvailable) {
            request = request.withChangeBufferRecovery(this::recoverChangeBufferAfterRedo);
        }

        // 4、恢复总控负责阶段顺序、redo flush/page force 与 traffic gate；失败不会发布半恢复引擎。
        lastRecoveryReport = crashRecoveryService.recover(request);
    }

    /**
     * 在 redo 连续边界安装后验证 Change Buffer，并按恢复模式决定是否发布写合并能力。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>完整校验 page3 固定身份、全局树根/segment、sequence 单调性、pending 精确计数、
     *     单目标 64 条可合并上限与逐目标 bitmap；
     *     任一持久证据冲突都保持 recovery gate 关闭。</li>
     *     <li>READ_ONLY_VALIDATE 只保留诊断快照，不安装任何会写 target/bitmap 的拦截器。</li>
     *     <li>FORCE 模式拒绝带 pending 证据的实例，避免健康页读取绕过合并后导出陈旧索引结果。</li>
     *     <li>NORMAL 模式证明 pending 可由持久 DD exact-version 解析后，set-once 安装发布前拦截器。</li>
     * </ol>
     *
     * @throws ChangeBufferStateException pending 记录无法在当前恢复模式安全合并时抛出，调用方必须停止恢复
     */
    private void recoverChangeBufferAfterRedo() {
        // 1、validator 只在成功提交只读 MTR 后发布 validatedPendingOperations。
        changeBufferRecoveryValidator.validateAfterRedo();
        long pending = changeBufferRecoveryValidator.validatedPendingOperations();
        log.info("validated Change Buffer after redo: pending={} recoveryMode={}",
                pending, config.recoveryMode());

        // 2、只读诊断不能改变 Buffer Pool 发布协议，更不能消费全局树或修改用户 bitmap。
        if (config.recoveryMode() == RecoveryMode.READ_ONLY_VALIDATE) {
            return;
        }

        // 3、FORCE 不启动普通 merge 生命周期；只要仍有待合并记录，就无法证明导出读取看到了完整二级状态。
        if (config.recoveryMode() == RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE) {
            if (pending > 0) {
                throw new ChangeBufferStateException(
                        "force recovery cannot export with pending change buffer records: " + pending);
            }
            return;
        }

        // 4、跨重启 mutation 必须由持久 DD 精确解析 schemaVersion；空树允许低层 legacy 组合根继续以 NONE 打开。
        if (pending > 0 && !persistentChangeBufferMetadataAvailable) {
            throw new ChangeBufferStateException(
                    "pending change buffer records require an exact-version persistent metadata resolver: "
                            + pending);
        }
        attachChangeBufferLoadInterceptor();
    }

    /**
     * 构造绑定当前引擎生命周期的事务恢复参与者，使恢复总控能把 UNDO_ROLLBACK 与 RESUME_PURGE 分成两个可诊断阶段。
     *
     * @return 同时委托恢复扫描/回滚和 persistent history purge 的阶段端口。
     */
    private TransactionUndoRecoveryParticipant transactionUndoRecoveryParticipant() {
        return new TransactionUndoRecoveryParticipant() {
            /**
             * 执行数据库引擎组合根恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
             *
             * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
             * @param transactionSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
             * @return {@code recoverAfterRedo} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
             */
            @Override
            public TransactionUndoRecoveryResult recoverAfterRedo(
                    Lsn recoveredToLsn, RecoveredTransactionSnapshot transactionSnapshot) {
                return recoverTransactionUndoAfterRedo(recoveredToLsn, transactionSnapshot);
            }

            /**
             * 在 redo 与事务 undo 恢复完成后转交引擎级 purge 恢复步骤；调用返回前普通 SQL 流量仍保持关闭。
             */
            @Override
            public PurgeSummary resumePurgeAfterRedoWithSummary() {
                return StorageEngine.this.resumePurgeAfterRedo();
            }
        };
    }

    /**
     * 在用户流量开放前推进恢复出的 committed history，复用前台测试和后台 driver 的同一个 purge coordinator。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取恢复后的 history 长度；空链直接返回，非空链若缺索引 resolver/coordinator 则 fail-closed。</li>
     *     <li>按 rollback slot 容量循环执行 purge batch；每个 batch 内部只持单个短 undo/index MTR，不跨批保留 latch。</li>
     *     <li>每次正进展后校验运行时 history 恰好减少已完成日志数；无进展表示到达 purge boundary 或 guard 延后，保留 head 后返回。</li>
     * </ol>
     *
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private PurgeSummary resumePurgeAfterRedo() {
        // 1、persistent history 非空却没有 metadata 定位能力时，不能假装 RESUME_PURGE 已完成后开放流量。
        int historyBefore = history.committedSize();
        if (historyBefore == 0) {
            return new PurgeSummary(0, 0, 0, 0, 0);
        }
        if (purgeCoordinator == null) {
            throw new TransactionRecoveryException(
                    "recovered committed history requires an index resolver before RESUME_PURGE: entries="
                            + historyBefore);
        }

        // 2、恢复期没有用户事务和后台 purge worker；循环只推进当前安全 boundary，单批上限沿用 slot 容量。
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        int purgedLogs = 0;
        int removedClustered = 0;
        int removedSecondary = 0;
        int deferred = 0;
        int skippedUnavailable = 0;
        while (historyBefore > 0) {
            PurgeSummary summary = purgeCoordinator.runBatch(config.slotCapacity());
            purgedLogs += summary.purgedLogs();
            removedClustered += summary.removedClusteredRecords();
            removedSecondary += summary.removedSecondaryEntries();
            deferred += summary.deferredLogs();
            skippedUnavailable += summary.recoveryUnavailableRecordsSkipped();
            int historyAfter = history.committedSize();

            // 3、零进展保留不安全 head；正进展必须和 history 摘除数完全一致，否则拒绝潜在无限循环/投影漂移。
            if (summary.purgedLogs() == 0) {
                break;
            }
            if (historyAfter != historyBefore - summary.purgedLogs()) {
                throw new TransactionRecoveryException(
                        "RESUME_PURGE history projection changed inconsistently: before=" + historyBefore
                                + ", purged=" + summary.purgedLogs() + ", after=" + historyAfter);
            }
            historyBefore = historyAfter;
        }
        return new PurgeSummary(purgedLogs, removedClustered, removedSecondary, deferred,
                skippedUnavailable);
    }

    /**
     * checkpoint 元数据参与者：在 redo label force 前捕获并 force 事务 next-counter。TransactionSystem 短锁只保护
     * 内存快照，文件 IO 发生在快照返回后；失败直接阻止 checkpoint/reclaim 前进。
     */
    private void persistTransactionRecoveryCheckpoint(Lsn checkpointLsn) {
        TransactionCounterSnapshot counters = txnSystem.snapshotCounters();
        transactionRecoveryCheckpointStore.write(new TransactionRecoveryCheckpoint(
                checkpointLsn, counters.nextTransactionId(), counters.nextTransactionNo()));
    }

    /**
     * 正式 UNDO_ROLLBACK / RESUME_PURGE 阶段参与者。redo 重放和 UNDO tablespace 续作完成后，page3 与 undo first
     * 页已经恢复到物理一致状态；本方法扫描 rseg header 重建内存 slot 目录，再逐 slot 读取 undo first 页 header。
     * PREPARED 段在 gate 关闭期消费外部决议；ACTIVE 段在配置 legacy 单索引或 DD resolver 时执行恢复期 rollback；
     * COMMITTED 段按 {@code COMMIT_NO} 重建 committed history，并复位事务 id/no 高水位，供启动后的后台 purge driver 续作。
     *
     * <p>简化点：未引入独立 {@code UndoRecoveryService} 类；当前仍由 engine 组合根作为
     * {@code TransactionUndoRecoveryParticipant} 实现阶段端口。若发现 ACTIVE 但既无 legacy 索引也无 DD resolver，
     * 恢复 fail-closed，绝不保留未回滚 slot 后开放流量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param recoveredToLsn redo replay 连续恢复边界；作为 page3 合成证据 LSN，并用于 sidecar/redo 覆盖诊断。
     * @param transactionSnapshot checkpoint/redo 合并后的不可变事务证据；page3 扫描必须与它交叉校验。
     * @return 事务 undo 恢复摘要。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private TransactionUndoRecoveryResult recoverTransactionUndoAfterRedo(
            Lsn recoveredToLsn, RecoveredTransactionSnapshot transactionSnapshot) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        if (recoveredToLsn == null || transactionSnapshot == null) {
            throw new DatabaseValidationException("transaction undo recovery inputs must not be null");
        }
        MiniTransaction scan = miniTransactionManager.beginReadOnly();
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        RollbackSegmentHeaderSnapshot snapshot;
        try {
            snapshot = rsegHeaderRepo.read(scan, config.undoSpaceId(),
                    rollbackSlots.rollbackSegmentId(), config.slotCapacity(),
                    config.undoCachedSegmentsPerKind());
            miniTransactionManager.commit(scan);
        } catch (RuntimeException error) {
            rollbackRecoveryScanMtr(scan, error);
            throw error;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        List<CachedUndoSegmentRef> cachedInsert = readRecoveredUndoCache(
                snapshot.cachedInsertSegments(), UndoLogKind.INSERT);
        List<CachedUndoSegmentRef> cachedUpdate = readRecoveredUndoCache(
                snapshot.cachedUpdateSegments(), UndoLogKind.UPDATE);
        List<FreeUndoSegmentRef> free = readRecoveredUndoFree(snapshot.freeListBase(),
                snapshot.occupiedSlots().values(), cachedInsert, cachedUpdate);
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return recoverRollbackSegmentTransactions(recoveredToLsn, transactionSnapshot,
                snapshot.occupiedSlots(), snapshot.historyBase(), cachedInsert, cachedUpdate, free);
    }

    /**
     * R 1.2/R 1.3 恢复 rseg slot 的事务状态。数据流：每个 page3 restored slot 只读打开 undo first 页，读取
     * {@code STATE}/{@code TRANSACTION_ID}/{@code COMMIT_NO} 后立即提交 MTR 释放 undo latch；随后先重建 COMMITTED
     * history，按外部决议完成 PREPARED，再 rollback ACTIVE，purge 留给独立恢复阶段。这样不会在 undo latch 下访问
     * B+Tree，也避免 recovery 抢跑 history/phase-two 的物理顺序。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param occupiedSlots page3 扫描得到的 slot->undo first page 映射。
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @param transactionSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param historyBase 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param cachedInsert 参与 {@code recoverRollbackSegmentTransactions} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param cachedUpdate 参与 {@code recoverRollbackSegmentTransactions} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param free 参与 {@code recoverRollbackSegmentTransactions} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code recoverRollbackSegmentTransactions} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private TransactionUndoRecoveryResult recoverRollbackSegmentTransactions(
            Lsn recoveredToLsn,
            RecoveredTransactionSnapshot transactionSnapshot,
            Map<UndoSlotId, PageId> occupiedSlots,
            RollbackSegmentHistoryBase historyBase,
            List<CachedUndoSegmentRef> cachedInsert,
            List<CachedUndoSegmentRef> cachedUpdate,
            List<FreeUndoSegmentRef> free) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        List<RecoveredUndoSlotEvidence> recoveredSlots = new ArrayList<>();
        for (Map.Entry<UndoSlotId, PageId> entry : occupiedSlots.entrySet()) {
            recoveredSlots.add(readRecoveredUndoSlot(entry.getKey(), entry.getValue()));
        }
        // 所有 header 已在短只读 MTR 中复制并释放 latch；先全量校验，避免冲突中途已经 rollback/history 部分 slot。
        RecoveredTransactionReconciliation reconciliation = new RecoveredTransactionReconciler()
                .reconcile(transactionSnapshot, recoveredToLsn, recoveredSlots);
        PersistentHistoryRecovery historyRecovery = new PersistentHistoryRecovery();
        List<HistoryEntry> committed = historyRecovery.rebuild(
                historyBase, occupiedSlots, recoveredSlots, this::readRecoveredHistoryNode,
                this::readRecoveredHistoryAffectedTables);
        if (!reconciliation.activeSlots().isEmpty() && clusteredIndex == null && indexMetadataResolver == null) {
            throw new TransactionRecoveryException(
                    "recovered ACTIVE transactions require the configured clustered index; "
                            + "cannot skip undo rollback before opening traffic: activeSlots="
                            + reconciliation.activeSlots().size());
        }
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        Map<TransactionId, List<RecoveredUndoSlotEvidence>> preparedGroups = groupByCreator(
                reconciliation.preparedSlots());
        Map<TransactionId, PreparedTransactionDecision> preparedDecisions = new LinkedHashMap<>();
        for (TransactionId transactionId : preparedGroups.keySet()) {
            PreparedTransactionDecision decision = preparedDecisionProvider.decisionFor(transactionId);
            if (decision == null || decision == PreparedTransactionDecision.UNRESOLVED) {
                throw new TransactionRecoveryException(
                        "recovered PREPARED transaction has no authoritative decision: transaction="
                                + transactionId.value());
            }
            if (decision == PreparedTransactionDecision.ROLLBACK
                    && clusteredIndex == null
                    && !(indexMetadataResolver instanceof UndoTargetMetadataResolver)) {
                throw new TransactionRecoveryException(
                        "recovered PREPARED rollback requires clustered index or exact undo target resolver: "
                                + transactionId.value());
            }
            preparedDecisions.put(transactionId, decision);
        }
        // page3 active/cache、undo 首页和 FSP inode 已全部交叉校验后才发布内存投影；失败启动不会留下半个缓存栈。
        undoSegmentReuse.restore(cachedInsert, cachedUpdate, free);
        txnSystem.restoreCounters(reconciliation.snapshot().nextTransactionId().value(),
                historyRecovery.nextTransactionNo(historyBase,
                        reconciliation.snapshot().nextTransactionNo().value()));
        occupiedSlots.forEach(rollbackSlots::restore);
        // 已有 physical history 必须先恢复，prepared UPDATE commit 的 append lease 才能与 page3 tail 精确衔接。
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        history.restore(committed);
        PreparedRecoveryResolution preparedResolution = resolveRecoveredPreparedTransactions(
                preparedGroups, preparedDecisions);
        int rolledBackActiveSlots = 0;
        int skippedActiveRollbackRecords = 0;

        Map<TransactionId, List<RecoveredUndoSlotEvidence>> activeGroups =
                groupByCreator(reconciliation.activeSlots());
        for (Map.Entry<TransactionId, List<RecoveredUndoSlotEvidence>> group : activeGroups.entrySet()) {
            List<RecoveredUndoLogIdentity> logs = group.getValue().stream()
                    .map(recovered -> new RecoveredUndoLogIdentity(
                            recovered.kind(), recovered.slotId(), recovered.firstPageId()))
                    .toList();
            RollbackSummary summary = rollbackService.rollbackRecovered(logs, group.getKey(), clusteredIndex);
            skippedActiveRollbackRecords += summary.recoveryUnavailableRecordsSkipped();
            rolledBackActiveSlots += logs.size();
        }
        // prepared/active phase-two MTR 产生了新的 terminal redo；开放流量前至少先使 redo durable，
        // 即使数据/undo dirty page 尚未落盘，下一次崩溃也能幂等重放到相同终态。
        if (preparedResolution.resolvedSlots() > 0 || rolledBackActiveSlots > 0) {
            redo.flush();
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return new TransactionUndoRecoveryResult(occupiedSlots.size(), rolledBackActiveSlots,
                0, committed.size(), skippedActiveRollbackRecords,
                preparedResolution.skippedUnavailableRecords());
    }

    /** 按 creator 保持 page3 扫描顺序分组；返回集合只含恢复值对象，不持有任何 page latch。 */
    private static Map<TransactionId, List<RecoveredUndoSlotEvidence>> groupByCreator(
            List<RecoveredUndoSlotEvidence> slots) {
        Map<TransactionId, List<RecoveredUndoSlotEvidence>> groups = new LinkedHashMap<>();
        for (RecoveredUndoSlotEvidence recovered : slots.stream()
                .sorted(Comparator.comparingLong(
                        slot -> slot.creatorTransactionId().value()))
                .toList()) {
            groups.computeIfAbsent(recovered.creatorTransactionId(), ignored -> new ArrayList<>())
                    .add(recovered);
        }
        return groups;
    }

    /**
     * 在 recovery gate 关闭期间重建并完成全部 PREPARED participant。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 creator 逐 first-page 短读 logical head/LAST_UNDO_NO，构造不持 latch 的 binding。</li>
     *     <li>由 TransactionManager 恢复 PREPARED 聚合与 active membership，保持 purge owner 不提前消失。</li>
     *     <li>COMMIT 先从 UPDATE logical chain 投影 affected tables，再复用 live prepared finalizer；ROLLBACK
     *         复用逐记录 inverse 与 prepared owner drop。</li>
     *     <li>每个 participant 只有在物理 terminal MTR 成功后才移出 active table；任一失败让整个启动 fail-closed。</li>
     * </ol>
     *
     * @param groups 已通过 redo/page3/header 状态闭包校验的 PREPARED slots
     * @param decisions 每个 creator 的外部权威 COMMIT/ROLLBACK 决议
     * @return 已完成 phase-two 的 slot 数量
     * @throws TransactionRecoveryException binding、决议、metadata 或 phase-two 失败时抛出
     */
    private PreparedRecoveryResolution resolveRecoveredPreparedTransactions(
            Map<TransactionId, List<RecoveredUndoSlotEvidence>> groups,
            Map<TransactionId, PreparedTransactionDecision> decisions) {
        int resolvedSlots = 0;
        int skippedUnavailableRecords = 0;
        for (Map.Entry<TransactionId, List<RecoveredUndoSlotEvidence>> group : groups.entrySet()) {
            // 1、每条 first page 单独读取并释放 S latch；global high-water 取两种 log 的最大物理值。
            List<RecoveredPreparedBinding> recoveredBindings = group.getValue().stream()
                    .map(this::readRecoveredPreparedBinding)
                    .toList();
            long highWater = 0L;
            for (RecoveredPreparedBinding recovered : recoveredBindings) {
                highWater = Math.max(highWater, recovered.lastUndoNo().value());
            }
            List<UndoLogBinding> bindings = recoveredBindings.stream()
                    .map(RecoveredPreparedBinding::binding)
                    .toList();
            // 2、运行态 PREPARED 登记后，phase-two manager/finalizer 可以复用 live 不变量。
            Transaction transaction = transactionManager.restorePrepared(
                    group.getKey(), rollbackSlots.rollbackSegmentId(), bindings, UndoNo.of(highWater));
            PreparedTransactionDecision decision = decisions.get(group.getKey());
            // 3、commit 的 affected-table side projection 已在崩溃中丢失，只能从持久 logical chain 重建。
            if (decision == PreparedTransactionDecision.COMMIT) {
                transactionManager.prepareCommitPrepared(transaction);
                UndoLogBinding update = transaction.undoContext().binding(UndoLogKind.UPDATE);
                Set<Long> affectedTables = update == null
                        ? Set.of()
                        : readRecoveredHistoryAffectedTables(
                                update.firstPageId(), update.logicalHead());
                undoLogManager.onCommitPrepared(transaction, affectedTables);
                transactionManager.commitPrepared(transaction);
            } else if (decision == PreparedTransactionDecision.ROLLBACK) {
                if (clusteredIndex != null) {
                    skippedUnavailableRecords += rollbackService
                            .rollbackPreparedRecovered(transaction, clusteredIndex)
                            .recoveryUnavailableRecordsSkipped();
                } else {
                    skippedUnavailableRecords += rollbackService
                            .rollbackPreparedRecovered(transaction, null)
                            .recoveryUnavailableRecordsSkipped();
                }
            } else {
                throw new TransactionRecoveryException(
                        "invalid prepared recovery decision after preflight: transaction="
                                + group.getKey().value() + ", decision=" + decision);
            }
            // 4、成功终态已经由 manager 移出 active table；slot 数按物理 participant owner 计数。
            resolvedSlots += group.getValue().size();
        }
        return new PreparedRecoveryResolution(resolvedSlots, skippedUnavailableRecords);
    }

    /** recovered PREPARED phase-two 的 slot 完成数与隔离记录跳过数。 */
    private record PreparedRecoveryResolution(int resolvedSlots, int skippedUnavailableRecords) {
    }

    /**
     * 从 PREPARED first page 复制运行时 binding 所需字段。读取完成前不访问 B+Tree、决议 provider 或 active table。
     *
     * @param evidence 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code readRecoveredPreparedBinding} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private RecoveredPreparedBinding readRecoveredPreparedBinding(
            RecoveredUndoSlotEvidence evidence) {
        MiniTransaction read = miniTransactionManager.beginReadOnly();
        try {
            UndoLogSegment segment = undoAccess.open(
                    read, evidence.firstPageId(), PageLatchMode.SHARED);
            if (!segment.isPrepared()
                    || segment.undoKind() != evidence.kind()
                    || !segment.creatorTransactionId().equals(evidence.creatorTransactionId())) {
                throw new TransactionRecoveryException(
                        "prepared undo binding drifted after reconciliation: "
                                + evidence.firstPageId());
            }
            RecoveredPreparedBinding recovered = new RecoveredPreparedBinding(
                    new UndoLogBinding(evidence.kind(), evidence.slotId(),
                            evidence.firstPageId(), segment.logicalHead()),
                    segment.logLastUndoNo());
            miniTransactionManager.commit(read);
            return recovered;
        } catch (RuntimeException error) {
            rollbackRecoveryScanMtr(read, error);
            throw error;
        }
    }

    /** history recovery 每个 first page 使用独立短 MTR，返回后不保留 latch/fix。
     *
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code readRecoveredHistoryNode} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private UndoHistoryNodeSnapshot readRecoveredHistoryNode(PageId firstPageId) {
        MiniTransaction read = miniTransactionManager.beginReadOnly();
        try {
            UndoHistoryNodeSnapshot snapshot = undoAccess.inspectHistoryNode(read, firstPageId);
            miniTransactionManager.commit(read);
            return snapshot;
        } catch (RuntimeException error) {
            rollbackRecoveryScanMtr(read, error);
            throw error;
        }
    }

    /**
     * 从一条恢复出的 UPDATE history logical head 投影稳定表集合，不读取独立 sidecar/count。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>用 first-page recordCount 冻结最大遍历步数，并校验传入 logical head 未超过物理高水位。</li>
     *     <li>每个 roll pointer 在独立短 MTR 中先读 schema-free identity，再经 legacy/DD resolver 取得 exact schema 完整解码。</li>
     *     <li>校验 creator、undoNo 严格下降和 UPDATE/DELETE 类型，收集 table id 后沿 prevRollPointer 继续。</li>
     *     <li>NULL 前驱前未超界则返回去重集合；超步数、断链或 metadata 不可解析均 fail-closed。</li>
     * </ol>
     *
     * @param firstPageId 当前 committed UPDATE undo log 首页。
     * @param logicalHead first-page header 中恢复出的权威逻辑链头。
     * @return 当前 logical chain 实际涉及的稳定 table id 集合。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private Set<Long> readRecoveredHistoryAffectedTables(PageId firstPageId, UndoLogicalHead logicalHead) {
        if (firstPageId == null || logicalHead == null) {
            throw new DatabaseValidationException("recovered history projection inputs must not be null");
        }
        if (logicalHead.isEmpty()) {
            return Set.of();
        }

        // 1、先短读物理 recordCount 作为损坏链上限；不把 first-page latch 带入逐条 metadata 解析。
        long maxRecords;
        MiniTransaction headerRead = miniTransactionManager.beginReadOnly();
        try {
            UndoLogSegment segment = undoAccess.open(headerRead, firstPageId, PageLatchMode.SHARED);
            maxRecords = segment.logRecordCount();
            if (logicalHead.undoNo().value() > segment.logLastUndoNo().value()) {
                throw new UndoLogFormatException("recovered history logical head exceeds physical high-water: "
                        + firstPageId);
            }
            miniTransactionManager.commit(headerRead);
        } catch (RuntimeException error) {
            rollbackRecoveryScanMtr(headerRead, error);
            throw error;
        }

        Set<Long> affected = new LinkedHashSet<>();
        RollPointer pointer = logicalHead.rollPointer();
        long previousUndoNo = Long.MAX_VALUE;
        long visited = 0L;
        boolean first = true;
        while (!pointer.isNull()) {
            if (visited++ >= maxRecords) {
                throw new TransactionRecoveryException(
                        "recovered history logical chain exceeds physical record count: " + firstPageId);
            }

            // 2、单 pointer 短读：identity 只选 metadata，完整 record 才提供版本链 predecessor。
            UndoRecord record;
            MiniTransaction recordRead = miniTransactionManager.beginReadOnly();
            try {
                UndoLogSegment segment = undoAccess.open(recordRead, firstPageId, PageLatchMode.SHARED);
                UndoRecordIdentity identity = segment.readRecordIdentity(pointer);
                BTreeIndex index = resolveRecoveryHistoryIndex(identity);
                record = segment.readRecord(pointer, index.keyDef(), index.schema());
                miniTransactionManager.commit(recordRead);
            } catch (RuntimeException error) {
                rollbackRecoveryScanMtr(recordRead, error);
                throw error;
            }

            // 3、history 只允许 UPDATE_ROW/DELETE_MARK，且同一 creator 的 undoNo 必须严格下降。
            if (record.type() != UndoRecordType.UPDATE_ROW && record.type() != UndoRecordType.DELETE_MARK) {
                throw new TransactionRecoveryException(
                        "recovered UPDATE history contains non-update record: " + record.type());
            }
            if (first && !record.undoNo().equals(logicalHead.undoNo())) {
                throw new TransactionRecoveryException(
                        "recovered history logical head pointer resolves to undoNo "
                                + record.undoNo().value() + " instead of " + logicalHead.undoNo().value());
            }
            if (record.undoNo().value() >= previousUndoNo) {
                throw new TransactionRecoveryException(
                        "recovered history logical undo numbers are not strictly descending: "
                                + record.undoNo().value() + " after " + previousUndoNo);
            }
            affected.add(record.tableId());
            first = false;
            previousUndoNo = record.undoNo().value();
            pointer = record.prevRollPointer();
        }

        // 4、HistoryEntry 构造器会排序并防御性冻结；此处集合只表达当前可达链，不包含 detached rollback 分支。
        return Set.copyOf(affected);
    }

    /** recovery affected-table 投影按 undo identity 选择 exact-version 聚簇 metadata。
     *
     * @param identity 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code resolveRecoveryHistoryIndex} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private BTreeIndex resolveRecoveryHistoryIndex(UndoRecordIdentity identity) {
        if (indexMetadataResolver != null) {
            return indexMetadataResolver.resolve(identity.tableId(), identity.indexId());
        }
        if (clusteredIndex != null && clusteredIndex.indexId() == identity.indexId()) {
            return clusteredIndex;
        }
        throw new TransactionRecoveryException(
                "recovered history requires index metadata: table=" + identity.tableId()
                        + ", index=" + identity.indexId());
    }

    /**
     * 恢复一个持久 cached 栈。每个 owner 先在短 MTR 中校验空单页 undo header，再在另一个短 MTR 中读取
     * page0/page2 FSP inode；刻意不同时持有 undo first-page latch 与 FSP latch，维持“空间账本先于数据页”的锁序。
     * 只有 {@code used=fragment=1, extent=0} 的 segment 才能恢复为可复用缓存，否则 page3 owner 视为损坏。
     * @param firstPages 参与 {@code readRecoveredUndoCache} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param kind 选择 {@code readRecoveredUndoCache} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private List<CachedUndoSegmentRef> readRecoveredUndoCache(List<PageId> firstPages, UndoLogKind kind) {
        List<CachedUndoSegmentRef> recovered = new ArrayList<>(firstPages.size());
        for (PageId firstPage : firstPages) {
            MiniTransaction pageMtr = miniTransactionManager.beginReadOnly();
            CachedUndoSegmentRef cached;
            try {
                cached = undoAccess.inspectCached(pageMtr, firstPage, kind);
                miniTransactionManager.commit(pageMtr);
            } catch (RuntimeException error) {
                rollbackRecoveryScanMtr(pageMtr, error);
                throw error;
            }

            MiniTransaction fspMtr = miniTransactionManager.beginReadOnly();
            try {
                UndoSegmentDropPlan plan = undoAllocator.inspectDropPlan(fspMtr, cached.handle());
                if (plan.usedPageCount() != 1L || plan.fragmentPageCount() != 1L || plan.extentCount() != 0L) {
                    throw new UndoLogFormatException("cached undo segment is not a single fragment owner: firstPage="
                            + firstPage + ", plan=" + plan);
                }
                miniTransactionManager.commit(fspMtr);
            } catch (RuntimeException error) {
                rollbackRecoveryScanMtr(fspMtr, error);
                throw error;
            }
            recovered.add(cached);
        }
        return List.copyOf(recovered);
    }

    /**
     * 按 page3 v4 length 有界恢复 free FIFO。每个节点使用独立首页 MTR，FSP identity 再用另一个 MTR 校验；
     * prev/next、cycle、tail、资格或与 active/cache owner 重复时一律 fail-closed。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param base 参与 {@code readRecoveredUndoFree} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param activeOwners 参与 {@code readRecoveredUndoFree} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param cachedInsert 参与 {@code readRecoveredUndoFree} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param cachedUpdate 参与 {@code readRecoveredUndoFree} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private List<FreeUndoSegmentRef> readRecoveredUndoFree(
            RollbackSegmentFreeListBase base, Collection<PageId> activeOwners,
            List<CachedUndoSegmentRef> cachedInsert, List<CachedUndoSegmentRef> cachedUpdate) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        if (base == null || activeOwners == null || cachedInsert == null || cachedUpdate == null) {
            throw new DatabaseValidationException("recovered undo free inputs must not be null");
        }
        if (base.length() == 0L) {
            return List.of();
        }
        Set<PageId> owners = new LinkedHashSet<>(activeOwners);
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        cachedInsert.forEach(item -> owners.add(item.handle().firstPageId()));
        cachedUpdate.forEach(item -> owners.add(item.handle().firstPageId()));
        List<FreeUndoSegmentRef> recovered = new ArrayList<>((int) base.length());
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        Optional<PageId> expectedPrevious = Optional.empty();
        PageId current = base.headPageId().orElseThrow();
        for (int index = 0; index < (int) base.length(); index++) {
            if (!owners.add(current)) {
                throw new UndoLogFormatException("duplicate/cyclic recovered free undo owner: " + current);
            }
            MiniTransaction pageMtr = miniTransactionManager.beginReadOnly();
            UndoFreeListNodeSnapshot node;
            try {
                node = undoAccess.inspectFree(pageMtr, current);
                miniTransactionManager.commit(pageMtr);
            } catch (RuntimeException error) {
                rollbackRecoveryScanMtr(pageMtr, error);
                throw error;
            }
            if (!node.previousFreePageId().equals(expectedPrevious)) {
                throw new UndoLogFormatException("recovered free prev link mismatch at " + current
                        + ": expected=" + expectedPrevious + ", current=" + node.previousFreePageId());
            }
            boolean last = index + 1 == (int) base.length();
            if (last) {
                if (node.nextFreePageId().isPresent() || !base.tailPageId().equals(Optional.of(current))) {
                    throw new UndoLogFormatException("recovered free tail/length mismatch at " + current);
                }
            } else if (node.nextFreePageId().isEmpty()) {
                throw new UndoLogFormatException("recovered free chain ended before persisted length at " + current);
            }

            MiniTransaction fspMtr = miniTransactionManager.beginReadOnly();
            try {
                UndoSegmentDropPlan plan = undoAllocator.inspectDropPlan(fspMtr, node.segment().handle());
                if (plan.usedPageCount() != 1L || plan.fragmentPageCount() != 1L || plan.extentCount() != 0L) {
                    throw new UndoLogFormatException("free undo segment is not a single fragment owner: firstPage="
                            + current + ", plan=" + plan);
                }
                miniTransactionManager.commit(fspMtr);
            } catch (RuntimeException error) {
                rollbackRecoveryScanMtr(fspMtr, error);
                throw error;
            }
            recovered.add(node.segment());
            expectedPrevious = Optional.of(current);
            if (!last) {
                current = node.nextFreePageId().orElseThrow();
            }
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return List.copyOf(recovered);
    }

    /**
     * 只读读取一个 restored undo slot 的恢复 header。异常时回滚该只读 MTR，避免损坏 undo 页导致 guard/lease 泄漏。
     * 返回值只含内存快照，不持有 page latch；调用方可安全地在之后进入 B+Tree rollback 或 history 入队。
     *
     * @param slotId 参与 {@code readRecoveredUndoSlot} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @return {@code readRecoveredUndoSlot} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private RecoveredUndoSlotEvidence readRecoveredUndoSlot(UndoSlotId slotId, PageId firstPageId) {
        MiniTransaction stateMtr = miniTransactionManager.beginReadOnly();
        try {
            UndoLogSegment segment = undoAccess.open(stateMtr, firstPageId, PageLatchMode.SHARED);
            boolean active = segment.isActive();
            boolean prepared = segment.isPrepared();
            boolean committed = segment.isCommitted();
            if (!active && !prepared && !committed) {
                throw new UndoLogFormatException("undo slot " + slotId + " first page " + firstPageId
                        + " has unknown state " + segment.state());
            }
            RecoveredUndoSlotEvidence recovered;
            if (active) {
                recovered = RecoveredUndoSlotEvidence.active(
                        slotId, firstPageId, segment.undoKind(), segment.creatorTransactionId());
            } else if (prepared) {
                recovered = RecoveredUndoSlotEvidence.prepared(
                        slotId, firstPageId, segment.undoKind(), segment.creatorTransactionId());
            } else {
                recovered = RecoveredUndoSlotEvidence.committed(
                        slotId, firstPageId, segment.undoKind(), segment.creatorTransactionId(),
                        segment.committedTransactionNo());
            }
            miniTransactionManager.commit(stateMtr);
            return recovered;
        } catch (RuntimeException e) {
            rollbackRecoveryScanMtr(stateMtr, e);
            throw e;
        }
    }

    /** 恢复只读扫描失败时释放 memo；清理异常只作 suppressed，不能覆盖原始损坏/IO 根因。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private void rollbackRecoveryScanMtr(MiniTransaction mtr, RuntimeException original) {
        try {
            miniTransactionManager.rollbackUncommitted(mtr);
        } catch (RuntimeException cleanupFailure) {
            original.addSuppressed(cleanupFailure);
        }
    }

    /** 恢复 PREPARED 聚合所需的无 latch binding 与物理 undoNo 高水位。
     *
     * @param binding 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param lastUndoNo 参与 {@code 构造} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    private record RecoveredPreparedBinding(UndoLogBinding binding, UndoNo lastUndoNo) {
        private RecoveredPreparedBinding {
            if (binding == null || lastUndoNo == null) {
                throw new DatabaseValidationException(
                        "recovered prepared binding fields must not be null");
            }
        }
    }

    /**
     * 构造 UNDO truncate 恢复参与者。它必须共享 engine 的 {@link TablespaceAccessController}、registry、redo 和
     * flushService：恢复续作会在同一空间上获取 X operation lease、写 marker/rebuild redo、走 WAL flush barrier，
     * 最后发布 registry 状态；若这些依赖不是同一实例，普通准入看到的生命周期状态会与恢复写入脱节。
     */
    private UndoTablespaceTruncationRecovery buildUndoTablespaceRecovery() {
        return new UndoTablespaceTruncationRecovery(Set.of(config.undoSpaceId()), store, config.pageSize(),
                registry, redo, undoTablespaceTruncationService);
    }

    /**
     * SPACE_FILE_RECONCILE 输入集合。系统 undo 由引擎固定加入；数据表空间仅限配置显式列出的集合。使用
     * {@link LinkedHashSet} 保持诊断顺序稳定，同时防御未来构造器变化导致的重复 SpaceId。
     * @return {@code recoverySpaceIds} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     */
    private List<SpaceId> recoverySpaceIds() {
        LinkedHashSet<SpaceId> spaces = new LinkedHashSet<>();
        if (changeBufferAvailable) {
            spaces.add(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID);
        }
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
        pageCleanerSupervisor = new PageCleanerSupervisor(
                () -> new PageCleanerWorker(flushService, config.pageCleanerQueueCapacity(),
                        config.backgroundFlushInterval(), config.backgroundFlushMaxPages()),
                1, config.backgroundFlushInterval(), config.backgroundFlushInterval());
        pageCleanerSupervisor.start();
    }

    /**
     * ASYNC_FLUSH 前台只唤醒后台 page cleaner，不在调用线程执行 IO。后台禁用或 worker 尚未启动时保持 no-op，
     * 因为真正需要阻塞的 SYNC_FLUSH/HARD_LIMIT 会走 {@code blockingFlushOnce}。
     */
    private void requestBackgroundFlush(int maxPages) {
        if (pageCleanerSupervisor != null) {
            pageCleanerSupervisor.requestFlush(maxPages);
        }
    }

    /**
     * 前台 capacity throttle 的同步刷页上限。后台 maxPages=0 是合法配置，含义是后台只推进 checkpoint；
     * 但前台 SYNC_FLUSH/HARD_LIMIT 已经在阻塞用户写入，必须允许刷出足够 dirty page 来推动 oldest dirty LSN。
     */
    private int foregroundCapacityFlushMaxPages() {
        return Math.max(1, config.bufferPoolCapacityFrames());
    }

    /**
     * 推进数据库引擎组合根刷盘或检查点边界；写数据前遵守 WAL，失败时不得清除尚未安全持久化的状态。
     */
    private void startBackgroundRedoFlusher() {
        if (!config.backgroundFlushEnabled()) {
            return;
        }
        redoFlushWorker = new RedoFlushWorker(new RedoLogManagerFlushTarget(redo), config.redoFlushInterval());
        redoFlushWorker.start();
    }

    /**
     * 0.4：启动后台 purge driver。DD 组合根传入 resolver 时，每条 undo 按 tableId/indexId 定位聚簇索引；
     * 完整 DD 路径的 coordinator 再按 {@link EngineConfig#purgeConfig()} 使用有界 table-token worker pool；低层兼容
     * 构造仍使用 direct 串行。后台未启用或两种元数据来源都缺失时不启动 driver（pool 在 close 仍会显式停止），
     * 避免在不能确认目标索引的情况下物理移除记录。
     */
    private void startBackgroundPurgeDriver() {
        if (!config.backgroundFlushEnabled() || clusteredIndex == null && indexMetadataResolver == null) {
            return;
        }
        purgeDriverWorker = new PurgeDriverWorker(
                purgeCoordinator, config.slotCapacity(), config.backgroundFlushInterval(),
                undoTruncationScheduler);
        purgeDriverWorker.start();
    }

    /**
     * 启动低优先级 Change Buffer 主动合并。demand-load correctness 不依赖该 worker；后台关闭时仍由发布前
     * interceptor 保证首次可见前合并。worker 必须晚于 redo flusher/page cleaner，避免其产生的 merge redo 无持久推进者。
     */
    private void startBackgroundChangeBufferMerge() {
        if (!config.backgroundFlushEnabled() || changeBufferMergeWorker == null) {
            return;
        }
        changeBufferMergeWorker.start();
    }

    /**
     * 0.10a/0.10c：启动后台 read-ahead 服务并接 Buffer Pool 钩子。仅在后台启用时启动；linear 阈值取 InnoDB 默认 56，
     * random 阈值取 {@link #RANDOM_READ_AHEAD_THRESHOLD}=0（禁用，对齐 MySQL OFF），故一般负载（含既有测试）不触发
     * 预取、行为不变。必须晚于 bootstrap/recover，使其只跟踪普通 getPage 访问。
     * @param lruPool 由组合根提供的 {@code LruBufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code startBackgroundReadAhead} 调用
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
        writeAdmission.assertWriteAllowed();
        flushService.flushThrough(redo.currentLsn(), config.flushTimeout());
    }

    /**
     * 关闭引擎。先停 read-ahead，再同时请求 purge driver/pool 停止并在共享 deadline 内等到记录边界；随后停止
     * page cleaner/redo flusher，调用 {@link FlushService#flushThrough} 按 WAL 顺序持久并清空 dirty，最后关闭
     * buffer pool、store、redo repository 和 checkpoint stores。任一后台组件超时都不释放其依赖的底层句柄。
     * CLOSED 再 close 为 no-op。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @throws EngineStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void close() {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        if (state == EngineState.CLOSED) {
            return;
        }
        if (state == EngineState.READ_ONLY) {
            stopBackgroundChangeBufferMerge();
            stopBackgroundPurgeDriver();
            closeOpenedHandles();
            return;
        }
        if (state != EngineState.OPEN) {
            throw new EngineStateException("close requires OPEN state: " + state);
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        stopBackgroundReadAhead();
        stopBackgroundChangeBufferMerge();
        stopBackgroundPurgeDriver();
        stopBackgroundPageCleaner();
        stopBackgroundRedoFlusher();
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        flushService.flushThrough(redo.currentLsn(), config.flushTimeout());
        // FORCE 导出模式不写 warmup sidecar；普通实例才保存热页定位。
        if (writeAdmission.mode() != StorageWriteAdmission.Mode.EXPORT_READ_ONLY) {
            new BufferPoolWarmupService().dump(pool, config.bufferPoolDumpFile());
        }
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        closeOpenedHandles();
    }

    /**
     * 释放引擎打开的底层句柄。普通 OPEN close 在调用前已经停后台 worker、flushThrough 并写 warmup dump；
     * READ_ONLY_VALIDATE close 则直接走这里，避免把诊断实例的关闭动作变成隐式刷盘或预热状态写入。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     */
    private void closeOpenedHandles() {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        List<RuntimeException> errors = new ArrayList<>();
        closeQuietly(pool, errors);
        closeQuietly(store, errors);
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        closeQuietly(redoRepo, errors);
        closeQuietly(checkpointStore, errors);
        closeQuietly(transactionRecoveryCheckpointStore, errors);
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        closeQuietly(doublewriteChannel, errors);
        closeQuietly(legacyDoublewriteRepo, errors);
        writeAdmission.close();
        state = EngineState.CLOSED;
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        if (!errors.isEmpty()) {
            DatabaseRuntimeException aggregate =
                    new DatabaseRuntimeException("engine close failed to release " + errors.size() + " handle(s)",
                            errors.get(0));
            errors.subList(1, errors.size()).forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    /**
     * open 失败清理。停止顺序与普通 close 一致，但每个失败只作为原始启动异常的 suppressed，不能覆盖导致
     * fail-closed 的 redo/sidecar/page3 根因；句柄聚合关闭结束后状态固定为 CLOSED，调用方可安全重复 close。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private void cleanupAfterFailedOpen(RuntimeException original) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        stopAfterFailedOpen(this::stopBackgroundReadAhead, original);
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        stopAfterFailedOpen(this::stopBackgroundChangeBufferMerge, original);
        stopAfterFailedOpen(this::stopBackgroundPurgeDriver, original);
        stopAfterFailedOpen(this::stopBackgroundPageCleaner, original);
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        stopAfterFailedOpen(this::stopBackgroundRedoFlusher, original);
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        try {
            closeOpenedHandles();
        } catch (RuntimeException closeFailure) {
            original.addSuppressed(closeFailure);
        }
    }

    /**
     * 定位并读取数据库引擎组合根领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param stopAction 在契约指定成功、失败或释放边界调用的回调；不得为 {@code null}，且不得破坏当前资源所有权和异常传播规则
     * @param original 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private static void stopAfterFailedOpen(Runnable stopAction, RuntimeException original) {
        try {
            stopAction.run();
        } catch (RuntimeException stopFailure) {
            original.addSuppressed(stopFailure);
        }
    }

    /**
     * 停止后台 page cleaner 后再进入 final flush。若 worker 仍在执行 flush cycle，stop 会等待该轮完成；
     * 超时不继续关闭 page store/buffer pool，避免后台 IO 与句柄释放并发。
     *
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
    private void stopBackgroundPageCleaner() {
        if (pageCleanerSupervisor == null) {
            return;
        }
        boolean stopped = pageCleanerSupervisor.stop(config.backgroundFlushStopTimeout());
        if (!stopped) {
            throw new DatabaseRuntimeException("page cleaner did not stop within "
                    + config.backgroundFlushStopTimeout());
        }
    }

    /**
     * 在停止 page cleaner、final flush 与关闭 Buffer Pool 前有界停止 Change Buffer worker。即使 worker 从未启动，
     * close 也会把 NEW 转为 STOPPED，使失败 open 和 FORCE/READ_ONLY 生命周期没有遗留线程所有权。
     *
     * @throws ChangeBufferStateException worker 在自身 stop timeout 内不能终止时抛出，调用方不得继续关闭页依赖
     */
    private void stopBackgroundChangeBufferMerge() {
        if (changeBufferMergeWorker != null) {
            changeBufferMergeWorker.close();
        }
    }

    /** 在 final flush / 关 store 之前停止后台 read-ahead，避免预取盘 IO 与句柄释放并发；超时则中止关闭。
     *
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
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
     *
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
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

    /**
     * 在 final flush 前停止后台 purge driver 与 coordinator worker pool；先向二者发布停止，再共享同一等待预算。
     * 即使 driver 从未启动，组合根仍必须停止惰性 pool，避免 READ_ONLY/失败 open 释放底层句柄后遗留平台线程。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>记录单一等待预算并向 driver 发布停止，使其不再发起新批次；该阶段不阻塞。</li>
     *     <li>向 coordinator pool 发布取消，排队任务不会再启动，运行任务在 record 边界观察线程中断。</li>
     *     <li>先等待 driver、再用同一预算的剩余时间等待 worker；任何一方超时都保留引擎句柄并拒绝继续关闭。</li>
     * </ol>
     *
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
    private void stopBackgroundPurgeDriver() {
        // 1、先记录共享预算并停止 dispatcher；不能先等待，否则 dispatcher 可能仍阻塞在尚未取消的 worker 批次。
        long budgetNanos = saturatedNanos(config.backgroundFlushStopTimeout());
        long startedNanos = System.nanoTime();
        if (purgeDriverWorker != null) {
            purgeDriverWorker.requestStop();
        }

        // 2、pool 与 driver 在任何等待前都收到停止；pool 取消排队 stage，但记录内物理修改不接受线程中断。
        if (purgeCoordinator != null) {
            purgeCoordinator.requestStopWorkers();
        }

        // 3、两个 await 消耗同一预算，避免分别等待完整 timeout 后把 close 最坏时延翻倍。
        boolean driverStopped = purgeDriverWorker == null
                || purgeDriverWorker.awaitStopped(remainingDuration(startedNanos, budgetNanos));
        boolean workersStopped = purgeCoordinator == null
                || purgeCoordinator.awaitWorkersStopped(remainingDuration(startedNanos, budgetNanos));
        if (!driverStopped || !workersStopped) {
            throw new DatabaseRuntimeException("purge background components did not stop within "
                    + config.backgroundFlushStopTimeout());
        }
    }

    /**
     * 将配置等待时间转换为不溢出的纳秒预算，供多个后台组件共享同一次 close 上限。
     *
     * @param timeout 已由 {@link EngineConfig} 校验为正的后台停止时长
     * @return 可参与单调时钟差值计算的纳秒数；超出 {@code long} 时饱和为最大值
     */
    private static long saturatedNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * 根据同一开始时刻计算剩余关闭预算；{@link System#nanoTime()} 的差值计算允许计时器跨符号位回绕。
     *
     * @param startedNanos 第一次发布停止前读取的单调时钟值
     * @param budgetNanos 配置转换后的非负、饱和纳秒预算
     * @return 尚可用于下一次有界等待的时长；预算耗尽时为 {@link Duration#ZERO}
     */
    private static Duration remainingDuration(long startedNanos, long budgetNanos) {
        long elapsed = System.nanoTime() - startedNanos;
        if (elapsed <= 0) {
            return Duration.ofNanos(budgetNanos);
        }
        return Duration.ofNanos(elapsed >= budgetNanos ? 0L : budgetNanos - elapsed);
    }

    /**
     * 返回 {@code miniTransactionManager} 对应的数据库引擎组合根受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     *
     * @return {@code miniTransactionManager} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     */
    public MiniTransactionManager miniTransactionManager() {
        requireOpen();
        return miniTransactionManager;
    }

    /**
     * 返回组合根唯一自增分配服务；调用方只提交 SpaceId/value 请求，不接触页 0 或 MTR。
     *
     * @return 已打开引擎的自增 facade
     */
    public AutoIncrementService autoIncrementService() {
        requireOpen();
        return autoIncrementService;
    }

    /**
     * 返回 {@code transactionManager} 对应的数据库引擎组合根受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     *
     * @return {@code transactionManager} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     */
    public TransactionManager transactionManager() {
        requireOpen();
        return transactionManager;
    }

    /**
     * 返回 {@code diskSpaceManager} 对应的数据库引擎组合根受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     *
     * @return {@code diskSpaceManager} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public DiskSpaceManager diskSpaceManager() {
        requireOpen();
        return diskSpaceManager;
    }

    /** Off-page TEXT/BLOB/JSON 页链门面；调用方必须用本 engine 的 MiniTransactionManager 开启 MTR。
     *
     * @return {@code lobStorage} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public LobStorage lobStorage() {
        requireOpen();
        return lobStorage;
    }

    /**
     * 返回 {@code btreeService} 对应的数据库引擎组合根受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     *
     * @return {@code btreeService} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    public SplitCapableBTreeIndexService btreeService() {
        requireOpen();
        return btreeService;
    }

    /**
     * 按数据库引擎组合根并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @return {@code lockManager} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     */
    public LockManager lockManager() {
        requireOpen();
        return lockManager;
    }

    /**
     * 返回 {@code btreeCurrentReadService} 对应的数据库引擎组合根受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     *
     * @return {@code btreeCurrentReadService} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public BTreeCurrentReadService btreeCurrentReadService() {
        requireOpen();
        return btreeCurrentReadService;
    }

    /**
     * 返回聚簇 DML 内核 facade。SQL gateway 通过它创建 statement guard 和执行事务终态；普通 INSERT 写入由
     * {@link #tableDmlService()} 先编排全部 secondary，再复用本入口的聚簇 undo anchor。低层单聚簇测试仍可显式调用。
     * @return {@code dmlService} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public ClusteredDmlService dmlService() {
        requireOpen();
        return dmlService;
    }

    /**
     * 返回生产组合根接线的表级多索引 DML facade。
     *
     * @return 与 {@link #dmlService()} 共享 transaction、undo、lock、B+Tree、redo 和类型 registry 的表级入口。
     * @throws EngineStateException 引擎尚未打开、已经关闭或 recovery gate 尚未开放时抛出。
     */
    public TableDmlService tableDmlService() {
        requireOpen();
        return tableDmlService;
    }

    /**
     * 采集当前 row-lock 诊断快照。数据流为：从生产共享 {@link LockManager} 复制只读锁表/等待图快照，
     * 再交给 server.lockobs 适配成 `data_locks` / `data_lock_waits` 行；本方法不授锁、不释放锁，也不访问
     * B+Tree page latch 或 BufferFrame。
     *
     * @param request 快照请求，不能为 null。
     * @return 不可变诊断快照。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public LockDiagnosticSnapshot lockDiagnosticSnapshot(SnapshotRequest request) {
        requireOpen();
        if (request == null) {
            throw new DatabaseValidationException("lock diagnostic snapshot request must not be null");
        }
        return lockObservationService.captureSnapshot(lockManager.snapshot(), request);
    }

    /** 索引页格式化入口；物理 DDL 用它初始化聚簇/二级索引 root，低层页格式测试也可显式调用。
     *
     * @return {@code indexPageAccess} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    public IndexPageAccess indexPageAccess() {
        requireOpen();
        return indexPageAccess;
    }

    /** 物理 CREATE/DROP TABLE 门面；DD/MDL publish 由更上层 {@code DatabaseEngine} 协调。
     * @return {@code tableDdlStorageService} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public TableDdlStorageService tableDdlStorageService() {
        requireOpen();
        return tableDdlStorageService;
    }

    /**
     * 返回 DD DROP 使用的 persistent history barrier。该 API 不暴露 HistoryEntry、undo page 或内部 Condition。
     *
     * @return {@code tablePurgeBarrier} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     */
    public TablePurgeBarrier tablePurgeBarrier() {
        requireOpen();
        return tablePurgeBarrier;
    }

    /**
     * 返回Online DROP使用的提交号/history退休屏障；API不暴露事务counter锁、HistoryEntry或undo page。
     *
     * @return 与当前实例TransactionSystem、HistoryList和purge worker共享owner的稳定屏障
     * @throws EngineStateException 引擎尚未OPEN或已经关闭时抛出
     */
    public IndexRetirementHistoryBarrier indexRetirementHistoryBarrier() {
        requireOpen();
        return indexRetirementHistoryBarrier;
    }

    /**
     * 返回与事务ReadView登记表共享owner的generation屏障；仅供Online shadow ALTER final quiescence。
     *
     * @return 当前OPEN实例的稳定storage API，不暴露TransactionSystem内部集合
     */
    public cn.zhangyis.db.storage.api.ReadViewRetentionBarrier readViewRetentionBarrier() {
        requireOpen();
        return txnSystem;
    }

    /**
     * 仅供组合根注入的 recovery completion hook 在普通 OPEN 发布前取得物理 DDL facade。
     *
     * <p>StorageEngine 在 {@link #open()} 内部仍保持 NEW，直到 DDL/SDI/orphan 收敛、flush 和写闸门封存全部完成；
     * 因而不能复用公开 {@link #tableDdlStorageService()}。本入口只在组件已经初始化且状态仍为 NEW 时返回，
     * 不开放事务、DML 或用户流量，也不能在 open 调用前猜测可用。</p>
     *
     * @return 当前 open 调用已接线且仍处恢复内部写窗口的物理 DDL facade
     * @throws EngineStateException 不在 recovery completion 初始化窗口时抛出
     */
    public TableDdlStorageService tableDdlStorageServiceForRecoveryCompletion() {
        if (state != EngineState.NEW || tableDdlStorageService == null
                || writeAdmission.mode() != StorageWriteAdmission.Mode.RECOVERY_INTERNAL) {
            throw new EngineStateException(
                    "table DDL recovery facade is unavailable: state=" + state);
        }
        return tableDdlStorageService;
    }

    /**
     * 仅供 recovery completion hook 读取已经由事务恢复重建的 persistent history barrier。
     *
     * @return 与本次 open 的 rollback/purge history 共享 owner 的表级屏障
     * @throws EngineStateException recovery 内部窗口尚未初始化或已经结束时抛出
     */
    public TablePurgeBarrier tablePurgeBarrierForRecoveryCompletion() {
        if (state != EngineState.NEW || tablePurgeBarrier == null
                || writeAdmission.mode() != StorageWriteAdmission.Mode.RECOVERY_INTERNAL) {
            throw new EngineStateException(
                    "table purge recovery barrier is unavailable: state=" + state);
        }
        return tablePurgeBarrier;
    }

    /**
     * 仅供DDL recovery completion hook取得已由事务恢复重建的索引退休屏障。
     *
     * @return 当前启动实例的transaction high-water/history组合屏障
     * @throws EngineStateException 不在RECOVERY_INTERNAL completion窗口时抛出
     */
    public IndexRetirementHistoryBarrier indexRetirementHistoryBarrierForRecoveryCompletion() {
        if (state != EngineState.NEW || indexRetirementHistoryBarrier == null
                || writeAdmission.mode() != StorageWriteAdmission.Mode.RECOVERY_INTERNAL) {
            throw new EngineStateException(
                    "index retirement recovery barrier is unavailable: state=" + state);
        }
        return indexRetirementHistoryBarrier;
    }

    /**
     * startup completion hook在流量开放前取得同一ReadView屏障；此时通常没有live view，但仍复用相同接口。
     *
     * @return 已构造且尚未发布OPEN的TransactionSystem屏障
     */
    public cn.zhangyis.db.storage.api.ReadViewRetentionBarrier
    readViewRetentionBarrierForRecoveryCompletion() {
        if (state != EngineState.NEW || txnSystem == null) {
            throw new EngineStateException(
                    "recovery completion read-view barrier requires initialized NEW engine");
        }
        return txnSystem;
    }

    /**
     * 仅供 recovery completion hook 构造 Online ADD INDEX candidate/manifest 解码器；registry 已在 open 初始化，
     * 但普通用户 accessor 此时仍必须保持关闭。
     *
     * @return 本次 StorageEngine open 的唯一只读类型 registry
     */
    public TypeCodecRegistry typeCodecRegistryForRecoveryCompletion() {
        if (state != EngineState.NEW || typeRegistry == null
                || writeAdmission.mode() != StorageWriteAdmission.Mode.RECOVERY_INTERNAL) {
            throw new EngineStateException(
                    "type registry recovery accessor is unavailable: state=" + state);
        }
        return typeRegistry;
    }

    /**
     * 返回 {@code undoLogManager} 对应的数据库引擎组合根受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     *
     * @return {@code undoLogManager} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    public UndoLogManager undoLogManager() {
        requireOpen();
        return undoLogManager;
    }

    /** 内存 rseg slot 目录（0.3：claim/release 持久到 page3，恢复期由 page3 重建）。
     *
     * @return {@code rollbackSegmentSlotManager} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    public RollbackSegmentSlotManager rollbackSegmentSlotManager() {
        requireOpen();
        return rollbackSlots;
    }

    /**
     * 配置本 engine 的兼容单聚簇索引（主要供不经 DD 组合根的低层测试），**必须在 {@link #open()} 之前**调用。
     * 同时服务恢复期回滚（R 1.2）与后台 purge driver（0.4）。未配置时不启动 purge；existing-open 仅在
     * 未发现 ACTIVE undo 时可继续，发现 ACTIVE 则 fail-closed，绝不以缺少索引为由跳过恢复回滚。
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @throws EngineStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void configureClusteredIndex(BTreeIndex clusteredIndex) {
        if (state == EngineState.OPEN) {
            throw new EngineStateException("clustered index must be configured before open(): " + state);
        }
        this.clusteredIndex = clusteredIndex;
    }

    /**
     * 配置 DD 索引解析器，必须早于 open/recovery。ACTIVE undo rollback 与后台 purge 会逐条读取 undo identity，
     * 解析器返回错误/缺失索引时启动 fail-closed，绝不回退 legacy 全局索引。
     *
     * @param resolver 由组合根提供的 {@code IndexMetadataResolver} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code configureIndexMetadataResolver} 调用
     * @throws EngineStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void configureIndexMetadataResolver(IndexMetadataResolver resolver) {
        if (state != EngineState.NEW) {
            throw new EngineStateException("index metadata resolver must be configured before open(): " + state);
        }
        if (resolver == null) {
            throw new DatabaseValidationException("index metadata resolver must not be null");
        }
        this.indexMetadataResolver = resolver;
    }

    /**
     * 配置后台存储致命失败的公共组合根通知，只能在 open 前设置。回调只负责发布外层生命周期状态，不能在
     * worker 线程递归关闭 StorageEngine；存储写闸门与 recovery gate 会在调用它之前先行关闭。
     *
     * @param handler 后台 fatal 通知端口；不得为 {@code null}，生命周期须覆盖当前 StorageEngine
     * @throws EngineStateException 引擎不再处于 NEW 时抛出，调用方不得在 worker 启动后替换失败所有权
     * @throws DatabaseValidationException handler 为空时抛出
     */
    public void configureBackgroundFatalFailureHandler(Consumer<DatabaseFatalException> handler) {
        if (state != EngineState.NEW) {
            throw new EngineStateException(
                    "background fatal failure handler must be configured before open(): " + state);
        }
        if (handler == null) {
            throw new DatabaseValidationException("background fatal failure handler must not be null");
        }
        this.backgroundFatalFailureHandler = handler;
    }

    /**
     * 定位并读取数据库引擎组合根领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @return {@code mvccReader} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public MvccReader mvccReader() {
        requireOpen();
        return mvccReader;
    }

    /**
     * 返回组合根持有的 unique point/non-unique logical-prefix 二级回表 MVCC 服务；
     * 调用方必须保持 ReadView 覆盖结果 hydration/投影。
     *
     * @return 与本引擎 B+Tree、MvccReader 和 type registry 共用协作者的读服务。
     */
    public SecondaryMvccReader secondaryMvccReader() {
        requireOpen();
        return secondaryMvccReader;
    }

    /**
     * 返回 non-unique secondary logical-prefix current-read 服务；调用方必须以事务终态释放其 predicate/row 锁。
     *
     * @return 与本引擎 TransactionManager、B+Tree、LockManager 和 type registry 共用协作者的锁定读入口。
     */
    public SecondaryCurrentReadService secondaryCurrentReadService() {
        requireOpen();
        return secondaryCurrentReadService;
    }

    /**
     * 校验当前状态后推进数据库引擎组合根状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @return {@code rollbackService} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     */
    public RollbackService rollbackService() {
        requireOpen();
        return rollbackService;
    }

    /**
     * 返回与 DML、undo、redo、事务锁和 recovery gate 共用组合根的 prepared transaction facade。
     *
     * @return storage resource-manager phase-one/phase-two 稳定入口
     * @throws EngineStateException 引擎尚未完成打开或已经关闭时抛出
     */
    public PreparedTransactionService preparedTransactionService() {
        requireOpen();
        return preparedTransactionService;
    }

    /**
     * 返回 DML、XA 与 Online ADD INDEX coordinator 共用的唯一 table gate。
     *
     * @return 当前 OPEN 引擎实例拥有的 gate；调用方不得关闭或替换它
     * @throws EngineStateException 引擎尚未完成打开或已经关闭时抛出
     */
    public OnlineDdlTableGate onlineDdlTableGate() {
        requireOpen();
        return onlineDdlTableGate;
    }

    /**
     * 返回 record、B+Tree、DML candidate codec 共用的 immutable 类型与 collation registry。
     *
     * @return 当前 OPEN 引擎组合根持有的只读 registry
     */
    public TypeCodecRegistry typeCodecRegistry() {
        requireOpen();
        return typeRegistry;
    }

    /** 返回与后台 driver/recovery 共用的 purge 协调器；只有配置可解析索引 metadata 时可用。
     *
     * @return {@code purgeCoordinator} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     * @throws EngineStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public PurgeCoordinator purgeCoordinator() {
        requireOpen();
        if (purgeCoordinator == null) {
            throw new EngineStateException("purge coordinator requires configured index metadata");
        }
        return purgeCoordinator;
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
     * 生成 recovery control-plane 诊断快照。该入口不要求引擎处于 OPEN：READ_ONLY_VALIDATE、FAILED recovery
     * 或启动失败后的上层诊断都需要读取 gate/report/progress。快照只复制不可变值，不暴露 gate/journal 可变对象。
     *
     * @return {@code recoveryDiagnostics} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public RecoveryDiagnosticsSnapshot recoveryDiagnostics() {
        Optional<RecoveryReport> report = Optional.ofNullable(lastRecoveryReport);
        if (report.isEmpty() && crashRecoveryService != null) {
            report = crashRecoveryService.lastReport();
        }
        Optional<String> failure = recoveryGate.lastFailure().map(StorageEngine::describeFailure);
        if (failure.isEmpty() && crashRecoveryService != null) {
            failure = crashRecoveryService.lastError().map(StorageEngine::describeFailure);
        }
        return new RecoveryDiagnosticsSnapshot(recoveryGate.state(), report,
                failure, recoveryProgressJournal.snapshot());
    }

    /**
     * 后台 page cleaner 当前状态。该查询不要求 engine OPEN：close 后测试/诊断需要确认 worker 已 STOPPED；
     * 如果配置禁用了后台 worker，则返回 NEW 表示未构造线程。
     */
    public PageCleanerState pageCleanerState() {
        return pageCleanerSupervisor == null ? PageCleanerState.NEW : pageCleanerSupervisor.state();
    }

    /** 后台 redo flusher 状态（诊断用）；未启动返回 NEW。 */
    public RedoFlushWorkerState redoFlushWorkerState() {
        return redoFlushWorker == null ? RedoFlushWorkerState.NEW : redoFlushWorker.state();
    }

    /** 最近一轮后台 flush/checkpoint tick 结果；禁用后台 worker 或尚未执行过 tick 时为空。 */
    public Optional<FlushCycleResult> lastBackgroundFlushCycle() {
        return pageCleanerSupervisor == null ? Optional.empty() : pageCleanerSupervisor.lastCycle();
    }

    /** 后台 page cleaner supervisor metrics；未启动时返回 NEW 状态的空快照。
     *
     * @return {@code pageCleanerMetrics} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public PageCleanerMetricsSnapshot pageCleanerMetrics() {
        if (pageCleanerSupervisor == null) {
            return new PageCleanerMetricsSnapshot(PageCleanerState.NEW, 0, 0, 0, false, "", 0, 0);
        }
        return pageCleanerSupervisor.metricsSnapshot();
    }

    /**
     * 采集 Change Buffer 持久边界与运行期计数。该查询只复制 header/segment/原子统计，不加载任何用户
     * 二级 leaf，也不会触发 merge；READ_ONLY_VALIDATE 可安全调用，关闭或失败 open 后拒绝访问已释放页句柄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验引擎处于 OPEN 或 READ_ONLY；legacy 无 system.ibd 直接返回 effective NONE 空快照。</li>
     *     <li>独立只读 MTR 读取 page3 configured mode 与 pending，返回前释放 header latch/fix。</li>
     *     <li>另一只读 MTR 检查 leaf/non-leaf segment 已用页并加稳定 root，避免 page3→page2 逆序锁页。</li>
     *     <li>合并弱一致事件计数、bitmap 观察数和 worker 状态，发布不含内部 Guard 的不可变快照。</li>
     * </ol>
     *
     * @return 当前 Change Buffer 控制面快照；成功时不为 {@code null}
     * @throws EngineStateException 引擎未完成启动或已经关闭时抛出
     * @throws ChangeBufferStateException 持久 segment 页数溢出或格式不一致时抛出
     */
    public ChangeBufferSnapshot changeBufferSnapshot() {
        // 1、诊断只允许句柄仍存活的两个稳定状态；legacy 不能读取不存在的 SpaceId 0。
        if (state != EngineState.OPEN && state != EngineState.READ_ONLY) {
            throw new EngineStateException("change buffer snapshot requires OPEN/READ_ONLY state: " + state);
        }
        if (!changeBufferAvailable) {
            return new ChangeBufferSnapshot(false, config.changeBufferConfig().mode(),
                    cn.zhangyis.db.storage.changebuffer.ChangeBufferMode.NONE, 0L,
                    changeBufferCounters.snapshot(), 0L, 0L,
                    cn.zhangyis.db.storage.changebuffer.ChangeBufferWorkerState.NEW);
        }

        // 2、header 与 segment inode 分开读取，避免先持 page3 再访问较低 page2 违反 MTR 物理页全序。
        ChangeBufferHeaderSnapshot header = miniTransactionManager.executeDetached(
                RedoAppendBudget.readOnly(), changeBufferHeaders::read);

        // 3、两个 segment 的 usedPageCount 是 FSP 权威分配证据；固定 page4 root 不属于任一 segment，单独加一。
        long systemTreePages = miniTransactionManager.executeDetached(RedoAppendBudget.readOnly(), read -> {
            long leafPages = diskSpaceManager.inspectDropSegmentPlan(read, header.leafSegment()).usedPageCount();
            long nonLeafPages = diskSpaceManager.inspectDropSegmentPlan(read, header.nonLeafSegment()).usedPageCount();
            try {
                return Math.addExact(1L, Math.addExact(leafPages, nonLeafPages));
            } catch (ArithmeticException overflow) {
                throw new ChangeBufferStateException("change buffer system tree page count overflow", overflow);
            }
        });

        // 4、LongAdder/ConcurrentHashMap 读只提供诊断弱一致性，不能反向参与 header 或恢复决策。
        return new ChangeBufferSnapshot(true, header.configuredMode(), effectiveChangeBufferConfig.mode(),
                header.pendingOperations(), changeBufferCounters.snapshot(), systemTreePages,
                changeBufferBitmaps.observedBitmapPageCount(), changeBufferMergeWorker.state());
    }

    /**
     * 返回 purge 驱动自动 undo 截断的原子观测快照。查询不要求 engine 已 OPEN；构造前返回与配置一致的
     * NEVER_RUN/DISABLED 初始状态，close 后仍保留最后完成 epoch 或失败诊断。
     *
     * @return 自洽的候选检查、延期、完成、失败与累计回收页统计
     */
    public UndoTruncationMetricsSnapshot undoTruncationMetrics() {
        if (undoTruncationScheduler == null) {
            return UndoTruncationMetricsSnapshot.initial(config.undoTruncationConfig().enabled());
        }
        return undoTruncationScheduler.metricsSnapshot();
    }

    /**
     * 等待后台 worker 清空显式请求并离开正在执行的 cycle。禁用后台 worker 时直接返回 true；超时语义和
     * {@link PageCleanerWorker#awaitIdle(Duration)} 一致。
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 或负值，零表示只做一次立即检查而不阻塞
     * @return 在超时或取消前观察到 {@code awaitBackgroundFlushIdle} 的目标状态时为 {@code true}；等待期限届满且状态仍未满足时为 {@code false}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public boolean awaitBackgroundFlushIdle(Duration timeout) {
        if (timeout == null) {
            throw new DatabaseValidationException("background flush await timeout must not be null");
        }
        return pageCleanerSupervisor == null || pageCleanerSupervisor.awaitIdle(timeout);
    }

    /**
     * 校验 {@code requireOpen} 涉及的数据库引擎组合根结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @throws EngineStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    private void requireOpen() {
        if (state != EngineState.OPEN) {
            throw new EngineStateException("engine not OPEN: " + state);
        }
        RecoveryState recoveryState = recoveryGate.state();
        if (recoveryState != RecoveryState.OPEN) {
            throw new EngineStateException("engine recovery gate not OPEN: " + recoveryState);
        }
    }

    /**
     * 释放本方法拥有的数据库引擎组合根资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     *
     * @param handle 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param errors 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
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

    private static String describeFailure(Throwable failure) {
        return failure.getClass().getSimpleName()
                + (failure.getMessage() == null || failure.getMessage().isBlank()
                ? "" : ": " + failure.getMessage());
    }
}
