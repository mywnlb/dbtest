package cn.zhangyis.db.engine;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.engine.adapter.DictionaryIndexMetadataResolver;
import cn.zhangyis.db.engine.adapter.DictionaryStorageMetadataMapper;
import cn.zhangyis.db.engine.adapter.DefaultSqlStorageGateway;
import cn.zhangyis.db.engine.adapter.DefaultSqlDdlGateway;
import cn.zhangyis.db.dd.ddl.DictionaryDdlService;
import cn.zhangyis.db.dd.ddl.OnlineDdlControlService;
import cn.zhangyis.db.dd.ddl.OnlineDdlOperationRegistry;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.recovery.DictionaryDdlRecoveryService;
import cn.zhangyis.db.dd.ddl.OnlineIndexBuildRuntime;
import cn.zhangyis.db.dd.recovery.OnlineIndexRecoveryRuntime;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryIsolationPlanner;
import cn.zhangyis.db.dd.recovery.RecoveryIsolationPlan;
import cn.zhangyis.db.dd.recovery.DictionaryRecoveryManifestRepository;
import cn.zhangyis.db.dd.recovery.DictionaryRecoverySnapshotPublisher;
import cn.zhangyis.db.dd.recovery.DictionaryTablespaceDiscovery;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.DictionaryControlSnapshot;
import cn.zhangyis.db.dd.repo.DictionaryIdRequest;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.engine.EngineConfig;
import cn.zhangyis.db.storage.engine.EngineTablespaceConfig;
import cn.zhangyis.db.storage.engine.StorageEngine;
import cn.zhangyis.db.storage.fil.online.OnlineIndexChangeLogFiles;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlAbortReason;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import cn.zhangyis.db.common.exception.RecoveryExportWriteRejectedException;
import cn.zhangyis.db.storage.fil.catalog.FileInternalCatalogStore;
import cn.zhangyis.db.storage.fil.catalog.FileDictionaryRecoveryManifestStore;
import cn.zhangyis.db.engine.recovery.DatabaseInstanceFileLock;
import cn.zhangyis.db.engine.recovery.RecoveryUnavailableTable;
import cn.zhangyis.db.engine.xa.FileXaRegistry;
import cn.zhangyis.db.engine.xa.PersistentXaCoordinator;
import cn.zhangyis.db.storage.recovery.PreparedTransactionDecisionProvider;
import lombok.extern.slf4j.Slf4j;
import cn.zhangyis.db.session.DefaultSqlSession;
import cn.zhangyis.db.session.SessionId;
import cn.zhangyis.db.session.SessionOptions;
import cn.zhangyis.db.session.SessionRegistry;
import cn.zhangyis.db.session.SqlSession;
import cn.zhangyis.db.sql.binder.DefaultSqlBinder;
import cn.zhangyis.db.sql.binder.SqlTypeCoercion;
import cn.zhangyis.db.sql.executor.DefaultSqlExecutor;
import cn.zhangyis.db.sql.optimizer.DefaultSqlStatementCompiler;
import cn.zhangyis.db.sql.optimizer.HeuristicQueryOptimizer;
import cn.zhangyis.db.sql.optimizer.SqlStatementCompiler;
import cn.zhangyis.db.sql.optimizer.logical.BoundToLogicalConverter;
import cn.zhangyis.db.sql.parser.DefaultSqlParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 数据库实例组合根：先从持久 DD discovery 用户表空间，再启动 StorageEngine crash recovery，最后续作 DDL recovery
 * 并发布 dictionary/DDL facade。上层不再需要手工拼接 {@code recoveryTablespaces} 或持有 catalog 实现。
 *
 * <p>教学简化：当前 {@code mysql.ibd} 使用 page-aligned append-only catalog adapter，而非真正的内部 DD B+Tree；
 * Repository/Facade 边界已经稳定，后续可替换后端而不改变 SQL/session 或物理 DDL 协议。
 */
@Slf4j
public final class DatabaseEngine implements AutoCloseable {

    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据库引擎组合根的不变量。
     */
    private static final SpaceId DICTIONARY_SPACE_ID = SpaceId.of(1);
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏数据库引擎组合根的不变量。
     */
    private static final int FIRST_USER_SPACE_ID = 1024;
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
    private static final int DICTIONARY_CACHE_CAPACITY = 256;

    /** 只保护组合根 open/close 状态，不参与任何页、MDL 或事务操作。 */
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    /** 序列化可能跨 lifecycle lock 外等待 Session 的 close 尝试；使用有界 tryLock。 */
    private final ReentrantLock closeWorkLock = new ReentrantLock(true);
    /**
     * 构造时冻结的 {@code baseConfig} 配置快照；已完成范围和组合校验，运行期策略读取它但不得就地修改。
     */
    private final EngineConfig baseConfig;
    /** 上层协调器持久 XA 决议端口；默认 unresolved，使未知 PREPARED 启动 fail-closed。 */
    private final PreparedTransactionDecisionProvider preparedDecisionProvider;
    /** 用户语句并发 read / shutdown write gate；不串行不同 Session。 */
    private final EngineSessionExecutionGate sessionExecutionGate;

    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private volatile DatabaseEngineState state = DatabaseEngineState.NEW;
    /**
     * 本对象持有的 {@code catalogStore} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private FileInternalCatalogStore catalogStore;
    /** 与 catalog 独立 magic 的恢复 manifest journal；必须晚于 control/catalog 关闭。 */
    private FileDictionaryRecoveryManifestStore manifestStore;
    /** manifest 事件语义与 durability witness；生命周期由 manifestStore 支撑。 */
    private DictionaryRecoveryManifestRepository manifestRepository;
    /** 从 open 前持有到全部资源关闭后的跨进程实例独占锁。 */
    private DatabaseInstanceFileLock instanceFileLock;
    /** instance lock 覆盖下打开的 append-only XID registry；必须晚于 storage、早于 instance lock 关闭。 */
    private FileXaRegistry xaRegistry;
    /** 共享 live prepared branch coordinator；Session 只依赖其稳定端口。 */
    private PersistentXaCoordinator xaCoordinator;
    /**
     * 本对象持有的 {@code controlStore} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private DictionaryControlStore controlStore;
    /**
     * 本对象持有的 {@code repository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private PersistentDictionaryRepository repository;
    /**
     * 本对象持有的 {@code cache} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private DictionaryObjectCache cache;
    /**
     * 本对象持有的 {@code metadataLocks} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private MetadataLockManager metadataLocks;
    /**
     * 本对象持有的 {@code storage} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private StorageEngine storage;
    /**
     * 本对象持有的 {@code dictionary} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private DataDictionaryService dictionary;
    /**
     * 本对象持有的 {@code ddl} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private DictionaryDdlService ddl;
    /** live/recovery/control 共享的唯一 Online DDL 轻量 registry；不拥有业务资源。 */
    private OnlineDdlOperationRegistry onlineDdlRegistry;
    /** 只读诊断与 durable cancel CAS 的稳定 Java/admin facade。 */
    private OnlineDdlControlService onlineDdlControl;
    /**
     * 本对象拥有的 {@code sqlMetadataMapper} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private DictionaryStorageMetadataMapper sqlMetadataMapper;
    /**
     * 本对象持有的 {@code sqlParser} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private DefaultSqlParser sqlParser;
    /**
     * 本对象持有的 {@code sqlBinder} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private DefaultSqlBinder sqlBinder;
    /**
     * 实例级无状态 SQL 编译 pipeline；由组合根选择具体 optimizer，所有 Session 只依赖稳定编译接口。
     */
    private SqlStatementCompiler sqlCompiler;
    /**
     * 本对象持有的 {@code sessions} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private SessionRegistry sessions;
    /** 启动完成后稳定发布的访问能力；OPEN 之前保持 NORMAL 占位且不对外可读。 */
    private volatile DatabaseAccessMode accessMode = DatabaseAccessMode.NORMAL;
    /** committed DD 中全部恢复隔离对象的不可变诊断快照。 */
    private volatile List<RecoveryUnavailableTable> unavailableTables = List.of();

    /**
     * 创建 {@code DatabaseEngine}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param config 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    public DatabaseEngine(EngineConfig config) {
        this(config, PreparedTransactionDecisionProvider.unresolved());
    }

    /**
     * 构造可消费持久 prepared transaction 决议的公共数据库组合根。
     *
     * @param config 数据库与底层存储配置
     * @param preparedDecisionProvider 上层 XID registry 的只读决议端口；不能为 null
     * @throws DatabaseValidationException 配置、provider 缺失或 undo/dictionary space 冲突时抛出
     */
    public DatabaseEngine(EngineConfig config,
                          PreparedTransactionDecisionProvider preparedDecisionProvider) {
        if (config == null) {
            throw new DatabaseValidationException("database engine config must not be null");
        }
        if (preparedDecisionProvider == null) {
            throw new DatabaseValidationException(
                    "database engine prepared decision provider must not be null");
        }
        if (config.undoSpaceId().equals(DICTIONARY_SPACE_ID)) {
            throw new DatabaseValidationException("undo space id conflicts with dictionary space id");
        }
        this.baseConfig = config;
        this.preparedDecisionProvider = preparedDecisionProvider;
        this.sessionExecutionGate = new EngineSessionExecutionGate(this::state, this::failClosed);
    }

    /**
     * 取得实例独占权后完成 catalog/manifest、storage recovery、DDL recovery 与 Session 门面的统一启动。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建实例目录并取得跨进程 instance lock；失败时尚未打开任何数据库持久文件。</li>
     *     <li>执行 catalog-loss admission，严格打开 catalog/manifest/control，注入写前 witness 并校正
     *     identity high-water；missing/empty catalog 有持久证据时在创建空字典前 fail-closed。</li>
     *     <li>从 committed DD binding discovery 表空间，启动 StorageEngine 完成 doublewrite、redo、undo、
     *     PREPARED 决议与 purge resume；失败时普通流量 gate 尚未发布。</li>
     *     <li>恢复 DDL marker、SDI 与 orphan，随后在 repository writer fence 内发布 clean manifest。</li>
     *     <li>最后构造 DDL/SQL/Session facade 并发布 OPEN；任一异常逆序关闭已创建资源，instance lock 最后释放。</li>
     * </ol>
     *
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
    public void open() {
        lifecycleLock.lock();
        try {
            if (state != DatabaseEngineState.NEW) {
                throw new DatabaseRuntimeException("database engine cannot open from state: " + state);
            }
            state = DatabaseEngineState.OPENING;
            try {
                // 1. instance lock 覆盖本次启动和后续完整服务期，阻止离线 recovery 与普通引擎并发。
                ensureBaseDirectory();
                instanceFileLock = DatabaseInstanceFileLock.acquire(
                        baseConfig.baseDir(), baseConfig.flushTimeout());
                xaRegistry = FileXaRegistry.openOrCreate(baseConfig.baseDir());
                // 2. 只有 catalog admission 成功后才允许打开/修复 manifest，并把它注入 control/repository。
                Path catalogPath = baseConfig.baseDir().resolve("mysql.ibd");
                Path controlPath = baseConfig.baseDir().resolve("mysql.dd.ctrl");
                Path manifestPath = baseConfig.baseDir().resolve("mysql.dd.manifest");
                Path tablesDirectory = baseConfig.baseDir().resolve("tables");
                catalogStore = CatalogBootstrapAdmission.openCatalog(
                        baseConfig, catalogPath, controlPath, tablesDirectory);
                initializeRecoveryManifest(manifestPath);
                controlStore = DictionaryControlStore.openOrCreate(controlPath, DICTIONARY_SPACE_ID,
                        FIRST_USER_SPACE_ID, manifestRepository);
                repository = new PersistentDictionaryRepository(catalogStore, manifestRepository);
                reconcileControlHighWater();
                // 3. FORCE 先把整组 SpaceId 原子固化为 DD 隔离状态；普通启动也从 DD 重建长期排除集合。
                RecoveryIsolationPlan isolationPlan = new DictionaryRecoveryIsolationPlanner(
                        controlStore, repository, DICTIONARY_SPACE_ID, baseConfig.undoSpaceId(), tablesDirectory)
                        .plan(baseConfig.recoveryMode(), baseConfig.forceSkippedSpaces());
                unavailableTables = isolationPlan.unavailableTables();
                accessMode = determineAccessMode(baseConfig, unavailableTables);
                cache = new DictionaryObjectCache(DICTIONARY_CACHE_CAPACITY);
                metadataLocks = new MetadataLockManager();
                onlineDdlRegistry = new OnlineDdlOperationRegistry(DICTIONARY_CACHE_CAPACITY);
                dictionary = new DataDictionaryService(repository, cache, metadataLocks);
                DictionaryRecoverySnapshotPublisher cleanSnapshotPublisher =
                        new DictionaryRecoverySnapshotPublisher(
                                repository, controlStore, manifestRepository, tablesDirectory);
                // 隔离提交后才允许 discovery；RECOVERY_UNAVAILABLE/RECOVERY_DISCARDED 永不进入用户文件打开列表。
                DictionaryTablespaceDiscovery discovery =
                        new DictionaryTablespaceDiscovery(repository, tablesDirectory);
                List<EngineTablespaceConfig> tablespaces = discovery.discover().stream()
                        .map(binding -> new EngineTablespaceConfig(binding.spaceId(), binding.path()))
                        .toList();
                PreparedTransactionDecisionProvider compositePreparedDecisions = transactionId ->
                        xaRegistry.containsTransaction(transactionId)
                                ? xaRegistry.decisionFor(transactionId)
                                : preparedDecisionProvider.decisionFor(transactionId);
                storage = new StorageEngine(
                        baseConfig.withRecoveryTablespaces(tablespaces), compositePreparedDecisions,
                        isolationPlan.exclusionPolicy(), () -> {
                            new DictionaryDdlRecoveryService(controlStore, repository, cache,
                                    storage.tableDdlStorageServiceForRecoveryCompletion(), tablesDirectory,
                                    storage.tablePurgeBarrierForRecoveryCompletion(),
                                    new OnlineIndexRecoveryRuntime(
                                            baseConfig.onlineDdlConfig(),
                                             new OnlineIndexChangeLogFiles(
                                                     baseConfig.onlineDdlDirectory(),
                                                     baseConfig.onlineDdlConfig()),
                                             storage.typeCodecRegistryForRecoveryCompletion()),
                                     onlineDdlRegistry,
                                    new cn.zhangyis.db.dd.ddl.DefaultIndexRetirementBarrier(
                                            storage.indexRetirementHistoryBarrierForRecoveryCompletion(),
                                            cache),
                                    new cn.zhangyis.db.dd.recovery.OnlineAlterRecoveryRuntime(
                                            new cn.zhangyis.db.storage.fil.online.OnlineAlterChangeLogFiles(
                                                    baseConfig.onlineDdlDirectory(),
                                                    baseConfig.onlineDdlConfig())),
                                    new cn.zhangyis.db.dd.ddl.DefaultOnlineAlterRetirementBarrier(
                                            storage.indexRetirementHistoryBarrierForRecoveryCompletion(),
                                            storage.tablePurgeBarrierForRecoveryCompletion(), cache))
                                     .recover(baseConfig.flushTimeout());
                            cleanSnapshotPublisher.publish();
                        });
                storage.configureIndexMetadataResolver(new DictionaryIndexMetadataResolver(repository));
                storage.configureBackgroundFatalFailureHandler(this::failClosed);
                storage.open();
                xaRegistry.completeRecoveryDecisions();
                xaCoordinator = new PersistentXaCoordinator(xaRegistry);

                // 4. DDL/SDI/orphan 与 clean publish 已在 StorageEngine completion hook 内、worker/写闸门发布前完成。
                ddl = new DictionaryDdlService(controlStore, repository, cache, metadataLocks,
                        storage.tableDdlStorageService(), tablesDirectory, storage.tablePurgeBarrier(),
                        cn.zhangyis.db.dd.ddl.DictionaryDdlFaultInjector.NO_OP,
                        cleanSnapshotPublisher,
                        new OnlineIndexBuildRuntime(storage.onlineDdlTableGate(),
                                 baseConfig.onlineDdlConfig(),
                                 new OnlineIndexChangeLogFiles(
                                         baseConfig.onlineDdlDirectory(), baseConfig.onlineDdlConfig()),
                                 storage.typeCodecRegistry()), onlineDdlRegistry,
                        new cn.zhangyis.db.dd.ddl.DefaultIndexRetirementBarrier(
                                storage.indexRetirementHistoryBarrier(), cache),
                        new cn.zhangyis.db.dd.ddl.OnlineAlterRuntime(
                                storage.onlineDdlTableGate(), baseConfig.onlineDdlConfig(),
                                new cn.zhangyis.db.storage.fil.online.OnlineAlterChangeLogFiles(
                                        baseConfig.onlineDdlDirectory(),
                                        baseConfig.onlineDdlConfig()),
                                storage.typeCodecRegistry(),
                                storage.readViewRetentionBarrier()),
                        new cn.zhangyis.db.dd.ddl.DefaultOnlineAlterRetirementBarrier(
                                storage.indexRetirementHistoryBarrier(),
                                storage.tablePurgeBarrier(), cache));
                onlineDdlControl = new OnlineDdlControlService(
                        repository.ddlLog(), onlineDdlRegistry, identity -> {
                    // durable cancel已释放 catalog writer fence；先精确唤醒 pending MDL，再唤醒 gate drain。
                    if (identity.ownerId() > 0) {
                        metadataLocks.cancelPending(
                                cn.zhangyis.db.dd.domain.MdlOwnerId.of(identity.ownerId()));
                    }
                    OnlineDdlTablePhase phase = storage.onlineDdlTableGate()
                            .phase(identity.tableId());
                    if (phase != OnlineDdlTablePhase.ABSENT
                            && phase != OnlineDdlTablePhase.ABORTING) {
                        if (identity.operation()
                                == cn.zhangyis.db.dd.ddl.DdlLogOperation.ALTER_TABLE_INPLACE
                                || identity.operation()
                                == cn.zhangyis.db.dd.ddl.DdlLogOperation.REBUILD_TABLE) {
                            // 通用ALTER在gate中由capture id登记；若误用旧build入口，取消虽已durable，
                            // 但正在等待base-copy/final drain的线程不会被唤醒。
                            storage.onlineDdlTableGate().beginAbort(
                                    cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId.of(
                                            identity.ddlId().value()),
                                    OnlineDdlAbortReason.CANCELLED);
                        } else {
                            storage.onlineDdlTableGate().beginAbort(
                                    OnlineIndexBuildId.of(identity.ddlId().value()),
                                    OnlineDdlAbortReason.CANCELLED);
                        }
                    }
                });
                // 5. SQL/session 是最后发布的用户入口，避免半恢复实例接受语句。
                sqlMetadataMapper = new DictionaryStorageMetadataMapper();
                sqlParser = new DefaultSqlParser();
                sqlBinder = new DefaultSqlBinder(new SqlTypeCoercion());
                sqlCompiler = new DefaultSqlStatementCompiler(
                        sqlBinder, new BoundToLogicalConverter(), new HeuristicQueryOptimizer());
                sessions = new SessionRegistry();
                state = DatabaseEngineState.OPEN;
                log.info("database engine opened: tablespaces={} dictionaryVersion={}", tablespaces.size(),
                        repository.snapshot().publishedVersion().value());
            } catch (RuntimeException failure) {
                state = DatabaseEngineState.FAILED;
                closeResources(failure);
                throw failure;
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    /** 返回强制 MDL+pin 租约语义的 DD 读取 facade。
     *
     * @return {@code dictionary} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public DataDictionaryService dictionary() {
        requireOpen();
        return dictionary;
    }

    /** 返回逻辑 DD 与物理 tablespace 的统一 DDL 协调器。
     *
     * @return {@code ddl} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public DictionaryDdlService ddl() {
        requireOpen();
        rejectRecoveryExportFacade("DDL");
        return ddl;
    }

    /**
     * 返回 Online DDL 诊断与管理控制面。该 facade 不暴露 gate、row-log 或 MDL 内部对象；
     * 取消权限仍由 {@code OnlineDdlCancelRequest} 显式声明。
     *
     * @return 与 live coordinator 共享 registry/marker repository 的稳定 Java/admin facade
     */
    public OnlineDdlControlService onlineDdlControl() {
        requireOpen();
        return onlineDdlControl;
    }

    /** 返回已完成 discovery/recovery 的底层存储组合根。
     *
     * @return {@code storage} 创建的模块协作者；成功时不为 {@code null}，其依赖和生命周期由当前组合根拥有
     */
    public StorageEngine storage() {
        requireOpen();
        rejectRecoveryExportFacade("raw storage");
        return storage;
    }

    /** @return 启动完成后稳定的数据库访问模式。 */
    public DatabaseAccessMode accessMode() {
        requireOpen();
        return accessMode;
    }

    /** @return 按 TableId 排序的 committed 恢复隔离对象，不包含可变页或内部句柄。 */
    public List<RecoveryUnavailableTable> unavailableTables() {
        requireOpen();
        return unavailableTables;
    }

    /**
     * 创建一个进程内 SQL Session。构造、初始 implicit transaction 和 registry publish 都在 lifecycle lock 内，
     * 因而 close 一旦切到 CLOSING，后续 openSession 不可能漏进 close 快照。
     *
     * @param options 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code openSession} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SqlSession openSession(SessionOptions options) {
        if (options == null) throw new DatabaseValidationException("session options must not be null");
        lifecycleLock.lock();
        try {
            requireOpen();
            SessionId id = sessions.nextId();
            DefaultSqlStorageGateway gateway = new DefaultSqlStorageGateway(storage, sqlMetadataMapper,
                    options.rowLockTimeout(),
                    accessMode == DatabaseAccessMode.RECOVERY_EXPORT_READ_ONLY);
            DefaultSqlSession session = new DefaultSqlSession(
                    id, options, dictionary, sqlParser, sqlBinder, sqlCompiler,
                    new DefaultSqlExecutor(gateway), gateway, new DefaultSqlDdlGateway(ddl),
                    xaCoordinator,
                    sessionExecutionGate,
                    accessMode == DatabaseAccessMode.RECOVERY_EXPORT_READ_ONLY,
                    () -> sessions.deregister(id));
            sessions.register(session);
            return session;
        } finally {
            lifecycleLock.unlock();
        }
    }

    /** 返回当前组合根生命周期状态，不触发任何 IO。 */
    public DatabaseEngineState state() {
        return state;
    }

    /** 根据请求模式和 commit 后隔离集合计算一次性服务能力，不从瞬时文件存在性推导。 */
    private static DatabaseAccessMode determineAccessMode(
            EngineConfig config, List<RecoveryUnavailableTable> unavailable) {
        if (config.recoveryMode() == cn.zhangyis.db.storage.recovery.RecoveryMode.READ_ONLY_VALIDATE) {
            return DatabaseAccessMode.VALIDATION_READ_ONLY;
        }
        if (config.recoveryMode()
                == cn.zhangyis.db.storage.recovery.RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE) {
            return DatabaseAccessMode.RECOVERY_EXPORT_READ_ONLY;
        }
        return unavailable.isEmpty() ? DatabaseAccessMode.NORMAL : DatabaseAccessMode.DEGRADED;
    }

    /** FORCE 模式只经受限 SQL gateway 读取健康对象，禁止调用方取得可绕过写闸门的底层 facade。 */
    private void rejectRecoveryExportFacade(String facade) {
        if (accessMode == DatabaseAccessMode.RECOVERY_EXPORT_READ_ONLY) {
            throw new RecoveryExportWriteRejectedException(
                    facade + " facade is unavailable in recovery export read-only mode");
        }
    }

    /**
     * 关闭数据库组合根。
     * <ol>
     *     <li>有界取得 close owner 锁，拒绝两个线程同时执行资源终结。</li>
     *     <li>在 lifecycle 短锁内切 CLOSING 并冻结 Session 快照；从此 openSession 和新 execute 均被拒绝。</li>
     *     <li>在 lifecycle 锁外取得 statement gate write permit，有界等待所有已准入 execute 完整退出。</li>
     *     <li>持 write permit 并发关闭快照 Session，使活动事务先 rollback、再释放 transaction-duration MDL/pin。</li>
     *     <li>仅在语句已静默后关闭 StorageEngine 与 DD sidecar，发布 CLOSED；任一 close 异常用 suppressed 聚合。</li>
     * </ol>
     * 若第 1、3、4 步 timeout，保持 CLOSING 且绝不关闭仍可能被执行线程访问的 storage。
     *
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
    @Override
    public void close() {
        // 1. closeWorkLock 只串行 shutdown owner，不保护 storage/session 内部状态。
        boolean closeOwner;
        try {
            closeOwner = closeWorkLock.tryLock(baseConfig.flushTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DatabaseRuntimeException("database close wait interrupted", interrupted);
        }
        if (!closeOwner) throw new DatabaseRuntimeException("database close already in progress beyond timeout");
        try {
            // 2. 状态与 snapshot 同批发布，消除 openSession 漏入关闭快照的窗口。
            List<SqlSession> snapshot;
            lifecycleLock.lock();
            try {
                if (state == DatabaseEngineState.CLOSED) return;
                state = DatabaseEngineState.CLOSING;
                snapshot = sessions == null ? List.of() : sessions.snapshot();
            } finally {
                lifecycleLock.unlock();
            }

            // 3. write permit 覆盖后续 Session rollback 与 storage/DD close；timeout 时不触碰任何下游资源。
            try (var ignored = sessionExecutionGate.awaitQuiescence(baseConfig.flushTimeout())) {
                // 4. 此时不存在持 operationLock 的 execute，Session.close 不会与 gate 形成反向锁序。
                SessionCloseResult sessionResult = closeSessions(snapshot, baseConfig.flushTimeout());
                if (!sessionResult.quiesced()) {
                    throw sessionResult.failure();
                }
                // 5. storage 关闭异常仍继续清 DD sidecar，并在 CLOSED 发布后把聚合错误返回调用方。
                RuntimeException aggregate = closeResources(sessionResult.failure());
                lifecycleLock.lock();
                try { state = DatabaseEngineState.CLOSED; }
                finally { lifecycleLock.unlock(); }
                if (aggregate != null) throw aggregate;
            }
        } finally {
            closeWorkLock.unlock();
        }
    }

    /**
     * 用 virtual thread 并发请求 snapshot 中所有 Session close，共享单一 engine deadline。close 异常只聚合，仍等待
     * 其它 Session；超时/中断则取消等待并返回 quiesced=false，禁止下游资源关闭。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code closeSessions} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code SessionCloseResult} 结果
     * @throws TimeoutException 操作在约定时限内无法完成时抛出；调用方可回滚或稍后重试
     */
    private static SessionCloseResult closeSessions(List<SqlSession> snapshot, java.time.Duration timeout) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        if (snapshot.isEmpty()) return new SessionCloseResult(true, null);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        List<Future<RuntimeException>> futures = new ArrayList<>(snapshot.size());
        for (SqlSession session : snapshot) {
            futures.add(executor.submit(() -> {
                try { session.close(); return null; }
                catch (RuntimeException failure) { return failure; }
            }));
        }
        long deadline = deadline(timeout);
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
        RuntimeException failure = null;
        boolean quiesced = true;
        try {
            for (Future<RuntimeException> future : futures) {
                long remaining = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
                if (remaining <= 0) throw new TimeoutException("session close deadline expired");
                RuntimeException sessionFailure = future.get(remaining, TimeUnit.NANOSECONDS);
                failure = appendFailure(failure, sessionFailure);
            }
        } catch (TimeoutException timeoutFailure) {
            quiesced = false;
            failure = appendFailure(failure,
                    new DatabaseRuntimeException("active sessions did not quiesce before engine close timeout",
                            timeoutFailure));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            quiesced = false;
            failure = appendFailure(failure,
                    new DatabaseRuntimeException("engine session close wait interrupted", interrupted));
        } catch (ExecutionException impossible) {
            failure = appendFailure(failure,
                    new DatabaseRuntimeException("session close task failed outside captured runtime error", impossible));
        } finally {
            if (!quiesced) for (Future<RuntimeException> future : futures) future.cancel(true);
            executor.shutdownNow();
        }
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        return new SessionCloseResult(quiesced, failure);
    }

    /**
     * 逆序释放组合根资源，并把关闭失败追加到原始异常。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先关闭 storage，使后台 worker、page IO 与持久事务协作全部停止。</li>
     *     <li>清除只引用下游资源的 SQL/Session facade 字段，避免失败状态下重新暴露入口。</li>
     *     <li>依次关闭 control、catalog 与 manifest channel；每个失败都进入同一 suppressed 异常图。</li>
     *     <li>所有数据库文件句柄释放后最后关闭 instance lock，随后返回聚合异常或 {@code null}。</li>
     * </ol>
     *
     * @param primary 触发清理的原始失败；正常 close 可为 {@code null}
     * @return 原始/首个关闭异常及其 suppressed 图；全部成功时为 {@code null}
     */
    private RuntimeException closeResources(RuntimeException primary) {
        // 1. 先停 storage，防止其后台任务在 DD 文件关闭后继续回调元数据。
        RuntimeException failure = primary;
        if (storage != null) {
            try {
                storage.close();
            } catch (RuntimeException closeFailure) {
                failure = appendFailure(failure, closeFailure);
            }
        }
        storage = null;
        onlineDdlControl = null;
        onlineDdlRegistry = null;
        xaCoordinator = null;
        // 2. 用户入口不拥有独立资源，但必须在下层 close 后清除引用。
        sqlMetadataMapper = null;
        sqlParser = null;
        sqlBinder = null;
        sqlCompiler = null;
        sessions = null;
        failure = closeOne(xaRegistry, failure);
        xaRegistry = null;
        // 3. witness 的调用方已经停止，按 control/catalog/manifest 顺序关闭持久 DD 文件。
        failure = closeOne(controlStore, failure);
        controlStore = null;
        failure = closeOne(catalogStore, failure);
        catalogStore = null;
        failure = closeOne(manifestStore, failure);
        manifestStore = null;
        manifestRepository = null;
        failure = closeOne(instanceFileLock, failure);
        instanceFileLock = null;
        // 4. instance lock 必须最后释放，避免离线工具观察到半关闭的数据库文件集合。
        return failure;
    }

    /**
     * 任一 Session 观察到 DatabaseFatalException 时立即关闭新 Session/statement 准入。这里只发布状态并记录根因，
     * 不在活动 execute 线程中递归 close/flush；调用方随后显式 close，由 statement gate 先完成 quiescence。
     *
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private void failClosed(cn.zhangyis.db.common.exception.DatabaseFatalException failure) {
        lifecycleLock.lock();
        try {
            if (state == DatabaseEngineState.OPEN) {
                state = DatabaseEngineState.FAILED;
                log.error("database engine entered FAILED after a fail-stop engine error", failure);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * 根据调用参数创建或转换 {@code closeOne} 返回的 {@code RuntimeException}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param resource 调用方打开的定位 IO 或编码写入对象；不得为 {@code null}，方法不接管所有权，失败时仍由创建方关闭
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     * @return {@code closeOne} 分类或包装后的领域异常；成功时不为 {@code null}，原始 cause 与 suppressed 异常关系保持不变
     */
    private static RuntimeException closeOne(AutoCloseable resource, RuntimeException failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
            return failure;
        } catch (Exception closeFailure) {
            RuntimeException wrapped = closeFailure instanceof RuntimeException runtime
                    ? runtime : new DatabaseRuntimeException("close database resource failed", closeFailure);
            return appendFailure(failure, wrapped);
        }
    }

    private static RuntimeException appendFailure(RuntimeException failure, RuntimeException additional) {
        if (additional == null) return failure;
        if (failure == null) {
            return additional;
        }
        failure.addSuppressed(additional);
        return failure;
    }

    private static long deadline(java.time.Duration timeout) {
        long now = System.nanoTime();
        try { return Math.addExact(now, timeout.toNanos()); }
        catch (ArithmeticException overflow) { return Long.MAX_VALUE; }
    }

    /**
     * 封装数据库引擎组合根中 {@code SessionCloseResult} 的槽位、预留或阶段结果；组件在创建时交叉校验，使恢复和释放路径能区分已完成与剩余工作。
     *
     * @param quiesced 资源是否处于删除、空闲、静默、持久化或终态；必须与权威状态机一致，不能由调用方猜测
     * @param failure 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    private record SessionCloseResult(boolean quiesced, RuntimeException failure) { }

    private void ensureBaseDirectory() {
        try {
            Files.createDirectories(baseConfig.baseDir());
        } catch (IOException e) {
            throw new DatabaseRuntimeException("create database base directory failed: " + baseConfig.baseDir(), e);
        }
    }

    /**
     * 从独立文件打开 recovery manifest；仅当 catalog 已经严格打开为权威真相时，允许把损坏 manifest
     * 原子移入 evidence 目录并创建新 journal。catalog 丢失路径在本方法前已由 admission 拒绝，绝不会
     * 用新空 manifest 掩盖恢复证据缺口。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>尝试恢复物理 journal 和全部逻辑 event；成功直接发布 store/repository 字段。</li>
     *     <li>失败先关闭可能已打开的 store，保持 catalog/control/用户文件不变。</li>
     *     <li>将原 manifest 以同盘 ATOMIC_MOVE 保留到唯一 evidence 路径；不支持原子移动则停止启动。</li>
     *     <li>创建全新独立 journal；后续 DDL recovery 完成后才发布首个 clean snapshot。</li>
     * </ol>
     *
     * @param manifestPath 固定 {@code mysql.dd.manifest} 路径
     * @throws DatabaseRuntimeException manifest 证据无法保留或新 journal 无法创建时抛出
     */
    private void initializeRecoveryManifest(Path manifestPath) {
        // 1. 同时验证物理 frame 与逻辑 event，避免只修一半格式。
        try {
            manifestStore = manifestExistsNoFollow(manifestPath)
                    ? FileDictionaryRecoveryManifestStore.openExisting(manifestPath)
                    : FileDictionaryRecoveryManifestStore.openOrCreate(manifestPath);
            manifestRepository = new DictionaryRecoveryManifestRepository(manifestStore);
            return;
        } catch (RuntimeException corruption) {
            // 2. 物理 store 可能已成功打开但逻辑 event 解码失败，先释放句柄才能在 Windows 移动。
            RuntimeException closeFailure = closeOne(manifestStore, null);
            manifestStore = null;
            manifestRepository = null;
            if (closeFailure != null) {
                corruption.addSuppressed(closeFailure);
            }
            if (!Files.exists(manifestPath, LinkOption.NOFOLLOW_LINKS)) {
                throw corruption;
            }

            // 3. 有效 catalog 是重建依据；损坏 sidecar 仍保留为独立 evidence，禁止覆盖。
            Path evidenceDirectory = baseConfig.baseDir().resolve("catalog-recovery").resolve("evidence");
            Path evidence = evidenceDirectory.resolve(
                    "mysql.dd.manifest.corrupt." + Long.toUnsignedString(System.nanoTime()));
            try {
                Files.createDirectories(evidenceDirectory);
                Files.move(manifestPath, evidence, StandardCopyOption.ATOMIC_MOVE);
                log.warn("preserved corrupt dictionary recovery manifest before rebuilding: source={} evidence={}",
                        manifestPath, evidence, corruption);
            } catch (AtomicMoveNotSupportedException failure) {
                corruption.addSuppressed(failure);
                throw new DatabaseRuntimeException(
                        "preserving corrupt dictionary recovery manifest requires atomic move", corruption);
            } catch (IOException failure) {
                corruption.addSuppressed(failure);
                throw new DatabaseRuntimeException(
                        "preserve corrupt dictionary recovery manifest failed: " + manifestPath, corruption);
            }

            // 4. 新 journal 此刻仍无 clean；只有后续 recovery 完成后的 publisher 能授权灾难重建。
            manifestStore = FileDictionaryRecoveryManifestStore.openOrCreate(manifestPath);
            manifestRepository = new DictionaryRecoveryManifestRepository(manifestStore);
        }
    }

    /**
     * 用 NOFOLLOW 属性读取区分“缺失”与“已经存在但损坏”的 manifest 目录项。
     *
     * <p>零长度、symlink 和非常规文件都返回存在，随后必须进入严格 existing-open 与证据保留路径；
     * 只有明确 {@link NoSuchFileException} 才允许创建新 journal。</p>
     *
     * @param manifestPath 固定恢复 manifest 路径
     * @return 目录项存在时为 {@code true}，明确缺失时为 {@code false}
     * @throws DatabaseRuntimeException 属性读取失败、无法安全裁决是否允许创建时抛出
     */
    private static boolean manifestExistsNoFollow(Path manifestPath) {
        try {
            Files.readAttributes(manifestPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return true;
        } catch (NoSuchFileException missing) {
            return false;
        } catch (IOException failure) {
            throw new DatabaseRuntimeException(
                    "inspect dictionary recovery manifest path failed: " + manifestPath, failure);
        }
    }

    /**
     * 双槽 control 若因 latest 槽损坏回退到上一代，committed catalog 可能已经引用更高身份。启动时把各 next-counter
     * 单调推进到 catalog 最大值之后，随后才允许 recovery/DDL reserve，避免 ID/version 重用。DDL id 从独立
     * durable DDL log 的最大 marker 反推。v1 尚未记录 CREATE SCHEMA，因此再用 committed dictionary version-1
     * 作为保守下界；恢复版本或 DROP 双版本只会制造安全空洞，不会让已分配 DDL identity 被复用。
     */
    private void reconcileControlHighWater() {
        DictionaryControlSnapshot control = controlStore.snapshot();
        var snapshot = repository.snapshot();
        long nextSchema = nextAfter(snapshot.schemas().keySet().stream().mapToLong(id -> id.value())
                .max().orElse(1L), "schema");
        long nextTable = nextAfter(snapshot.tables().keySet().stream().mapToLong(id -> id.value())
                .max().orElse(0L), "table");
        long nextIndex = nextAfter(snapshot.indexes().keySet().stream().mapToLong(id -> id.value())
                .max().orElse(0L), "index");
        long nextSpace = nextAfter(snapshot.tables().values().stream()
                .flatMap(table -> table.storageBinding().stream())
                .mapToLong(binding -> binding.spaceId().value()).max().orElse(FIRST_USER_SPACE_ID - 1L), "space");
        long ddlEvidence = Math.max(repository.ddlLog().highestDdlId(),
                snapshot.publishedVersion().value() - 1L);
        long nextDdl = nextAfter(ddlEvidence, "ddl");
        long nextVersion = nextAfter(snapshot.publishedVersion().value(), "version");
        int schemaCount = advanceCount(control.nextSchemaId(), nextSchema, "schema");
        int tableCount = advanceCount(control.nextTableId(), nextTable, "table");
        int indexCount = advanceCount(control.nextIndexId(), nextIndex, "index");
        int spaceCount = advanceCount(control.nextSpaceId(), nextSpace, "space");
        int ddlCount = advanceCount(control.nextDdlId(), nextDdl, "ddl");
        int versionCount = advanceCount(control.nextDictionaryVersion(), nextVersion, "version");
        if ((long) schemaCount + tableCount + indexCount + spaceCount + ddlCount + versionCount > 0) {
            controlStore.reserve(new DictionaryIdRequest(schemaCount, tableCount, indexCount, spaceCount,
                    ddlCount, versionCount));
        }
    }

    private static int advanceCount(long current, long required, String kind) {
        if (current >= required) {
            return 0;
        }
        try {
            return Math.toIntExact(required - current);
        } catch (ArithmeticException overflow) {
            throw new DatabaseRuntimeException("dictionary " + kind + " control reconciliation range too large",
                    overflow);
        }
    }

    private static long nextAfter(long value, String kind) {
        try {
            return Math.addExact(value, 1L);
        } catch (ArithmeticException overflow) {
            throw new DatabaseRuntimeException("dictionary " + kind + " identity range exhausted", overflow);
        }
    }

    private void requireOpen() {
        if (state != DatabaseEngineState.OPEN) {
            throw new DatabaseRuntimeException("database engine is not OPEN: " + state);
        }
    }
}
