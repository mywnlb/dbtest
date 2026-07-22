package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.DdlId;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.domain.TableOptions;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.exception.MetadataLockTimeoutException;
import cn.zhangyis.db.dd.mdl.MdlDuration;
import cn.zhangyis.db.dd.mdl.MdlKey;
import cn.zhangyis.db.dd.mdl.MdlMode;
import cn.zhangyis.db.dd.mdl.MdlRequest;
import cn.zhangyis.db.dd.mdl.MdlTicket;
import cn.zhangyis.db.dd.mdl.MetadataLockManager;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.DictionaryIdAllocation;
import cn.zhangyis.db.dd.repo.DictionaryIdRequest;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.recovery.DictionaryCleanSnapshotPublisher;
import cn.zhangyis.db.dd.sdi.SerializedDictionaryInfoService;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.storage.api.ddl.StorageColumnDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageColumnType;
import cn.zhangyis.db.storage.api.ddl.StorageColumnTypeId;
import cn.zhangyis.db.storage.api.ddl.StorageIndexDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageIndexKeyPart;
import cn.zhangyis.db.storage.api.ddl.StorageIndexOrder;
import cn.zhangyis.db.storage.api.ddl.StorageTableDefinition;
import cn.zhangyis.db.storage.api.ddl.TableDdlStorageService;
import cn.zhangyis.db.storage.api.ddl.DdlUndoMarker;
import cn.zhangyis.db.storage.api.TablePurgeBarrier;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexBuildDescriptor;
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexBuildDuplicateKeyException;
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexDropDescriptor;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.StorageColumnRewrite;
import cn.zhangyis.db.storage.api.ddl.StorageDefaultValue;
import cn.zhangyis.db.storage.api.ddl.StorageTableRebuildRequest;
import cn.zhangyis.db.storage.api.ddl.TableRebuildException;
import cn.zhangyis.db.storage.api.ddl.OnlineIndexScanBatch;
import cn.zhangyis.db.storage.api.ddl.OnlineAlterDescriptorSet;
import cn.zhangyis.db.storage.api.ddl.OnlineAlterIndexAddRequest;
import cn.zhangyis.db.storage.api.ddl.OnlineAlterIndexDescriptor;
import cn.zhangyis.db.storage.api.ddl.OnlineAlterIndexDescriptorAction;
import cn.zhangyis.db.storage.api.ddl.OnlineAlterIndexDropRequest;
import cn.zhangyis.db.storage.api.ddl.BTreeIndexMetadataFactory;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlAbortReason;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlTablePhase;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexCaptureTarget;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexCandidate;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterCaptureTarget;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterCandidate;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterCandidateEntry;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterIndexTarget;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineClusteredIdentityCodec;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId;
import cn.zhangyis.db.storage.api.ddl.online.NoOpOnlineDdlCandidateCodec;
import cn.zhangyis.db.storage.fil.online.FileOnlineIndexChangeLog;
import cn.zhangyis.db.storage.fil.online.FileOnlineAlterChangeLog;
import cn.zhangyis.db.storage.record.online.SecondaryIndexCandidateCodec;
import cn.zhangyis.db.storage.record.online.MultiIndexAlterCandidateCodec;
import cn.zhangyis.db.storage.record.online.ClusteredIdentityCandidateCodec;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.dd.recovery.backup.RecoveryBackupArtifact;
import cn.zhangyis.db.dd.recovery.backup.RecoveryBackupService;
import cn.zhangyis.db.dd.recovery.backup.ValidatedRecoveryBackup;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * MDL、ID/version control、持久 DD、cache 与物理 storage DDL 的唯一协调器。锁顺序固定为 schema→table；
 * 等待 MDL 时尚未持有 MTR/page/file 资源，物理 CREATE durable 后才发布 ACTIVE，DROP 则先发布 DROP_PENDING。
 */
@Slf4j
public final class DictionaryDdlService {

    /**
     * 本对象持有的 {@code control} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DictionaryControlStore control;
    /**
     * 本对象持有的 {@code repository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PersistentDictionaryRepository repository;
    /**
     * 本对象持有的 {@code cache} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DictionaryObjectCache cache;
    /**
     * 本对象持有的 {@code locks} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final MetadataLockManager locks;
    /**
     * 本对象持有的 {@code physical} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final TableDdlStorageService physical;
    /** DROP_PENDING 发布前等待 committed history 清零的稳定 storage API。 */
    private final TablePurgeBarrier purgeBarrier;
    /**
     * 构造时冻结的 {@code tablesDirectory} 规范化路径；必须位于所属表空间或日志目录内，IO 层依赖它防止访问错误文件。
     */
    private final Path tablesDirectory;
    /**
     * 构造时冻结的 {@code faultInjector} 领域快照；其身份、版本与范围来自同一次权威读取，下游步骤依赖它检测并发变化和避免发布陈旧状态。
     */
    private final DictionaryDdlFaultInjector faultInjector;
    /** ACTIVE DD 发布前写入完整 table SDI，保证文件冗余先于 catalog 提交 durable。 */
    private final SerializedDictionaryInfoService sdi;
    /** DDL 前检查 manifest fence，并在稳定终点发布 clean snapshot；兼容构造使用 no-op。 */
    private final DictionaryCleanSnapshotPublisher cleanSnapshotPublisher;
    /** 可信 clean backup 的 lazy identity、固定路径、HMAC 与物理副本协调器。 */
    private final RecoveryBackupService recoveryBackups;
    /** 生产组合根注入时 CREATE INDEX 走 online 链；null 仅表示低层 legacy/恢复兼容测试。 */
    private final OnlineIndexBuildRuntime onlineIndexRuntime;
    /** live coordinator 发布阶段与终点的轻量 registry；不拥有 MDL/gate/文件资源。 */
    private final OnlineDdlOperationRegistry onlineDdlRegistry;
    /** 非null时DROP INDEX启用Online retirement协议；legacy/test构造继续保留blocking fallback。 */
    private final IndexRetirementBarrier indexRetirementBarrier;
    /** 通用multi-index与shadow capture运行期；null表示兼容构造未启用Slice E/F。 */
    private final OnlineAlterRuntime onlineAlterRuntime;
    /** 多INDEX或source TABLESPACE的table-level退休屏障；与通用runtime成对注入。 */
    private final OnlineAlterRetirementBarrier onlineAlterRetirementBarrier;
    /** 对initial/final MDL内冻结的逻辑aggregate生成恢复可验证的canonical schema checkpoint。 */
    private final DdlSchemaDigestService schemaDigests = new DdlSchemaDigestService();
    /** 通用ALTER在任何identity或物理副作用前冻结策略；当前打开instant与单index在线能力。 */
    private final OnlineAlterStrategySelector alterStrategySelector;

    /**
     * 构造不接 persistent history barrier、也不注入故障的低层 DDL 服务；仅供孤立组件测试使用。
     *
     * @param control         字典 id/version/DDL id 的 durable 单调分配器。
     * @param repository      committed catalog 事务仓储。
     * @param cache           table metadata pin/invalidation cache。
     * @param locks           schema/table MDL 锁入口。
     * @param physical        物理 tablespace CREATE/DROP facade。
     * @param tablesDirectory 受控表空间文件目录。
     * @throws DatabaseValidationException 任一协作者或目录为空时抛出。
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory) {
        this(control, repository, cache, locks, physical, tablesDirectory,
                TablePurgeBarrier.NONE, DictionaryDdlFaultInjector.NO_OP,
                DictionaryCleanSnapshotPublisher.noOp());
    }

    /**
     * 构造生产 DDL 服务，DROP 在发布 DROP_PENDING 前使用 StorageEngine 的真实 persistent history barrier。
     *
     * @param control         字典 id/version/DDL id 的 durable 单调分配器。
     * @param repository      committed catalog 事务仓储。
     * @param cache           table metadata pin/invalidation cache。
     * @param locks           schema/table MDL 锁入口。
     * @param physical        物理 tablespace CREATE/DROP facade。
     * @param tablesDirectory 受控表空间文件目录。
     * @param purgeBarrier    与 storage commit/purge/recovery 共享 history owner 的表级等待 API。
     * @throws DatabaseValidationException 任一协作者、目录或 barrier 为空时抛出。
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                TablePurgeBarrier purgeBarrier) {
        this(control, repository, cache, locks, physical, tablesDirectory,
                purgeBarrier, DictionaryDdlFaultInjector.NO_OP,
                DictionaryCleanSnapshotPublisher.noOp());
    }

    /**
     * 构造不接 history barrier、但允许在 durable DDL 状态边界注入故障的测试服务。
     *
     * @param control         字典 id/version 分配器。
     * @param repository      committed catalog 仓储。
     * @param cache           metadata cache。
     * @param locks           MDL 管理器。
     * @param physical        物理 DDL facade。
     * @param tablesDirectory 受控 tablespace 目录。
     * @param faultInjector   CREATE/DROP durable 边界故障接缝，不能为 {@code null}。
     * @throws DatabaseValidationException 任一依赖为空时抛出。
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                DictionaryDdlFaultInjector faultInjector) {
        this(control, repository, cache, locks, physical, tablesDirectory,
                TablePurgeBarrier.NONE, faultInjector, DictionaryCleanSnapshotPublisher.noOp());
    }

    /**
     * 构造同时注入真实/测试 purge barrier 与 durable 状态边界故障点的完整服务。
     *
     * @param control         字典 id/version 分配器。
     * @param repository      committed catalog 仓储。
     * @param cache           metadata cache。
     * @param locks           MDL 管理器。
     * @param physical        物理 DDL facade。
     * @param tablesDirectory 受控 tablespace 目录，构造时转为绝对规范路径。
     * @param purgeBarrier    DROP_PENDING 前等待 affected-table history 清零的稳定 storage API。
     * @param faultInjector   durable 状态发布后的测试故障接缝。
     * @throws DatabaseValidationException 任一依赖为空时抛出，服务不会进入可执行状态。
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                TablePurgeBarrier purgeBarrier,
                                DictionaryDdlFaultInjector faultInjector) {
        this(control, repository, cache, locks, physical, tablesDirectory, purgeBarrier, faultInjector,
                DictionaryCleanSnapshotPublisher.noOp());
    }

    /**
     * 构造接入灾难恢复 clean snapshot fence 的生产 DDL 服务。
     *
     * @param control 字典 identity/version durable 分配器
     * @param repository committed catalog 仓储
     * @param cache metadata cache
     * @param locks MDL 管理器
     * @param physical 物理 DDL facade
     * @param tablesDirectory 受控表空间目录
     * @param purgeBarrier persistent history 表引用屏障
     * @param faultInjector durable 边界故障接缝
     * @param cleanSnapshotPublisher DDL 前 fence 与稳定终点 manifest publisher
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                TablePurgeBarrier purgeBarrier,
                                DictionaryDdlFaultInjector faultInjector,
                                DictionaryCleanSnapshotPublisher cleanSnapshotPublisher) {
        this(control, repository, cache, locks, physical, tablesDirectory, purgeBarrier,
                faultInjector, cleanSnapshotPublisher, null);
    }

    /**
     * 构造生产 DDL 服务并强制 CREATE INDEX 使用 Online ADD INDEX runtime。
     *
     * @param control durable identity/version allocator
     * @param repository committed catalog 与 DDL log repository
     * @param cache metadata pin/invalidation cache
     * @param locks schema/table MDL manager
     * @param physical storage DDL facade
     * @param tablesDirectory 受控 tablespace 目录
     * @param purgeBarrier persistent history barrier
     * @param faultInjector durable phase fault seam
     * @param cleanSnapshotPublisher clean recovery manifest publisher
     * @param onlineIndexRuntime DML gate、row-log、scan config 与 record codec 组合根
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                TablePurgeBarrier purgeBarrier,
                                DictionaryDdlFaultInjector faultInjector,
                                DictionaryCleanSnapshotPublisher cleanSnapshotPublisher,
                                OnlineIndexBuildRuntime onlineIndexRuntime) {
        this(control, repository, cache, locks, physical, tablesDirectory, purgeBarrier,
                faultInjector, cleanSnapshotPublisher, onlineIndexRuntime,
                new OnlineDdlOperationRegistry(256));
    }

    /**
     * 构造共享 Online DDL registry 的生产服务；只有组合根与需要验证控制面的协作测试使用此入口。
     *
     * @param control durable identity/version allocator
     * @param repository committed catalog 与 DDL marker repository
     * @param cache metadata cache
     * @param locks schema/table MDL manager
     * @param physical storage DDL facade
     * @param tablesDirectory 受控 tablespace 目录
     * @param purgeBarrier persistent history barrier
     * @param faultInjector durable phase fault seam
     * @param cleanSnapshotPublisher clean recovery manifest publisher
     * @param onlineIndexRuntime Online ADD INDEX gate/log/codec runtime；阻塞兼容路径允许为空
     * @param onlineDdlRegistry live/recovery/control facade 共享的轻量 operation registry
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                TablePurgeBarrier purgeBarrier,
                                DictionaryDdlFaultInjector faultInjector,
                                DictionaryCleanSnapshotPublisher cleanSnapshotPublisher,
                                 OnlineIndexBuildRuntime onlineIndexRuntime,
                                 OnlineDdlOperationRegistry onlineDdlRegistry) {
        this(control, repository, cache, locks, physical, tablesDirectory, purgeBarrier,
                faultInjector, cleanSnapshotPublisher, onlineIndexRuntime, onlineDdlRegistry, null);
    }

    /**
     * 构造同时启用Online ADD与Online DROP的生产coordinator；blocking DROP仍由不注入retirement barrier的旧构造保留。
     *
     * @param control durable identity/version allocator
     * @param repository committed catalog与DDL marker repository
     * @param cache metadata cache
     * @param locks schema/table MDL manager
     * @param physical storage DDL facade
     * @param tablesDirectory 受控tablespace目录
     * @param purgeBarrier blocking table DDL使用的persistent history barrier
     * @param faultInjector durable边界故障接缝
     * @param cleanSnapshotPublisher clean recovery manifest publisher
     * @param onlineIndexRuntime Online DDL gate与ADD row-log runtime
     * @param onlineDdlRegistry live/recovery/control共享registry
     * @param indexRetirementBarrier Online DROP的history high-water与exact source pin屏障
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                TablePurgeBarrier purgeBarrier,
                                DictionaryDdlFaultInjector faultInjector,
                                DictionaryCleanSnapshotPublisher cleanSnapshotPublisher,
                                OnlineIndexBuildRuntime onlineIndexRuntime,
                                OnlineDdlOperationRegistry onlineDdlRegistry,
                                IndexRetirementBarrier indexRetirementBarrier) {
        this(control, repository, cache, locks, physical, tablesDirectory, purgeBarrier,
                faultInjector, cleanSnapshotPublisher, onlineIndexRuntime, onlineDdlRegistry,
                indexRetirementBarrier, null, null);
    }

    /**
     * 构造完整Online ALTER生产coordinator；只有组合根与端到端协作测试应使用该入口。
     *
     * @param control durable identity/version allocator
     * @param repository committed catalog与DDL marker repository
     * @param cache exact-version dictionary cache
     * @param locks schema/table MDL manager
     * @param physical 物理DDL facade
     * @param tablesDirectory 受控tablespace目录
     * @param purgeBarrier persistent table history屏障
     * @param faultInjector durable阶段故障接缝
     * @param cleanSnapshotPublisher clean recovery manifest publisher
     * @param onlineIndexRuntime legacy单ADD/DROP runtime
     * @param onlineDdlRegistry live/recovery/control共享operation registry
     * @param indexRetirementBarrier legacy单DROP retirement barrier
     * @param onlineAlterRuntime 通用journal/capture/ReadView barrier runtime
     * @param onlineAlterRetirementBarrier 多资源table-level retirement barrier
     */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                TablePurgeBarrier purgeBarrier,
                                DictionaryDdlFaultInjector faultInjector,
                                DictionaryCleanSnapshotPublisher cleanSnapshotPublisher,
                                OnlineIndexBuildRuntime onlineIndexRuntime,
                                OnlineDdlOperationRegistry onlineDdlRegistry,
                                IndexRetirementBarrier indexRetirementBarrier,
                                OnlineAlterRuntime onlineAlterRuntime,
                                OnlineAlterRetirementBarrier onlineAlterRetirementBarrier) {
        if (control == null || repository == null || cache == null || locks == null || physical == null
                || tablesDirectory == null || purgeBarrier == null || faultInjector == null
                || cleanSnapshotPublisher == null || onlineDdlRegistry == null
                || (onlineAlterRuntime == null) != (onlineAlterRetirementBarrier == null)) {
            throw new DatabaseValidationException("dictionary DDL collaborators/path must not be null");
        }
        this.control = control;
        this.repository = repository;
        this.cache = cache;
        this.locks = locks;
        this.physical = physical;
        this.purgeBarrier = purgeBarrier;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
        this.faultInjector = faultInjector;
        this.sdi = new SerializedDictionaryInfoService(physical);
        this.cleanSnapshotPublisher = cleanSnapshotPublisher;
        Path instanceRoot = this.tablesDirectory.getParent();
        if (instanceRoot == null) {
            throw new DatabaseValidationException(
                    "tables directory has no instance root for recovery backups");
        }
        this.recoveryBackups = new RecoveryBackupService(instanceRoot, physical);
        this.onlineIndexRuntime = onlineIndexRuntime;
        this.onlineDdlRegistry = onlineDdlRegistry;
        this.indexRetirementBarrier = indexRetirementBarrier;
        this.onlineAlterRuntime = onlineAlterRuntime;
        this.onlineAlterRetirementBarrier = onlineAlterRetirementBarrier;
        this.alterStrategySelector = onlineAlterRuntime == null
                ? OnlineAlterStrategySelector.productionV1()
                : OnlineAlterStrategySelector.productionComplete();
    }

    /**
     * 创建 schema；X MDL 覆盖重复名称检查、identity/version 预留、catalog 与 clean manifest 发布。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验调用参数并检查既有 manifest failure fence；失败时没有 identity 或 catalog 副作用。</li>
     *     <li>取得 schema X MDL，在同一锁域内复核名称尚未存在。</li>
     *     <li>durable 预留 schema/DDL/version，并由 dictionary transaction 先写 intent、再提交 schema catalog。</li>
     *     <li>在 catalog 稳定后发布 clean manifest；失败建立后续 DDL fence，但不回滚已经 durable 的 schema。</li>
     * </ol>
     *
     * @param owner 参与 {@code createSchema} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param charsetId 参与 {@code createSchema} 的原始数值身份 {@code charsetId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param collationId 参与 {@code createSchema} 的原始数值身份 {@code collationId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code createSchema} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SchemaDefinition createSchema(MdlOwnerId owner, ObjectName name, int charsetId, int collationId,
                                         Duration timeout) {
        // 1. manifest 已有失败必须早于 control reservation 和 MDL 等待被观察。
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (name == null || charsetId <= 0 || collationId <= 0) {
            throw new DatabaseValidationException("create schema name/charset/collation invalid");
        }
        // 2. schema X 把名称检查与本次 catalog publish 串成单一 DDL 临界区。
        try (MdlTicket ignored = locks.acquire(new MdlRequest(owner, MdlKey.schema(name.canonicalName()),
                MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), timeout)) {
            // 3. control witness 与 catalog mutation intent 都早于对应权威文件提交点。
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(1, 0, 0, 0, 1, 1));
            DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
            SchemaDefinition schema = new SchemaDefinition(SchemaId.of(ids.firstSchemaId()), name,
                    charsetId, collationId, version);
            try (DictionaryTransaction transaction = repository.begin(version)) {
                transaction.createSchema(schema);
                transaction.commit();
            }
            log.info("created schema: name={} id={} ddlId={} version={}", name.canonicalName(),
                    schema.id().value(), ids.firstDdlId(), version.value());
            // 4. schema 没有单表 SDI，只有 clean manifest 能保存其名称和默认字符语义。
            cleanSnapshotPublisher.publish();
            return schema;
        }
    }

    /**
     * 创建表并用独立 DDL log 记录物理引擎与 DD 提交边界。DD 或 marker append 报错时不能证明 catalog header
     * 未 durable，因此不在当前进程猜测补偿结果，由重启后的 committed DD + marker 联合裁决。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得 schema IX/table X MDL，解析 schema 与重复表名；失败时未分配身份、未写文件。</li>
     *     <li>durable 预留对象/space/ddl/version，构造逻辑/物理定义并在物理 CREATE 前写 PREPARED。</li>
     *     <li>调用 storage facade 创建 tablespace/segments/root 并满足 redo durability，再写 ENGINE_DONE。</li>
     *     <li>以最终 binding 写入 durable SDI，再单批发布 ACTIVE DD；成功后写 DICTIONARY_COMMITTED，
     *     不确定响应保留文件并停止本次 DDL。</li>
     *     <li>发布 metadata cache 后写 COMMITTED；终态缺失时启动恢复以 ACTIVE DD 为真相补齐。</li>
     * </ol>
     *
     * @param owner 本次 DDL 的 MDL owner，schema/table ticket 都归该 owner 生命周期。
     * @param command 已完成结构构造校验的 CREATE TABLE 命令，必须引用已存在 schema。
     * @param timeout MDL 与物理 CREATE 使用的正有界等待时间。
     * @return ACTIVE 且携带 durable storage binding 的表定义；返回前 marker 已进入 COMMITTED。
     * @throws DatabaseValidationException owner/command/timeout 无效时抛出，且不进入 DDL 流程。
     * @throws cn.zhangyis.db.common.exception.DatabaseRuntimeException 物理创建、DD 发布或 DDL marker 协作失败时抛出；
     *                                                               调用方必须停止本次 DDL，不得自行删除 marker path。
     * @throws cn.zhangyis.db.dd.exception.DictionaryObjectExistsException 目标身份或唯一键已被占用时抛出；调用方应回滚本次变更或改用其他合法身份
     */
    public TableDefinition createTable(MdlOwnerId owner, CreateTableCommand command, Duration timeout) {
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (command == null) {
            throw new DatabaseValidationException("create table command must not be null");
        }
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(command.name().schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner, MdlKey.table(command.name().canonicalKey()),
                     MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), timeout)) {
            // 1、名称解析发生在身份预留前；重复/缺失 schema 不产生 control、catalog 或文件副作用。
            SchemaDefinition schema = repository.findSchema(command.name().schema())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "schema does not exist: " + command.name().schema().displayName()));
            if (repository.findTable(schema.id(), command.name().table()).isPresent()) {
                throw new cn.zhangyis.db.dd.exception.DictionaryObjectExistsException(
                        "table already exists: " + command.name().canonicalKey());
            }

            // 2、一次 control force 固定全部 identity；PREPARED 必须早于物理 CREATE，才能精确关联潜在 orphan。
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(
                    0, 1, command.indexes().size(), 1, 1, 1));
            DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
            TableId tableId = TableId.of(ids.firstTableId());
            List<ColumnDefinition> columns = columns(command);
            List<IndexDefinition> indexes = indexes(command, ids.firstIndexId(), columns);
            Path path = tablePath(tableId, ids.firstSpaceId());
            ensureTablesDirectory();
            StorageTableDefinition storageRequest = storageDefinition(tableId, ids.firstSpaceId(), path,
                    version, command, columns, indexes);
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            TableOptions options = new TableOptions(
                    "", schema.defaultCharsetId(), schema.defaultCollationId());
            TableDefinition plannedTarget = new TableDefinition(
                    tableId, schema.id(), command.name().table(), version, TableState.ACTIVE,
                    columns, indexes, Optional.empty(), options);
            // PREPARED 早于物理 CREATE；marker outcome 不确定时立即停止，由恢复按 exact path 裁决。
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), version.value(), tableId.value()),
                    0L, DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED,
                    cn.zhangyis.db.domain.SpaceId.of(ids.firstSpaceId()), path,
                    Optional.empty(), Optional.empty(), DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.empty(), Optional.empty(),
                    Optional.of(schemaDigests.digest(schema, plannedTarget, version.value())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            faultInjector.afterCreatePrepared(prepared);

            // 3、storage 返回代表物理初始化与所需 redo durability 已完成，此后才允许记录 ENGINE_DONE。
            TableStorageBinding binding = physical.createTable(storageRequest);
            DdlLogRecord engineDone = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            faultInjector.afterCreateEngineDone(engineDone);

            // 4、最终 binding 已确定后先写 durable SDI；ACTIVE 仍是字典提交裁决点，SDI 不反向决定 catalog。
            TableDefinition table = new TableDefinition(tableId, schema.id(), command.name().table(), version,
                    TableState.ACTIVE, columns, indexes, java.util.Optional.of(binding),
                    options);
            sdi.write(table, timeout);
            try {
                try (DictionaryTransaction transaction = repository.begin(version)) {
                    transaction.createTable(table);
                    transaction.commit();
                }
            } catch (RuntimeException publishFailure) {
                // catalog force 可能已成功但返回 IO 错误；立即 drop 会造成 durable ACTIVE 指向缺失文件。
                log.warn("dictionary CREATE publish outcome is uncertain; retaining physical storage: "
                                + "tableId={} space={} path={}", tableId.value(), ids.firstSpaceId(), path,
                        publishFailure);
                throw publishFailure;
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            faultInjector.afterCreateDictionaryCommitted(table);

            // 5、cache 只发布 committed DD；terminal marker 晚于 cache，缺失时恢复可幂等补齐。
            cache.publishTable(table);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
            log.info("created table: name={} tableId={} space={} ddlId={} version={}",
                    command.name().canonicalKey(), tableId.value(), ids.firstSpaceId(), ids.firstDdlId(),
                    version.value());
            cleanSnapshotPublisher.publish();
            return table;
        }
    }

    /**
     * 在既有 ACTIVE 表中离线构建并发布一个二级索引。v1 全程持 table MDL X，不实现 online DDL row log。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得 schema IX/table X，解析 ACTIVE table、重复索引名和 key columns；随后等待 purge history
     *     与旧 metadata pin 排空，保证构建期间没有旧 descriptor 访问该表。</li>
     *     <li>一次 durable control reserve 固定 index/DDL/version，写 CREATE_INDEX PREPARED marker；
     *     marker 同时携带 table id 与 secondary index id。</li>
     *     <li>storage 在同一物理提交中 staged segments/root/footer，再扫描聚簇 live rows backfill；
     *     UNIQUE 重复属于确定性失败，会先回收 staged 资源再把 marker 写为 ROLLED_BACK。</li>
     *     <li>物理构建 durable 后写 ENGINE_DONE，以旧 rowFormatVersion 组装新 ACTIVE aggregate，先写 SDI
     *     再提交 DD，并写 DICTIONARY_COMMITTED；DD append outcome 不确定时保留 footer 供恢复裁决。</li>
     *     <li>发布新 cache，exact-CAS 清 footer并写 COMMITTED；footer clear 失败不撤销 committed DD，
     *     启动恢复会按新 DD binding 补齐。</li>
     * </ol>
     *
     * @param owner 本次 DDL 的普通低半区 MDL owner，不能复用 Session transaction owner
     * @param command 完整表名和非聚簇 index 定义
     * @param timeout MDL、purge/pin、WAL 与物理持久化共用的正有界时限
     * @return 包含新 IndexDefinition/binding、字典版本已推进且 rowFormatVersion 不变的 ACTIVE table
     * @throws DatabaseValidationException owner/command/timeout 或 key 定义无效时抛出
     * @throws SecondaryIndexBuildDuplicateKeyException UNIQUE backfill 发现重复非 NULL key 时抛出；
     *                                                   staged 资源清理成功后 marker 为 ROLLED_BACK
     * @throws cn.zhangyis.db.common.exception.DatabaseRuntimeException 物理、SDI、DD 或 marker 状态不确定时抛出；
     *                                                               调用方不得自行删除 segment
     * @throws cn.zhangyis.db.dd.exception.DictionaryObjectExistsException 目标身份或唯一键已被占用时抛出；调用方应回滚本次变更或改用其他合法身份
     * @throws DictionaryDdlException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    public TableDefinition createSecondaryIndex(MdlOwnerId owner,
                                                CreateSecondaryIndexCommand command,
                                                Duration timeout) {
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (command == null) {
            throw new DatabaseValidationException("create secondary index command must not be null");
        }
        if (onlineIndexRuntime != null) {
            return createSecondaryIndexOnline(owner, command, timeout);
        }
        QualifiedTableName name = command.table();
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner, MdlKey.table(name.canonicalKey()),
                     MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), timeout)) {
            // 1. MDL X 下重新绑定名称和列；所有可预见错误均早于 identity reserve 与物理副作用。
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "schema does not exist: " + name.schema().displayName()));
            TableDefinition active = repository.findTable(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("table does not exist: " + name.canonicalKey()));
            if (active.indexes().stream().anyMatch(
                    index -> index.name().equals(command.index().name()))) {
                throw new cn.zhangyis.db.dd.exception.DictionaryObjectExistsException(
                        "index already exists: " + command.index().name().displayName());
            }
            Map<ObjectName, Long> columnIds = new LinkedHashMap<>();
            for (ColumnDefinition column : active.columns()) {
                columnIds.put(column.name(), column.columnId());
            }
            List<IndexKeyPart> keyParts = command.index().keyParts().stream()
                    .map(part -> new IndexKeyPart(
                            requireColumnId(columnIds, part.columnName()), part.order(), part.prefixBytes()))
                    .toList();
            if (keyParts.stream().map(IndexKeyPart::columnId).distinct().count() != keyParts.size()) {
                throw new DatabaseValidationException("CREATE INDEX key parts must not repeat a column");
            }
            purgeBarrier.awaitUnreferenced(active.id().value(), timeout);
            if (!cache.awaitUnpinned(active.id(), timeout)) {
                throw new DictionaryDdlException(
                        "timed out waiting old dictionary pins before CREATE INDEX: " + active.id().value());
            }
            TableStorageBinding oldBinding = active.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException("ACTIVE table has no physical binding: " + active.id().value()));

            // 2. PREPARED 先于任何 staged segment；index id 同时进入 marker secondary identity。
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 1, 0, 1, 1));
            DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
            IndexDefinition newIndex = new IndexDefinition(
                    IndexId.of(ids.firstIndexId()), command.index().name(), command.index().unique(),
                    false, keyParts);
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            List<IndexDefinition> targetIndexes = appendIndex(active.indexes(), newIndex);
            TableDefinition plannedTarget = logicalVersion(
                    active, version, TableState.ACTIVE, targetIndexes);
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), version.value(), active.id().value()),
                    newIndex.id().value(), DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                    oldBinding.spaceId(), oldBinding.path(), Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(schemaDigests.digest(schema, active, oldBinding.rowFormatVersion())),
                    Optional.empty(),
                    Optional.of(schemaDigests.digest(
                            schema, plannedTarget, oldBinding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            faultInjector.afterCreateIndexPrepared(prepared);

            // 3. footer 与 segments/root 同批 durable；backfill 每行短 MTR，确定性 unique 冲突执行完整物理补偿。
            StorageIndexDefinition storageIndex = storageIndex(newIndex);
            SecondaryIndexBuildDescriptor staged = physical.beginSecondaryIndexBuild(
                    oldBinding, ddlId.value(), version.value(), storageIndex, timeout);
            StorageTableDefinition buildingDefinition = storageDefinition(active, newIndex);
            IndexStorageBinding completed;
            try {
                completed = physical.backfillSecondaryIndex(
                        buildingDefinition, oldBinding, staged, timeout);
            } catch (SecondaryIndexBuildDuplicateKeyException duplicate) {
                try {
                    physical.rollbackSecondaryIndexBuild(oldBinding, staged, timeout);
                    repository.ddlLog().transition(
                            ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
                } catch (RuntimeException cleanupFailure) {
                    duplicate.addSuppressed(cleanupFailure);
                }
                throw duplicate;
            }
            DdlLogRecord engineDone = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            faultInjector.afterCreateIndexEngineDone(engineDone);

            // 4. metadata-only publish 保持 row format version；SDI 先于 catalog，catalog 仍是唯一提交真相。
            List<IndexStorageBinding> bindings = new ArrayList<>(oldBinding.indexes());
            bindings.add(completed);
            TableStorageBinding newBinding = new TableStorageBinding(
                    oldBinding.tableId(), oldBinding.spaceId(), oldBinding.path(),
                    oldBinding.rowFormatVersion(), bindings, oldBinding.lobSegment());
            TableDefinition published = new TableDefinition(
                    active.id(), active.schemaId(), active.name(), version, TableState.ACTIVE,
                    active.columns(), targetIndexes, java.util.Optional.of(newBinding), active.options());
            sdi.write(published, timeout);
            // 新 index 已进入 target SDI；DD 响应不确定时必须阻断旧 aggregate，避免后续 DML 漏维护新索引。
            cache.invalidateTable(active.id(), version);
            try {
                commitUpdate(version, published);
            } catch (RuntimeException publishFailure) {
                log.warn("CREATE INDEX DD publish outcome is uncertain; retaining staged descriptor: "
                                + "tableId={} indexId={} ddlId={}",
                        active.id().value(), newIndex.id().value(), ddlId.value(), publishFailure);
                throw publishFailure;
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            faultInjector.afterCreateIndexDictionaryCommitted(published);

            // 5. cache 只发布 committed aggregate；清 footer 后 terminal marker 证明恢复资源已收敛。
            cache.publishTable(published);
            physical.clearSecondaryIndexBuild(oldBinding, staged, timeout);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
            log.info("created secondary index: table={} index={} indexId={} ddlId={} version={}",
                    name.canonicalKey(), newIndex.name().canonicalName(), newIndex.id().value(),
                    ddlId.value(), version.value());
            cleanSnapshotPublisher.publish();
            return published;
        }
    }

    /**
     * 生产 Online ADD INDEX 协调器。initial/final X 仅覆盖状态冻结与发布，base scan 在 SU 下允许 SR/SW；
     * candidate 不是提交事件，final 两遍 reconciliation 始终回读 cutover 当前聚簇真相。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>schema IX/table X 下重读并验证旧 aggregate，预留 identity，冻结 table gate并持久化 manifest/marker。</li>
     *     <li>创建 staged root/segments，force CAPTURING frame，发布 gate target并把同一 MDL ticket X→SU。</li>
     *     <li>以配置批次扫描聚簇 live rows，逐行幂等 ensure target；每个批次边界检查 durable abort。</li>
     *     <li>SU→X 后 seal gate、force row log，对 candidate 先删 before/after，再按当前聚簇行重新 ensure。</li>
     *     <li>双向验证、WAL/表空间 force 与 RECONCILED durable 后写 ENGINE_DONE；此后只允许前滚 DD。</li>
     *     <li>按 SDI→cache invalidate→DD→DICTIONARY_COMMITTED→cache publish→footer clear→COMMITTED 发布，最后清 gate/log。</li>
     * </ol>
     *
     * @param owner 本次 DDL 独立 MDL owner
     * @param command 已通过 SQL/binder 归一化的 secondary index 命令
     * @param timeout 所有 MDL、gate、row-log、WAL 与 file force 的正等待上限
     * @return 已发布新 secondary binding 的 ACTIVE table aggregate
     * @throws DictionaryDdlException ENGINE_DONE 前失败且已完成补偿，或 ENGINE_DONE 后需重启前滚时抛出
     */
    private TableDefinition createSecondaryIndexOnline(MdlOwnerId owner,
                                                       CreateSecondaryIndexCommand command,
                                                       Duration timeout) {
        QualifiedTableName name = command.table();
        OnlineIndexBuildId buildId = null;
        FileOnlineIndexChangeLog changeLog = null;
        SecondaryIndexBuildDescriptor staged = null;
        TableStorageBinding oldBinding = null;
        DdlId ddlId = null;
        OnlineDdlOperationTracker tracker = null;
        boolean markerPrepared = false;
        boolean engineDone = false;
        boolean reconciledDurable = false;
        boolean forwardOnly = false;

        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner,
                     MdlKey.table(name.canonicalKey()), MdlMode.SHARED_UPGRADABLE,
                     MdlDuration.TRANSACTION), timeout)) {
            // 1. X 下冻结本次 command 的所有 DD identity；失败早于 staged segment/page 创建。
            locks.upgrade(tableTicket, MdlMode.EXCLUSIVE, timeout);
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "schema does not exist: " + name.schema().displayName()));
            TableDefinition active = repository.findTable(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("table does not exist: " + name.canonicalKey()));
            if (active.indexes().stream().anyMatch(index -> index.name().equals(command.index().name()))) {
                throw new cn.zhangyis.db.dd.exception.DictionaryObjectExistsException(
                        "index already exists: " + command.index().name().displayName());
            }
            Map<ObjectName, Long> columnIds = new LinkedHashMap<>();
            for (ColumnDefinition column : active.columns()) {
                columnIds.put(column.name(), column.columnId());
            }
            List<IndexKeyPart> keyParts = command.index().keyParts().stream()
                    .map(part -> new IndexKeyPart(requireColumnId(columnIds, part.columnName()),
                            part.order(), part.prefixBytes())).toList();
            if (keyParts.stream().map(IndexKeyPart::columnId).distinct().count() != keyParts.size()) {
                throw new DatabaseValidationException("CREATE INDEX key parts must not repeat a column");
            }
            purgeBarrier.awaitUnreferenced(active.id().value(), timeout);
            if (!cache.awaitUnpinned(active.id(), timeout)) {
                throw new DictionaryDdlException(
                        "timed out waiting old dictionary pins before online CREATE INDEX: "
                                + active.id().value());
            }
            oldBinding = active.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException("ACTIVE table has no physical binding: " + active.id().value()));
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 1, 0, 1, 1));
            DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
            IndexDefinition newIndex = new IndexDefinition(IndexId.of(ids.firstIndexId()),
                    command.index().name(), command.index().unique(), false, keyParts);
            ddlId = DdlId.of(ids.firstDdlId());
            buildId = OnlineIndexBuildId.of(ddlId.value());
            List<IndexDefinition> targetIndexes = appendIndex(active.indexes(), newIndex);
            TableDefinition plannedTarget = logicalVersion(
                    active, version, TableState.ACTIVE, targetIndexes);
            tracker = onlineDdlRegistry.register(new OnlineDdlOperationIdentity(
                    ddlId, DdlLogOperation.CREATE_INDEX, active.id().value(),
                    newIndex.id().value(), name.canonicalKey(),
                    newIndex.name().canonicalName(), active.version().value(),
                    version.value(), owner.value(), false, java.util.OptionalLong.empty()),
                    DdlExecutionProtocol.ONLINE_INDEX_V1);
            tracker.advanceRuntime(OnlineDdlRuntimePhase.ACTIVATING, OnlineDdlWaitReason.NONE);
            if (!tracker.beginDurablePrepare()) {
                throw new OnlineDdlCancellationException(
                        "online CREATE INDEX cancelled before durable prepare: ddl=" + ddlId.value());
            }
            OnlineIndexBuildManifest manifest = new OnlineIndexBuildManifest(buildId, active.id(),
                    active.version(), version, newIndex);
            byte[] manifestBytes = new OnlineIndexBuildManifestCodec().encode(manifest);
            Path rowLogPath = onlineIndexRuntime.logFiles().pathFor(buildId);
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), version.value(), active.id().value()),
                    newIndex.id().value(), DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                    oldBinding.spaceId(), oldBinding.path(), Optional.of(rowLogPath), Optional.empty(),
                    DdlExecutionProtocol.ONLINE_INDEX_V1,
                    Optional.of(schemaDigests.digest(schema, active, oldBinding.rowFormatVersion())),
                    Optional.empty(),
                    Optional.of(schemaDigests.digest(
                            schema, plannedTarget, oldBinding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            try {
                repository.ddlLog().prepare(prepared);
            } catch (RuntimeException prepareFailure) {
                tracker.failDurablePrepare("MARKER_PREPARE_FAILED");
                throw prepareFailure;
            }
            markerPrepared = true;
            tracker.markDurablePrepared(prepared);
            faultInjector.afterCreateIndexPrepared(prepared);
            requireOnlineDdlNotCancelled(ddlId, "after durable prepare");

            // marker先于gate/row-log/descriptor：若此后任一点崩溃，source+OPEN恢复分支可以确定性清理或回滚。
            onlineIndexRuntime.gate().beginActivation(active.id().value(), buildId, timeout);
            sampleOnlineDdl(tracker, active.id().value(), null);
            physical.makeSecondaryIndexBuildDurable(oldBinding,
                    onlineIndexRuntime.gate().terminalRedoHighWater(active.id().value()), timeout);
            changeLog = onlineIndexRuntime.logFiles().create(new OnlineIndexLogHeader(
                    buildId, active.id().value(), newIndex.id().value(), active.version().value(),
                    version.value(), oldBinding.rowFormatVersion(), manifestBytes));
            requireOnlineDdlNotCancelled(ddlId, "after change-log creation");

            // 2. descriptor 与 CAPTURING frame durable 后才发布 target；MDL ticket 原地降级，不释放 owner identity。
            StorageIndexDefinition storageIndex = storageIndex(newIndex);
            staged = physical.beginSecondaryIndexBuild(
                    oldBinding, ddlId.value(), version.value(), storageIndex, timeout);
            requireOnlineDdlNotCancelled(ddlId, "after staged index creation");
            StorageTableDefinition buildingDefinition = storageDefinition(active, newIndex);
            List<IndexStorageBinding> buildingBindings = new ArrayList<>(oldBinding.indexes());
            buildingBindings.add(staged.indexBinding());
            TableStorageBinding buildingBinding = new TableStorageBinding(oldBinding.tableId(),
                    oldBinding.spaceId(), oldBinding.path(), oldBinding.rowFormatVersion(),
                    buildingBindings, oldBinding.lobSegment());
            var secondary = new BTreeIndexMetadataFactory().createTable(
                    buildingDefinition, buildingBinding).requireSecondary(newIndex.id().value());
            SecondaryIndexCandidateCodec candidateCodec = new SecondaryIndexCandidateCodec(
                    secondary.layout(), onlineIndexRuntime.typeRegistry());
            changeLog.appendState(OnlineIndexLogRecordType.GENERATION_STARTED, new byte[0]);
            long capturingSequence = changeLog.appendState(
                    OnlineIndexLogRecordType.CAPTURING, new byte[0]);
            changeLog.forceThrough(capturingSequence, timeout);
            onlineIndexRuntime.gate().publishCapture(new OnlineIndexCaptureTarget(
                    buildId, active.id().value(), newIndex.id().value(), changeLog, candidateCodec));
            locks.downgrade(tableTicket, MdlMode.SHARED_UPGRADABLE);
            tracker.advanceRuntime(OnlineDdlRuntimePhase.CAPTURING, OnlineDdlWaitReason.NONE);
            sampleOnlineDdl(tracker, active.id().value(), changeLog);
            faultInjector.afterCreateIndexCaptureDurable(prepared);

            // 3. 批次之间没有 page guard；并发 DML candidate 会由事务 commit/prepare force，不阻塞 base scan。
            tracker.advanceRuntime(OnlineDdlRuntimePhase.BASE_SCAN, OnlineDdlWaitReason.NONE);
            Optional<SearchKey> continuation = Optional.empty();
            while (true) {
                requireOnlineDdlNotCancelled(ddlId, "before base-scan batch");
                if (changeLog.abortRequired()
                        || onlineIndexRuntime.gate().phase(active.id().value())
                        == OnlineDdlTablePhase.ABORTING) {
                    throw new DictionaryDdlException("online CREATE INDEX capture requested abort");
                }
                OnlineIndexScanBatch batch = physical.scanSecondaryIndexBuildBatch(
                        buildingDefinition, oldBinding, staged, continuation,
                        onlineIndexRuntime.config().scanBatchRows());
                for (var row : batch.rows()) {
                    staged = physical.ensureSecondaryIndexLiveForBaseScan(
                            buildingDefinition, oldBinding, staged, row);
                }
                tracker.addScanBatch(batch.rows().size(), Optional.empty());
                sampleOnlineDdl(tracker, active.id().value(), changeLog);
                requireOnlineDdlNotCancelled(ddlId, "after base-scan batch");
                if (batch.complete()) {
                    break;
                }
                continuation = batch.continuation();
            }

            // 4. final X 与 gate seal 排除新的 clustered mutation；两遍处理把多余 candidate 收敛到当前聚簇真相。
            tracker.advanceRuntime(
                    OnlineDdlRuntimePhase.WAITING_FINAL_MDL, OnlineDdlWaitReason.METADATA_LOCK);
            requireOnlineDdlNotCancelled(ddlId, "before final MDL wait");
            try {
                locks.upgrade(tableTicket, MdlMode.EXCLUSIVE, timeout);
            } catch (MetadataLockTimeoutException waitFailure) {
                if (onlineDdlCancelled(ddlId)) {
                    throw new OnlineDdlCancellationException(
                            "online CREATE INDEX cancelled during final MDL wait: ddl="
                                    + ddlId.value(), waitFailure);
                }
                throw waitFailure;
            }
            requireOnlineDdlNotCancelled(ddlId, "after final MDL wait");
            TableDefinition finalSource = repository.findTable(schema.id(), name.table())
                    .orElseThrow(() -> new DictionaryDdlException(
                            "online CREATE INDEX source disappeared before final X"));
            requireLiveDigest(prepared.sourceSchemaDigest().orElseThrow(),
                    schemaDigests.digest(schema, finalSource, oldBinding.rowFormatVersion()),
                    ddlId, "source at final X");
            tracker.advanceRuntime(
                    OnlineDdlRuntimePhase.FINALIZING, OnlineDdlWaitReason.GATE_QUIESCENCE);
            onlineIndexRuntime.gate().beginSeal(buildId, timeout);
            requireOnlineDdlNotCancelled(ddlId, "after gate finalization");
            long appended = changeLog.highestAppendedSequence();
            if (appended > changeLog.highestForcedSequence()) {
                changeLog.forceThrough(appended, timeout);
            }
            long sealedSequence = changeLog.appendState(OnlineIndexLogRecordType.SEALED, new byte[0]);
            changeLog.forceThrough(sealedSequence, timeout);
            List<cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecord> candidates =
                    changeLog.readAll().stream()
                            .filter(record -> record.type() == OnlineIndexLogRecordType.CANDIDATE)
                            .toList();
            tracker.advanceRuntime(OnlineDdlRuntimePhase.RECONCILING, OnlineDdlWaitReason.NONE);
            int reconciliationBatch = onlineIndexRuntime.config().scanBatchRows();
            for (int start = 0; start < candidates.size(); start += reconciliationBatch) {
                requireOnlineDdlNotCancelled(ddlId, "before reconciliation delete batch");
                int end = Math.min(candidates.size(), start + reconciliationBatch);
                for (var record : candidates.subList(start, end)) {
                    OnlineIndexCandidate candidate = candidateCodec.decode(record.payload());
                    if (candidate.beforeEntry().isPresent()) {
                        staged = physical.removeSecondaryIndexEntryExact(
                                buildingDefinition, oldBinding, staged,
                                candidate.beforeEntry().orElseThrow());
                    }
                    if (candidate.afterEntry().isPresent()) {
                        staged = physical.removeSecondaryIndexEntryExact(
                                buildingDefinition, oldBinding, staged,
                                candidate.afterEntry().orElseThrow());
                    }
                }
            }
            for (int start = 0; start < candidates.size(); start += reconciliationBatch) {
                requireOnlineDdlNotCancelled(ddlId, "before reconciliation ensure batch");
                int end = Math.min(candidates.size(), start + reconciliationBatch);
                for (var record : candidates.subList(start, end)) {
                    OnlineIndexCandidate candidate = candidateCodec.decode(record.payload());
                    var identityEntry = candidate.afterEntry().orElseGet(
                            () -> candidate.beforeEntry().orElseThrow());
                    staged = physical.ensureSecondaryIndexCurrentForEntry(
                            buildingDefinition, oldBinding, staged, identityEntry);
                }
            }

            // 5. 两向只读验证与物理force完成后竞争forward fence；只有胜者才能写RECONCILED/ENGINE_DONE。
            tracker.advanceRuntime(OnlineDdlRuntimePhase.VERIFYING, OnlineDdlWaitReason.NONE);
            physical.verifySecondaryIndexBuild(buildingDefinition, oldBinding, staged,
                    onlineIndexRuntime.config().scanBatchRows());
            requireOnlineDdlNotCancelled(ddlId, "after verification before forward fence");
            physical.makeSecondaryIndexBuildDurable(oldBinding,
                    onlineIndexRuntime.gate().terminalRedoHighWater(active.id().value()), timeout);
            requireOnlineDdlNotCancelled(ddlId, "after physical force before forward fence");
            DdlControlCasResult direction = repository.ddlLog().compareAndSetControl(
                    ddlId, DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.FORWARD_ONLY, Optional.empty());
            if (!direction.changed()) {
                if (direction.observedRecord().controlState() == DdlControlState.CANCEL_REQUESTED) {
                    throw new DictionaryDdlException(
                            "online CREATE INDEX cancellation won before publish fence: ddl="
                                    + ddlId.value());
                }
                throw new DictionaryDdlLogStateException(
                        "online CREATE INDEX observed unexpected control CAS result: ddl="
                                + ddlId.value() + " control="
                                + direction.observedRecord().controlState());
            }
            forwardOnly = true;
            tracker.observeDurable(direction.observedRecord());
            tracker.advanceRuntime(OnlineDdlRuntimePhase.FORWARD_FENCED, OnlineDdlWaitReason.NONE);
            long reconciled = changeLog.appendState(
                    OnlineIndexLogRecordType.RECONCILED, new byte[0]);
            changeLog.forceThrough(reconciled, timeout);
            reconciledDurable = true;
            DdlLogRecord engineDoneRecord = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            engineDone = true;
            tracker.observeDurable(engineDoneRecord);
            tracker.advanceRuntime(OnlineDdlRuntimePhase.PUBLISHING, OnlineDdlWaitReason.NONE);
            faultInjector.afterCreateIndexEngineDone(engineDoneRecord);

            // 6. ENGINE_DONE 后 staged tree 已是完整发布候选；任何失败保留 descriptor/log/gate 供同步重启前滚。
            List<IndexStorageBinding> bindings = new ArrayList<>(oldBinding.indexes());
            bindings.add(staged.indexBinding());
            TableStorageBinding newBinding = new TableStorageBinding(oldBinding.tableId(), oldBinding.spaceId(),
                    oldBinding.path(), oldBinding.rowFormatVersion(), bindings, oldBinding.lobSegment());
            TableDefinition published = new TableDefinition(active.id(), active.schemaId(), active.name(),
                    version, TableState.ACTIVE, active.columns(), targetIndexes,
                    Optional.of(newBinding), active.options());
            sdi.write(published, timeout);
            cache.invalidateTable(active.id(), version);
            commitUpdate(version, published);
            DdlLogRecord dictionaryCommitted = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            tracker.observeDurable(dictionaryCommitted);
            faultInjector.afterCreateIndexDictionaryCommitted(published);
            cache.publishTable(published);
            physical.clearSecondaryIndexBuild(oldBinding, staged, timeout);
            DdlLogRecord committed = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
            tracker.observeDurable(committed);
            onlineIndexRuntime.gate().clearBuild(buildId);
            changeLog.close();
            onlineIndexRuntime.logFiles().delete(buildId, rowLogPath);
            cleanSnapshotPublisher.publish();
            onlineDdlRegistry.complete(ddlId, OnlineDdlTerminalResult.COMPLETED,
                    Optional.empty(), false);
            log.info("created online secondary index: table={} index={} indexId={} ddlId={} version={}",
                    name.canonicalKey(), newIndex.name().canonicalName(), newIndex.id().value(),
                    ddlId.value(), version.value());
            return published;
        } catch (RuntimeException failure) {
            if (forwardOnly || engineDone || reconciledDurable) {
                completeOnlineDdlFailed(tracker, ddlId, onlineErrorCode(failure), true);
                log.error("online CREATE INDEX failed at/after durable FORWARD_ONLY; restart must resolve marker and finish forward: ddlId={}",
                        ddlId == null ? 0 : ddlId.value(), failure);
                throw failure;
            }

            // ENGINE_DONE 前全部证据仍可回滚；abort gate 先阻止迟到 append，再回收 descriptor/segments。
            if (tracker != null && !tracker.snapshot().terminal()) {
                tracker.advanceRuntime(OnlineDdlRuntimePhase.ABORTING, OnlineDdlWaitReason.NONE);
            }
            OnlineDdlAbortReason abortReason = onlineAbortReason(failure);
            boolean cleanupSafe = true;
            if (buildId != null) {
                try {
                    if (changeLog != null && !changeLog.abortRequired()) {
                        changeLog.markAbortRequired(abortReason, timeout);
                    }
                    OnlineDdlTablePhase phase = oldBinding == null
                            ? OnlineDdlTablePhase.ABSENT
                            : onlineIndexRuntime.gate().phase(oldBinding.tableId());
                    if (phase != OnlineDdlTablePhase.ABSENT && phase != OnlineDdlTablePhase.ABORTING) {
                        onlineIndexRuntime.gate().beginAbort(buildId, abortReason);
                    }
                    if (phase != OnlineDdlTablePhase.ABSENT) {
                        onlineIndexRuntime.gate().awaitAbortQuiescence(buildId, timeout);
                    }
                } catch (RuntimeException abortFailure) {
                    failure.addSuppressed(abortFailure);
                    cleanupSafe = false;
                }
            }
            SecondaryIndexBuildDescriptor cleanupStaged = staged;
            if (cleanupSafe && cleanupStaged == null && markerPrepared && oldBinding != null) {
                try {
                    Optional<SecondaryIndexBuildDescriptor> durable =
                            physical.readSecondaryIndexBuild(oldBinding);
                    if (durable.isPresent()) {
                        SecondaryIndexBuildDescriptor observed = durable.orElseThrow();
                        DdlLogRecord marker = repository.ddlLog().find(ddlId).orElseThrow();
                        if (observed.ddlOperationId() != ddlId.value()
                                || observed.dictionaryVersion() != marker.marker().dictionaryVersion()
                                || observed.tableId() != oldBinding.tableId()
                                || observed.indexBinding().indexId() != marker.secondaryObjectId()) {
                            throw new DictionaryDdlException(
                                    "online CREATE INDEX cleanup observed another descriptor owner");
                        }
                        cleanupStaged = observed;
                    }
                } catch (RuntimeException descriptorFailure) {
                    failure.addSuppressed(descriptorFailure);
                    cleanupSafe = false;
                }
            }
            if (cleanupSafe && cleanupStaged != null && oldBinding != null) {
                try {
                    physical.rollbackSecondaryIndexBuild(oldBinding, cleanupStaged, timeout);
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                    cleanupSafe = false;
                }
            }
            boolean markerRolledBack = !markerPrepared;
            if (cleanupSafe && markerPrepared && ddlId != null) {
                try {
                    repository.ddlLog().transition(
                            ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
                    markerRolledBack = true;
                } catch (RuntimeException markerFailure) {
                    failure.addSuppressed(markerFailure);
                    cleanupSafe = false;
                }
            }
            if (cleanupSafe && markerRolledBack && buildId != null && oldBinding != null) {
                try {
                    if (onlineIndexRuntime.gate().phase(oldBinding.tableId()) != OnlineDdlTablePhase.ABSENT) {
                        onlineIndexRuntime.gate().clearBuild(buildId);
                    }
                } catch (RuntimeException gateFailure) {
                    failure.addSuppressed(gateFailure);
                    cleanupSafe = false;
                }
            }
            if (cleanupSafe && markerRolledBack && changeLog != null) {
                Path path = changeLog.path();
                try {
                    changeLog.close();
                    onlineIndexRuntime.logFiles().delete(buildId, path);
                } catch (RuntimeException fileFailure) {
                    failure.addSuppressed(fileFailure);
                }
            } else if (cleanupSafe && markerRolledBack && buildId != null) {
                try {
                    Path path = onlineIndexRuntime.logFiles().pathFor(buildId);
                    onlineIndexRuntime.logFiles().delete(buildId, path);
                } catch (RuntimeException fileFailure) {
                    failure.addSuppressed(fileFailure);
                }
            }
            if (tracker != null) {
                if (cleanupSafe && markerRolledBack) {
                    onlineDdlRegistry.complete(ddlId, OnlineDdlTerminalResult.ROLLED_BACK,
                            Optional.of(onlineErrorCode(failure)), false);
                } else {
                    completeOnlineDdlFailed(
                            tracker, ddlId, onlineErrorCode(failure), false);
                }
            }
            throw failure;
        }
    }

    /**
     * 检查 marker 的 durable control；只有 CANCEL_REQUESTED 会触发反向收敛，FORWARD_ONLY 由后续前滚路径消费。
     *
     * @param ddlId 当前 live Online DDL 的精确 marker identity
     * @param checkpoint 有界安全点名称，用于领域异常诊断
     * @throws DictionaryDdlLogStateException marker 意外消失时抛出，调用方必须 fail-closed
     * @throws OnlineDdlCancellationException durable cancel 已经胜出时抛出，coordinator 应进入回滚
     */
    private void requireOnlineDdlNotCancelled(DdlId ddlId, String checkpoint) {
        DdlLogRecord marker = repository.ddlLog().find(ddlId).orElseThrow(() ->
                new DictionaryDdlLogStateException(
                        "online DDL marker disappeared at " + checkpoint + ": ddl=" + ddlId.value()));
        if (marker.controlState() == DdlControlState.CANCEL_REQUESTED) {
            throw new OnlineDdlCancellationException(
                    "online DDL observed durable cancellation at " + checkpoint
                            + ": ddl=" + ddlId.value());
        }
    }

    /** @return marker 当前是否已持久进入 CANCEL_REQUESTED；marker 不存在时返回 false 保留原等待异常。 */
    private boolean onlineDdlCancelled(DdlId ddlId) {
        return repository.ddlLog().find(ddlId)
                .map(record -> record.controlState() == DdlControlState.CANCEL_REQUESTED)
                .orElse(false);
    }

    /**
     * 分别从 gate 和 row-log 取得单锁快照，再写入 tracker；两个下游锁从不嵌套。
     *
     * @param tracker 当前 operation 的轻量诊断 owner
     * @param tableId gate 中的精确正 table identity
     * @param changeLog 已创建的 row-log；创建前为 {@code null}
     */
    private void sampleOnlineDdl(
            OnlineDdlOperationTracker tracker, long tableId,
            FileOnlineIndexChangeLog changeLog) {
        var gate = onlineIndexRuntime.gate().snapshot(tableId);
        tracker.updateGate(gate.phase(), gate.inFlightAdmissions(),
                gate.ioLeases(), gate.terminalRedoHighWater().value());
        if (changeLog != null) {
            var logSnapshot = changeLog.snapshot();
            tracker.updateChangeLog(logSnapshot.candidateCount(),
                    logSnapshot.sizeBytes(), logSnapshot.maxBytes(),
                    logSnapshot.terminalReserveBytes(),
                    logSnapshot.highestAppendedSequence(),
                    logSnapshot.highestForcedSequence(), logSnapshot.generation());
        }
    }

    /**
     * 从通用 ALTER gate 与 journal 分别采样，不在两个内部锁之间形成嵌套等待。
     *
     * @param tracker 当前 DDL 的进程内可观察状态 owner
     * @param tableId gate 使用的正表标识，必须与 journal header 一致
     * @param changeLog 通用 ALTER journal；创建前允许为 {@code null}
     */
    private void sampleOnlineAlterDdl(
            OnlineDdlOperationTracker tracker, long tableId,
            FileOnlineAlterChangeLog changeLog) {
        var gate = onlineAlterRuntime.gate().snapshot(tableId);
        tracker.updateGate(gate.phase(), gate.inFlightAdmissions(),
                gate.ioLeases(), gate.terminalRedoHighWater().value());
        if (changeLog != null) {
            var logSnapshot = changeLog.snapshot();
            tracker.updateChangeLog(logSnapshot.candidateCount(),
                    logSnapshot.sizeBytes(), logSnapshot.maxBytes(),
                    logSnapshot.terminalReserveBytes(),
                    logSnapshot.highestAppendedSequence(),
                    logSnapshot.highestForcedSequence(), logSnapshot.generation());
        }
    }

    /** 把未能在本进程收敛的 active tracker 移入 FAILED_CLOSED history，保留恢复需求。 */
    private void completeOnlineDdlFailed(
            OnlineDdlOperationTracker tracker, DdlId ddlId,
            String errorCode, boolean forwardRecoveryRequired) {
        if (tracker == null || ddlId == null) {
            return;
        }
        onlineDdlRegistry.complete(ddlId, OnlineDdlTerminalResult.FAILED_CLOSED,
                Optional.of(errorCode), forwardRecoveryRequired);
    }

    /** @return 有界稳定诊断码，不把异常 message/stack 放入 tracker history。 */
    private static String onlineErrorCode(RuntimeException failure) {
        if (failure instanceof OnlineDdlCancellationException) {
            return "CANCEL_REQUESTED";
        }
        if (failure instanceof SecondaryIndexBuildDuplicateKeyException) {
            return "UNIQUE_CONFLICT";
        }
        if (failure instanceof MetadataLockTimeoutException) {
            return "METADATA_LOCK_TIMEOUT";
        }
        return "ONLINE_DDL_FAILED";
    }

    /** 按故障类型选择 durable abort 分类；未知的调用方取消仍归入 CANCELLED。 */
    private static OnlineDdlAbortReason onlineAbortReason(RuntimeException failure) {
        if (failure instanceof OnlineDdlCancellationException) {
            return OnlineDdlAbortReason.CANCELLED;
        }
        if (failure instanceof SecondaryIndexBuildDuplicateKeyException) {
            return OnlineDdlAbortReason.UNIQUE_CONFLICT;
        }
        if (failure instanceof MetadataLockTimeoutException) {
            return OnlineDdlAbortReason.METADATA_LOCK_TIMEOUT;
        }
        if (failure instanceof cn.zhangyis.db.storage.api.ddl.TableDdlStorageException) {
            return OnlineDdlAbortReason.VALIDATION_FAILED;
        }
        return OnlineDdlAbortReason.CANCELLED;
    }

    /**
     * 原子删除既有 ACTIVE 表的一个二级索引。v1 全程持 table MDL X，先提交不含索引的新 DD，再回收
     * leaf/non-leaf segment；聚簇索引和 {@code IF EXISTS} 语义不在本入口支持。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得 schema IX/table X，在锁内重读 ACTIVE aggregate，按名称定位唯一二级索引及同 ordinal
     *     物理 binding，并等待 purge history 与旧 metadata pin 排空。</li>
     *     <li>预留 DDL/version 并写 DROP_INDEX PREPARED marker；随后把目标完整 binding 以 DROP action
     *     写入 page3，建立 crash recovery 的精确资源所有权。</li>
     *     <li>组装只删除一个逻辑/物理索引的新 ACTIVE aggregate，先写 SDI 再提交 DD；DD 是提交裁决点，
     *     outcome 不确定时保留 descriptor，恢复按当前 DD 前滚或回滚。</li>
     *     <li>补 DICTIONARY_COMMITTED 并发布 cache；此后旧索引已经逻辑不可达，才允许单 MTR 回收两个
     *     segment 并清 descriptor。</li>
     *     <li>物理收敛后写 ENGINE_DONE 与 COMMITTED；任一后置失败都保留可幂等恢复的 marker/新 DD。</li>
     * </ol>
     *
     * @param owner 本次独立 DDL statement 的 MDL owner，不能复用用户事务 owner
     * @param command 完整表名和目标索引逻辑名称；不存在目标必须报错
     * @param timeout MDL、purge/pin、WAL 与文件 force 共用的正有界时限
     * @return 不再包含目标 index definition/binding 的新 ACTIVE table aggregate
     * @throws DatabaseValidationException owner/command/timeout 无效时抛出
     * @throws DictionaryObjectNotFoundException schema、table 或目标索引不存在时抛出
     * @throws DictionaryDdlException 目标是聚簇索引、binding 不一致或持久协作失败时抛出；调用方不得自行释放 segment
     */
    public TableDefinition dropSecondaryIndex(MdlOwnerId owner,
                                              DropSecondaryIndexCommand command,
                                              Duration timeout) {
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (command == null) {
            throw new DatabaseValidationException("drop secondary index command must not be null");
        }
        if (onlineIndexRuntime != null && indexRetirementBarrier != null) {
            return dropSecondaryIndexOnline(owner, command, timeout);
        }
        QualifiedTableName name = command.table();
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner, MdlKey.table(name.canonicalKey()),
                     MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), timeout)) {
            // 1. MDL X 下重读名称与 binding，确保 Parser/Binder 的纯逻辑命令不会携带陈旧 index identity。
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "schema does not exist: " + name.schema().displayName()));
            TableDefinition active = repository.findTable(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "table does not exist: " + name.canonicalKey()));
            int indexOrdinal = -1;
            for (int ordinal = 0; ordinal < active.indexes().size(); ordinal++) {
                if (active.indexes().get(ordinal).name().equals(command.indexName())) {
                    indexOrdinal = ordinal;
                    break;
                }
            }
            if (indexOrdinal < 0) {
                throw new DictionaryObjectNotFoundException(
                        "index does not exist: " + command.indexName().displayName());
            }
            IndexDefinition removedIndex = active.indexes().get(indexOrdinal);
            if (removedIndex.clustered()) {
                throw new DictionaryDdlException(
                        "DROP INDEX cannot remove clustered primary index: "
                                + removedIndex.name().displayName());
            }
            TableStorageBinding oldBinding = active.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException(
                            "ACTIVE table has no physical binding: " + active.id().value()));
            if (indexOrdinal >= oldBinding.indexes().size()
                    || oldBinding.indexes().get(indexOrdinal).indexId() != removedIndex.id().value()) {
                throw new DictionaryDdlException(
                        "DROP INDEX logical/physical ordinal identity mismatch: index="
                                + removedIndex.id().value());
            }
            IndexStorageBinding removedBinding = oldBinding.indexes().get(indexOrdinal);
            purgeBarrier.awaitUnreferenced(active.id().value(), timeout);
            if (!cache.awaitUnpinned(active.id(), timeout)) {
                throw new DictionaryDdlException(
                        "timed out waiting old dictionary pins before DROP INDEX: "
                                + active.id().value());
            }

            // 2. PREPARED 固定 table/index/version identity；descriptor durable 前不得改变 DD 可达性。
            DictionaryIdAllocation ids = control.reserve(
                    new DictionaryIdRequest(0, 0, 0, 0, 1, 1));
            DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            List<IndexDefinition> targetIndexes = new ArrayList<>(active.indexes());
            targetIndexes.remove(indexOrdinal);
            targetIndexes = List.copyOf(targetIndexes);
            TableDefinition plannedTarget = logicalVersion(
                    active, version, TableState.ACTIVE, targetIndexes);
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), version.value(), active.id().value()),
                    removedIndex.id().value(), DdlLogOperation.DROP_INDEX, DdlLogPhase.PREPARED,
                    oldBinding.spaceId(), oldBinding.path(), Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(schemaDigests.digest(schema, active, oldBinding.rowFormatVersion())),
                    Optional.empty(), Optional.of(schemaDigests.digest(
                            schema, plannedTarget, oldBinding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            faultInjector.afterDropIndexPrepared(prepared);
            SecondaryIndexDropDescriptor staged = physical.beginSecondaryIndexDrop(
                    oldBinding, ddlId.value(), version.value(), removedBinding, timeout);
            faultInjector.afterDropIndexStaged(staged);

            // 3. 新 aggregate 精确删除同 ordinal 的 definition/binding；SDI 可先写，但 committed DD 是唯一裁决点。
            List<IndexStorageBinding> bindings = new ArrayList<>(oldBinding.indexes());
            bindings.remove(indexOrdinal);
            TableStorageBinding newBinding = new TableStorageBinding(
                    oldBinding.tableId(), oldBinding.spaceId(), oldBinding.path(),
                    oldBinding.rowFormatVersion(), bindings, oldBinding.lobSegment());
            TableDefinition published = new TableDefinition(
                    active.id(), active.schemaId(), active.name(), version, TableState.ACTIVE,
                    active.columns(), targetIndexes, Optional.of(newBinding), active.options());
            sdi.write(published, timeout);
            // 提交点前建立 publication barrier；未知结果下旧 cache 不得继续把已删除索引视为权威 binding。
            cache.invalidateTable(active.id(), version);
            try {
                commitUpdate(version, published);
            } catch (RuntimeException publishFailure) {
                log.warn("DROP INDEX DD publish outcome is uncertain; retaining drop descriptor: "
                                + "tableId={} indexId={} ddlId={}",
                        active.id().value(), removedIndex.id().value(), ddlId.value(), publishFailure);
                throw publishFailure;
            }
            // 此接缝覆盖“新 DD durable、marker 仍 PREPARED”的关键恢复窗口。
            faultInjector.afterDropIndexDictionaryPublished(published);

            // 4. marker/cache 都不得早于 DD；segment free 则严格晚于新 cache 发布。
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            faultInjector.afterDropIndexDictionaryCommitted(published);
            cache.publishTable(published);
            physical.finishSecondaryIndexDrop(newBinding, staged, timeout);

            // 5. descriptor 与两个 segment 已在同一 MTR 收敛，最后只需补审计阶段。
            DdlLogRecord engineDone = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            faultInjector.afterDropIndexEngineDone(engineDone);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            log.info("dropped secondary index: table={} index={} indexId={} ddlId={} version={}",
                    name.canonicalKey(), removedIndex.name().canonicalName(),
                    removedIndex.id().value(), ddlId.value(), version.value());
            cleanSnapshotPublisher.publish();
            return published;
        }
    }

    /**
     * Online删除一个普通二级索引。prepare期持SU并允许DML继续维护source index；短final X只负责冻结source、
     * 安装retirement fence和发布target DD，随后降回SU并在不阻塞业务DML的情况下延迟回收segment。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得schema IX/table SU，重读ACTIVE aggregate并定位exact non-clustered definition/binding；预留DDL/version、
     *     注册tracker并写ONLINE_DROP_INDEX_V1 PREPARED/OPEN marker。</li>
     *     <li>在gate声明RETIREMENT_OPEN后写durable DROP descriptor；source DD仍可达，普通DML不写row-log并继续维护旧索引。</li>
     *     <li>等待SU→X，重新计算source digest，再让gate排空跨界admission/write transaction；取消在本阶段仍可胜出。</li>
     *     <li>final X下捕获transaction high-water与source pin version，先一次性安装fence，再竞争OPEN→FORWARD_ONLY；
     *     取消先胜出则转入确定性descriptor回滚。</li>
     *     <li>前滚胜出后写target SDI、exact commit不含索引的新DD、推进DICTIONARY_COMMITTED并发布cache；提交后绝不重新加入索引。</li>
     *     <li>X降回SU并清gate，使target DML继续；有界等待history/pin fence，安全后单MTR回收两个segment并清descriptor。</li>
     *     <li>推进ENGINE_DONE/COMMITTED、发布clean snapshot与terminal tracker；forward后的任一失败只保留证据供恢复续作。</li>
     * </ol>
     *
     * @param owner 本次独立DDL statement的MDL owner；不能复用用户事务owner
     * @param command 完整表名与待删二级索引名称；不存在或聚簇索引会在持久副作用前拒绝
     * @param timeout MDL、gate、WAL、history与pin等待的正有界时限
     * @return 已逻辑删除目标索引且物理retirement完成的新ACTIVE aggregate
     * @throws DatabaseValidationException owner、command或timeout无效时抛出
     * @throws DictionaryObjectNotFoundException schema、table或index不存在时抛出
     * @throws OnlineDdlCancellationException durable取消在forward fence前胜出时抛出，descriptor与marker已回滚或保留恢复证据
     * @throws OnlineDdlRetirementTimeoutException target DD已提交但旧资源尚不安全时抛出，只允许前滚恢复
     * @throws DictionaryDdlException binding/digest/physical协作失败时抛出；forward后调用方必须重启续作
     */
    private TableDefinition dropSecondaryIndexOnline(MdlOwnerId owner,
                                                     DropSecondaryIndexCommand command,
                                                     Duration timeout) {
        QualifiedTableName name = command.table();
        OnlineIndexBuildId buildId = null;
        SecondaryIndexDropDescriptor staged = null;
        TableStorageBinding sourceBinding = null;
        DdlId ddlId = null;
        OnlineDdlOperationTracker tracker = null;
        boolean markerPrepared = false;
        boolean forwardOnly = false;
        boolean targetPublished = false;
        boolean gateCleared = false;

        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner,
                     MdlKey.table(name.canonicalKey()), MdlMode.SHARED_UPGRADABLE,
                     MdlDuration.TRANSACTION), timeout)) {
            // 1. SU排除同表DDL但兼容普通DML；所有logical/physical identity在prepare前从同一aggregate冻结。
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "schema does not exist: " + name.schema().displayName()));
            TableDefinition source = repository.findTable(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "table does not exist: " + name.canonicalKey()));
            int indexOrdinal = -1;
            for (int ordinal = 0; ordinal < source.indexes().size(); ordinal++) {
                if (source.indexes().get(ordinal).name().equals(command.indexName())) {
                    indexOrdinal = ordinal;
                    break;
                }
            }
            if (indexOrdinal < 0) {
                throw new DictionaryObjectNotFoundException(
                        "index does not exist: " + command.indexName().displayName());
            }
            IndexDefinition removedIndex = source.indexes().get(indexOrdinal);
            if (removedIndex.clustered()) {
                throw new DictionaryDdlException(
                        "DROP INDEX cannot remove clustered primary index: "
                                + removedIndex.name().displayName());
            }
            sourceBinding = source.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException(
                            "ACTIVE table has no physical binding: " + source.id().value()));
            if (indexOrdinal >= sourceBinding.indexes().size()
                    || sourceBinding.indexes().get(indexOrdinal).indexId() != removedIndex.id().value()) {
                throw new DictionaryDdlException(
                        "DROP INDEX logical/physical ordinal identity mismatch: index="
                                + removedIndex.id().value());
            }
            IndexStorageBinding removedBinding = sourceBinding.indexes().get(indexOrdinal);
            DictionaryIdAllocation ids = control.reserve(
                    new DictionaryIdRequest(0, 0, 0, 0, 1, 1));
            DictionaryVersion targetVersion = DictionaryVersion.of(ids.dictionaryVersion());
            ddlId = DdlId.of(ids.firstDdlId());
            buildId = OnlineIndexBuildId.of(ddlId.value());
            List<IndexDefinition> targetIndexes = new ArrayList<>(source.indexes());
            targetIndexes.remove(indexOrdinal);
            targetIndexes = List.copyOf(targetIndexes);
            TableDefinition plannedTarget = logicalVersion(
                    source, targetVersion, TableState.ACTIVE, targetIndexes);
            tracker = onlineDdlRegistry.register(new OnlineDdlOperationIdentity(
                            ddlId, DdlLogOperation.DROP_INDEX, source.id().value(),
                            removedIndex.id().value(), name.canonicalKey(),
                            removedIndex.name().canonicalName(), source.version().value(),
                            targetVersion.value(), owner.value(), false,
                            java.util.OptionalLong.empty()),
                    DdlExecutionProtocol.ONLINE_DROP_INDEX_V1);
            tracker.advanceRuntime(OnlineDdlRuntimePhase.ACTIVATING, OnlineDdlWaitReason.NONE);
            if (!tracker.beginDurablePrepare()) {
                throw new OnlineDdlCancellationException(
                        "online DROP INDEX cancelled before durable prepare: ddl=" + ddlId.value());
            }
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), targetVersion.value(), source.id().value()),
                    removedIndex.id().value(), DdlLogOperation.DROP_INDEX, DdlLogPhase.PREPARED,
                    sourceBinding.spaceId(), sourceBinding.path(), Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ONLINE_DROP_INDEX_V1,
                    Optional.of(schemaDigests.digest(
                            schema, source, sourceBinding.rowFormatVersion())),
                    Optional.empty(), Optional.of(schemaDigests.digest(
                            schema, plannedTarget, sourceBinding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            try {
                repository.ddlLog().prepare(prepared);
            } catch (RuntimeException prepareFailure) {
                tracker.failDurablePrepare("MARKER_PREPARE_FAILED");
                throw prepareFailure;
            }
            markerPrepared = true;
            tracker.markDurablePrepared(prepared);
            faultInjector.afterDropIndexPrepared(prepared);
            requireOnlineDdlNotCancelled(ddlId, "after online DROP prepare");

            // 2. gate只声明owner，不冻结DML；page3 descriptor不修改index页，SU足以排除其它DDL footer竞争。
            onlineIndexRuntime.gate().beginRetirement(
                    source.id().value(), buildId, timeout);
            sampleOnlineDdl(tracker, source.id().value(), null);
            staged = physical.beginSecondaryIndexDrop(
                    sourceBinding, ddlId.value(), targetVersion.value(), removedBinding, timeout);
            faultInjector.afterDropIndexStaged(staged);
            requireOnlineDdlNotCancelled(ddlId, "after online DROP descriptor");

            // 3. final X排除旧metadata lease；gate随后复核DML admission/transaction投影，二者共同封闭source writer集合。
            tracker.advanceRuntime(
                    OnlineDdlRuntimePhase.WAITING_FINAL_MDL, OnlineDdlWaitReason.METADATA_LOCK);
            try {
                locks.upgrade(tableTicket, MdlMode.EXCLUSIVE, timeout);
            } catch (MetadataLockTimeoutException waitFailure) {
                if (onlineDdlCancelled(ddlId)) {
                    throw new OnlineDdlCancellationException(
                            "online DROP INDEX cancelled during final MDL wait: ddl="
                                    + ddlId.value(), waitFailure);
                }
                throw waitFailure;
            }
            requireOnlineDdlNotCancelled(ddlId, "after online DROP final MDL");
            TableDefinition finalSource = repository.findTable(schema.id(), name.table())
                    .orElseThrow(() -> new DictionaryDdlException(
                            "online DROP INDEX source disappeared before final X"));
            requireLiveDigest(prepared.sourceSchemaDigest().orElseThrow(),
                    schemaDigests.digest(schema, finalSource, sourceBinding.rowFormatVersion()),
                    ddlId, "DROP source at final X");
            tracker.advanceRuntime(
                    OnlineDdlRuntimePhase.FINALIZING, OnlineDdlWaitReason.GATE_QUIESCENCE);
            onlineIndexRuntime.gate().beginSeal(buildId, timeout);
            requireOnlineDdlNotCancelled(ddlId, "after online DROP gate finalization");

            // 4. 当前page3格式没有独立generation字段；target dictionary version是单slot descriptor的单调代际并与owner精确CAS。
            DdlRetirementFence fence = indexRetirementBarrier.captureIndexFence(
                    source.id().value(), source.version().value(), removedIndex.id().value(),
                    staged.dictionaryVersion(), ddlId.value());
            DdlLogRecord fenced = repository.ddlLog().installRetirementFence(
                    ddlId, DdlLogPhase.PREPARED, DdlControlState.OPEN, fence);
            tracker.observeDurable(fenced);
            tracker.updateRetirement(true, false);
            requireOnlineDdlNotCancelled(ddlId, "after online DROP retirement fence");
            DdlControlCasResult direction = repository.ddlLog().compareAndSetControl(
                    ddlId, DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.FORWARD_ONLY, Optional.empty());
            if (!direction.changed()) {
                if (direction.observedRecord().controlState() == DdlControlState.CANCEL_REQUESTED) {
                    throw new OnlineDdlCancellationException(
                            "online DROP INDEX cancellation won before publish fence: ddl="
                                    + ddlId.value());
                }
                throw new DictionaryDdlLogStateException(
                        "online DROP INDEX observed unexpected control CAS result: ddl="
                                + ddlId.value() + " control="
                                + direction.observedRecord().controlState());
            }
            forwardOnly = true;
            tracker.observeDurable(direction.observedRecord());
            tracker.advanceRuntime(OnlineDdlRuntimePhase.FORWARD_FENCED, OnlineDdlWaitReason.NONE);
            faultInjector.afterDropIndexForwardFenced(direction.observedRecord());

            // 5. FORWARD_ONLY之后DD是唯一方向；publication outcome不确定时保留marker/fence/descriptor供恢复分类。
            List<IndexStorageBinding> targetBindings = new ArrayList<>(sourceBinding.indexes());
            targetBindings.remove(indexOrdinal);
            TableStorageBinding publishedBinding = new TableStorageBinding(
                    sourceBinding.tableId(), sourceBinding.spaceId(), sourceBinding.path(),
                    sourceBinding.rowFormatVersion(), targetBindings, sourceBinding.lobSegment());
            TableDefinition published = new TableDefinition(
                    source.id(), source.schemaId(), source.name(), targetVersion, TableState.ACTIVE,
                    source.columns(), targetIndexes, Optional.of(publishedBinding), source.options());
            tracker.advanceRuntime(OnlineDdlRuntimePhase.PUBLISHING, OnlineDdlWaitReason.NONE);
            sdi.write(published, timeout);
            cache.invalidateTable(source.id(), targetVersion);
            try {
                commitUpdate(targetVersion, published);
                targetPublished = true;
            } catch (RuntimeException publishFailure) {
                log.warn("online DROP INDEX DD publish outcome is uncertain; retaining retirement evidence: "
                                + "tableId={} indexId={} ddlId={}",
                        source.id().value(), removedIndex.id().value(), ddlId.value(), publishFailure);
                throw publishFailure;
            }
            faultInjector.afterDropIndexDictionaryPublished(published);
            DdlLogRecord dictionaryCommitted = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            tracker.observeDurable(dictionaryCommitted);
            faultInjector.afterDropIndexDictionaryCommitted(published);
            cache.publishTable(published);

            // 6. target已发布后先恢复业务DML；SU继续排除其它DDL，retirement等待不持MDL manager mutex或storage页资源。
            locks.downgrade(tableTicket, MdlMode.SHARED_UPGRADABLE);
            onlineIndexRuntime.gate().clearBuild(buildId);
            gateCleared = true;
            tracker.advanceRuntime(
                    OnlineDdlRuntimePhase.RETIRING, OnlineDdlWaitReason.RETIREMENT_FENCE);
            sampleOnlineDdl(tracker, source.id().value(), null);
            indexRetirementBarrier.awaitIndexSafe(fence, timeout);
            tracker.updateRetirement(true, true);
            tracker.advanceRuntime(OnlineDdlRuntimePhase.RETIRING, OnlineDdlWaitReason.NONE);
            physical.finishSecondaryIndexDrop(publishedBinding, staged, timeout);

            // 7. segment与descriptor已在单MTR收敛，最后只推进审计阶段并把弱一致tracker转入terminal history。
            DdlLogRecord engineDone = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            tracker.observeDurable(engineDone);
            faultInjector.afterDropIndexEngineDone(engineDone);
            DdlLogRecord committed = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            tracker.observeDurable(committed);
            cleanSnapshotPublisher.publish();
            onlineDdlRegistry.complete(ddlId, OnlineDdlTerminalResult.COMPLETED,
                    Optional.empty(), false);
            log.info("dropped online secondary index: table={} index={} indexId={} ddlId={} version={}",
                    name.canonicalKey(), removedIndex.name().canonicalName(),
                    removedIndex.id().value(), ddlId.value(), targetVersion.value());
            return published;
        } catch (RuntimeException failure) {
            if (forwardOnly) {
                // target提交后即使retirement timeout也必须让新DML继续；这里只清运行期gate，不触碰descriptor/segment。
                if (targetPublished && !gateCleared && buildId != null && sourceBinding != null) {
                    try {
                        if (onlineIndexRuntime.gate().phase(sourceBinding.tableId())
                                != OnlineDdlTablePhase.ABSENT) {
                            onlineIndexRuntime.gate().clearBuild(buildId);
                        }
                    } catch (RuntimeException gateFailure) {
                        failure.addSuppressed(gateFailure);
                    }
                }
                completeOnlineDdlFailed(tracker, ddlId, onlineErrorCode(failure), true);
                log.error("online DROP INDEX failed after durable FORWARD_ONLY; restart must continue retirement: ddlId={}",
                        ddlId == null ? 0 : ddlId.value(), failure);
                throw failure;
            }

            // forward fence前只有source DD可达；先阻止迟到gate用户，再exact清descriptor，最后写ROLLED_BACK。
            if (tracker != null && !tracker.snapshot().terminal()) {
                tracker.advanceRuntime(OnlineDdlRuntimePhase.ABORTING, OnlineDdlWaitReason.NONE);
            }
            boolean cleanupSafe = true;
            if (buildId != null && sourceBinding != null) {
                try {
                    OnlineDdlTablePhase phase = onlineIndexRuntime.gate().phase(sourceBinding.tableId());
                    if (phase != OnlineDdlTablePhase.ABSENT && phase != OnlineDdlTablePhase.ABORTING) {
                        onlineIndexRuntime.gate().beginAbort(
                                buildId, onlineAbortReason(failure));
                    }
                    if (phase != OnlineDdlTablePhase.ABSENT) {
                        onlineIndexRuntime.gate().awaitAbortQuiescence(buildId, timeout);
                    }
                } catch (RuntimeException gateFailure) {
                    failure.addSuppressed(gateFailure);
                    cleanupSafe = false;
                }
            }
            SecondaryIndexDropDescriptor cleanupStaged = staged;
            if (cleanupSafe && cleanupStaged == null && markerPrepared
                    && sourceBinding != null && ddlId != null) {
                try {
                    Optional<SecondaryIndexDropDescriptor> durable =
                            physical.readSecondaryIndexDrop(sourceBinding);
                    if (durable.isPresent()) {
                        SecondaryIndexDropDescriptor observed = durable.orElseThrow();
                        DdlLogRecord marker = repository.ddlLog().find(ddlId).orElseThrow();
                        if (observed.ddlOperationId() != ddlId.value()
                                || observed.dictionaryVersion()
                                != marker.marker().dictionaryVersion()
                                || observed.tableId() != sourceBinding.tableId()
                                || observed.indexBinding().indexId()
                                != marker.secondaryObjectId()) {
                            throw new DictionaryDdlException(
                                    "online DROP INDEX cleanup observed another descriptor owner");
                        }
                        cleanupStaged = observed;
                    }
                } catch (RuntimeException descriptorFailure) {
                    failure.addSuppressed(descriptorFailure);
                    cleanupSafe = false;
                }
            }
            if (cleanupSafe && cleanupStaged != null && sourceBinding != null) {
                try {
                    physical.rollbackSecondaryIndexDrop(sourceBinding, cleanupStaged, timeout);
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                    cleanupSafe = false;
                }
            }
            boolean markerRolledBack = !markerPrepared;
            if (cleanupSafe && markerPrepared && ddlId != null) {
                try {
                    DdlLogRecord rolledBack = repository.ddlLog().transition(
                            ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
                    if (tracker != null) {
                        tracker.observeDurable(rolledBack);
                    }
                    markerRolledBack = true;
                } catch (RuntimeException markerFailure) {
                    failure.addSuppressed(markerFailure);
                    cleanupSafe = false;
                }
            }
            if (cleanupSafe && markerRolledBack && buildId != null && sourceBinding != null) {
                try {
                    if (onlineIndexRuntime.gate().phase(sourceBinding.tableId())
                            != OnlineDdlTablePhase.ABSENT) {
                        onlineIndexRuntime.gate().clearBuild(buildId);
                    }
                } catch (RuntimeException gateFailure) {
                    failure.addSuppressed(gateFailure);
                    cleanupSafe = false;
                }
            }
            if (tracker != null) {
                if (cleanupSafe && markerRolledBack) {
                    onlineDdlRegistry.complete(ddlId, OnlineDdlTerminalResult.ROLLED_BACK,
                            Optional.of(onlineErrorCode(failure)), false);
                } else {
                    completeOnlineDdlFailed(
                            tracker, ddlId, onlineErrorCode(failure), false);
                }
            }
            throw failure;
        }
    }

    /**
     * DROP 两版本状态机：持 table MDL X 时先等待 persistent history 表引用归零，再在 cache 建立不可回退的本地
     * 准入屏障并提交 DROP_PENDING；随后等待旧 cache pin、执行物理删除，最后提交 DROPPED。catalog publish 结果不确定时
     * 屏障保留到重启，避免旧内存快照复活表；
     * 若物理/最终 publish 失败，DROP_PENDING+binding 是启动恢复的权威续作输入。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得 schema IX 与 table X MDL，解析唯一 ACTIVE 表及其 durable storage binding。</li>
     *     <li>在尚未发布 DROP_PENDING、未持有 MTR/page/file 资源时等待 persistent history 表引用归零；超时保持 ACTIVE。</li>
     *     <li>预留单调 ddl/版本并写 PREPARED，先失效 cache 准入，再持久发布 DROP_PENDING 与 DICTIONARY_COMMITTED；
     *         发布结果不确定时保留屏障等待重启裁决。</li>
     *     <li>等待旧 dictionary pin 排空后执行物理 DROP并写 ENGINE_DONE；失败保留 DROP_PENDING+binding 供恢复续作。</li>
     *     <li>物理文件删除成功后发布 DROPPED 新版本，再写 COMMITTED，完成可审计逻辑生命周期。</li>
     * </ol>
     *
     * @param owner   本次 DDL 的 MDL owner；schema/table ticket 都归该 owner 生命周期。
     * @param name    待删除表的完整 schema/table 名称，不能为 {@code null}。
     * @param timeout MDL、history barrier、cache pin 与物理 DROP 共用的正有界等待时间。
     * @throws DatabaseValidationException owner/name/timeout 无效时抛出，且不改变 catalog/cache/文件。
     * @throws cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException history 在 timeout 内仍引用表时抛出，表保持 ACTIVE。
     * @throws DictionaryDdlException metadata/binding 不完整、cache pin 排空或物理 DROP 失败时抛出；DROP_PENDING durable 后由恢复续作。
     */
    public void dropTable(MdlOwnerId owner, QualifiedTableName name, Duration timeout) {
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (name == null) {
            throw new DatabaseValidationException("drop table name must not be null");
        }
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner, MdlKey.table(name.canonicalKey()),
                     MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), timeout)) {
            // 1、MDL 阻断新表访问后读取 ACTIVE metadata；此时还未改 cache/catalog/物理文件。
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("schema does not exist: " + name.schema().displayName()));
            TableDefinition active = repository.findTableForRecovery(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("table does not exist: " + name.canonicalKey()));
            TableStorageBinding binding = active.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException("ACTIVE table has no physical binding: " + active.id().value()));

            if (active.state() == TableState.RECOVERY_UNAVAILABLE
                    || active.state() == TableState.RECOVERY_DISCARDED) {
                dropRecoveryIsolatedLocked(schema, active, binding, timeout);
                return;
            }
            if (active.state() != TableState.ACTIVE) {
                throw new DictionaryDdlException("table is not ACTIVE or recovery-isolated: "
                        + active.id().value() + " state=" + active.state());
            }

            // 2、history 仍引用表 metadata 时不能发布 DROP_PENDING；等待只持 Java Condition，不持存储资源。
            purgeBarrier.awaitUnreferenced(active.id().value(), timeout);

            // 3、barrier 清零后才建立不可回退 cache 屏障并持久发布 DROP_PENDING。
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 0, 0, 1, 2));
            DictionaryVersion pendingVersion = DictionaryVersion.of(ids.dictionaryVersion());
            DictionaryVersion droppedVersion = DictionaryVersion.of(ids.dictionaryVersion() + 1);
            TableDefinition pending = lifecycle(active, pendingVersion, TableState.DROP_PENDING);
            TableDefinition dropped = lifecycle(pending, droppedVersion, TableState.DROPPED);
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), pendingVersion.value(), active.id().value()),
                    0L, DdlLogOperation.DROP_TABLE, DdlLogPhase.PREPARED,
                    binding.spaceId(), binding.path(), Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(schemaDigests.digest(schema, active, binding.rowFormatVersion())),
                    Optional.of(schemaDigests.digest(schema, pending, binding.rowFormatVersion())),
                    Optional.of(schemaDigests.digest(schema, dropped, binding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            faultInjector.afterDropPrepared(prepared);
            // 先阻断本地重载。append 报错无法证明 DROP_PENDING 未 durable，失败后只能由重启读取 catalog 裁决。
            cache.invalidateTable(active.id(), pendingVersion);
            try {
                commitUpdate(pendingVersion, pending);
            } catch (RuntimeException publishFailure) {
                log.warn("dictionary DROP_PENDING publish outcome is uncertain; retaining cache barrier: "
                                + "tableId={} version={}", active.id().value(), pendingVersion.value(),
                        publishFailure);
                throw publishFailure;
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            faultInjector.afterDropPendingPublished(pending);

            // 4、DROP_PENDING 已 durable；排空旧 pin 后删除表空间，失败由同一 pending binding 恢复。
            if (!cache.awaitUnpinned(active.id(), timeout)) {
                throw new DictionaryDdlException("timed out waiting dictionary pins before DROP: "
                        + active.id().value());
            }
            physical.dropTable(binding, timeout);
            DdlLogRecord engineDone = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            faultInjector.afterDropEngineDone(engineDone);

            // 5、物理删除完成后发布 DROPPED，普通 lookup 从此永久不可见。
            commitUpdate(droppedVersion, dropped);
            faultInjector.afterDropDictionaryCommitted(dropped);
            repository.ddlLog().transition(ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            log.info("dropped table: name={} tableId={} ddlId={} version={}", name.canonicalKey(),
                    active.id().value(), ids.firstDdlId(), droppedVersion.value());
            cleanSnapshotPublisher.publish();
        }
    }

    /** 将 ACTIVE 表转换为 DISCARDED，并把物理文件移入受控 quarantine。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param owner 参与 {@code discardTablespace} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param quarantine 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DictionaryDdlException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    public void discardTablespace(MdlOwnerId owner, QualifiedTableName name, Path quarantine, Duration timeout) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        if (name == null || quarantine == null) {
            throw new DatabaseValidationException("discard table name/quarantine must not be null");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        quarantine = checkedTransferPath(quarantine);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner, MdlKey.table(name.canonicalKey()),
                     MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), timeout)) {
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("schema does not exist: " + name.schema().displayName()));
            TableDefinition active = repository.findTable(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("table does not exist: " + name.canonicalKey()));
            if (active.state() != TableState.ACTIVE) {
                throw new DictionaryDdlException("table is not ACTIVE: " + active.id().value());
            }
            TableStorageBinding binding = active.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException("ACTIVE table has no physical binding: " + active.id().value()));
            Path expectedQuarantine = transferPath("discarded", active.id(), binding);
            if (!quarantine.equals(expectedQuarantine)) {
                throw new DatabaseValidationException(
                        "DISCARD target must match current table/space identity: " + expectedQuarantine);
            }
            purgeBarrier.awaitUnreferenced(active.id().value(), timeout);
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 0, 0, 1, 2));
            DictionaryVersion pendingVersion = DictionaryVersion.of(ids.dictionaryVersion());
            DictionaryVersion discardedVersion = DictionaryVersion.of(ids.dictionaryVersion() + 1);
            TableDefinition pending = lifecycle(
                    active, pendingVersion, TableState.DISCARD_PENDING);
            TableDefinition discarded = lifecycle(
                    active, discardedVersion, TableState.DISCARDED);
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), pendingVersion.value(), active.id().value()),
                    0L, DdlLogOperation.DISCARD_TABLESPACE, DdlLogPhase.PREPARED,
                    binding.spaceId(), binding.path(), Optional.of(quarantine), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(schemaDigests.digest(schema, active, binding.rowFormatVersion())),
                    Optional.of(schemaDigests.digest(schema, pending, binding.rowFormatVersion())),
                    Optional.of(schemaDigests.digest(schema, discarded, binding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            cache.invalidateTable(active.id(), pendingVersion);
            commitUpdate(pendingVersion, pending);
            repository.ddlLog().transition(ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            if (!cache.awaitUnpinned(active.id(), timeout)) {
                throw new DictionaryDdlException("timed out waiting dictionary pins before DISCARD: " + active.id().value());
            }
            physical.discardTablespace(binding, quarantine, timeout);
            repository.ddlLog().transition(ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            commitUpdate(discardedVersion, discarded);
            repository.ddlLog().transition(ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            cleanSnapshotPublisher.publish();
        }
    }

    /** 校验外部 DISCARDED 文件并重新挂载为 ACTIVE。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param owner 参与 {@code importTablespace} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param source 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param identity 表空间文件或 segment 的稳定身份与生命周期快照；不得为 {@code null}，必须与已打开文件和当前 generation 一致
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DictionaryDdlException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    public void importTablespace(MdlOwnerId owner, QualifiedTableName name, Path source,
                                 TablespaceFileIdentity identity, Duration timeout) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        if (name == null || source == null || identity == null) {
            throw new DatabaseValidationException("import table/source/identity must not be null");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        source = checkedTransferPath(source);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner, MdlKey.table(name.canonicalKey()),
                     MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), timeout)) {
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("schema does not exist: " + name.schema().displayName()));
            TableDefinition discarded = repository.findTableForRecovery(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("table does not exist: " + name.canonicalKey()));
            if (discarded.state() != TableState.DISCARDED) {
                throw new DictionaryDdlException("table is not DISCARDED: " + discarded.id().value());
            }
            TableStorageBinding binding = discarded.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException("DISCARDED table has no physical binding: " + discarded.id().value()));
            Path expectedSource = transferPath("incoming", discarded.id(), binding);
            if (!source.equals(expectedSource)) {
                throw new DatabaseValidationException(
                        "IMPORT source must match current table/space identity: " + expectedSource);
            }
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 0, 0, 1, 2));
            DictionaryVersion pendingVersion = DictionaryVersion.of(ids.dictionaryVersion());
            DictionaryVersion activeVersion = DictionaryVersion.of(ids.dictionaryVersion() + 1);
            TableDefinition pending = lifecycle(
                    discarded, pendingVersion, TableState.IMPORT_PENDING);
            TableDefinition active = lifecycle(
                    discarded, activeVersion, TableState.ACTIVE);
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), pendingVersion.value(), discarded.id().value()),
                    0L, DdlLogOperation.IMPORT_TABLESPACE, DdlLogPhase.PREPARED,
                    binding.spaceId(), binding.path(), Optional.of(source), Optional.of(identity),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(schemaDigests.digest(schema, discarded, binding.rowFormatVersion())),
                    Optional.of(schemaDigests.digest(schema, pending, binding.rowFormatVersion())),
                    Optional.of(schemaDigests.digest(schema, active, binding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            cache.invalidateTable(discarded.id(), pendingVersion);
            commitUpdate(pendingVersion, pending);
            repository.ddlLog().transition(ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            physical.importTablespace(binding, source, identity, timeout);
            repository.ddlLog().transition(ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            commitUpdate(activeVersion, active);
            cache.publishTable(active);
            repository.ddlLog().transition(ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            cleanSnapshotPublisher.publish();
        }
    }

    /**
     * 使用实例固定 quarantine 路径执行 DISCARD；SQL 层只能提交逻辑表名，不能影响主机文件路径。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>只读解析 committed table identity，为受控文件名提供 table/space 诊断信息。</li>
     *     <li>创建 {@code tablespace-transfer/discarded} 目录并规范化固定目标路径。</li>
     *     <li>进入既有 DISCARD 状态机；其在 table MDL X 下重读 identity 并拒绝并发 drop/recreate 造成的不匹配。</li>
     *     <li>成功后文件与 DD 均处于 DISCARDED；失败保留 DDL log 供恢复续作。</li>
     * </ol>
     *
     * @param owner 独立 DDL statement owner；不得复用 Session transaction owner
     * @param name 已由 Binder 限定的逻辑表名
     * @param timeout 本条 statement 剩余正有界时间
     */
    public void discardTablespace(MdlOwnerId owner, QualifiedTableName name, Duration timeout) {
        // 1、预读只用于路径命名；权威状态仍由下游 table X 内重验。
        TransferTarget target = transferTarget(name);
        // 2、固定目录与文件名不接受 SQL 输入。
        Path quarantine = transferPath("discarded", target.tableId(), target.binding());
        ensureTransferDirectory(quarantine.getParent());
        // 3、原有状态机在锁内复核同一个 tableId/spaceId。
        if (target.state() == TableState.RECOVERY_UNAVAILABLE) {
            discardRecoveryUnavailable(owner, name, quarantine, timeout);
        } else {
            discardTablespace(owner, name, quarantine, timeout);
        }
        // 4、终态与恢复证据由被调用状态机完整发布。
    }

    /**
     * 将 FORCE 隔离的对象从 {@code RECOVERY_UNAVAILABLE} 收敛为 {@code RECOVERY_DISCARDED}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得 schema IX/table X，重读隔离状态、binding 与固定 quarantine identity；不读取损坏 page0。</li>
     *     <li>等待 committed history 与旧 metadata pin 清零，写 PREPARED 并建立不可回退 cache 屏障。</li>
     *     <li>调用 raw/offline storage 在独占 SpaceId lease 内幂等移动文件，成功后写 ENGINE_DONE。</li>
     *     <li>以 marker 预留的唯一版本提交 RECOVERY_DISCARDED，再依次写 DICTIONARY_COMMITTED/COMMITTED。</li>
     * </ol>
     *
     * @param owner 独立 DDL owner；不得是 session 保留 owner
     * @param name 恢复可见目录中的隔离表名
     * @param quarantine 由实例 transfer 根与 table/space identity 派生的固定目标
     * @param timeout MDL、history、pin 与物理 lease 共用的正上界
     * @throws DictionaryDdlException 状态、binding、持久阶段或物理移动无法安全收敛时抛出
     */
    public void discardRecoveryUnavailable(
            MdlOwnerId owner, QualifiedTableName name, Path quarantine, Duration timeout) {
        // 1. 任何文件副作用前先固定逻辑名称与路径所有权。
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (name == null || quarantine == null) {
            throw new DatabaseValidationException(
                    "recovery discard table/quarantine must not be null");
        }
        Path target = checkedTransferPath(quarantine);
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner,
                     MdlKey.table(name.canonicalKey()), MdlMode.EXCLUSIVE,
                     MdlDuration.TRANSACTION), timeout)) {
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "schema does not exist: " + name.schema().displayName()));
            TableDefinition unavailable = repository.findTableForRecovery(
                    schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "table does not exist: " + name.canonicalKey()));
            if (unavailable.state() != TableState.RECOVERY_UNAVAILABLE) {
                throw new DictionaryDdlException(
                        "table is not RECOVERY_UNAVAILABLE: " + unavailable.id().value());
            }
            TableStorageBinding binding = unavailable.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException(
                            "RECOVERY_UNAVAILABLE table has no binding: " + unavailable.id().value()));
            Path expected = transferPath("discarded", unavailable.id(), binding);
            if (!target.equals(expected)) {
                throw new DatabaseValidationException(
                        "recovery DISCARD target must match table/space identity: " + expected);
            }

            // 2. 隔离对象仍可能被恢复事务 history 引用；先清零再发布 durable 物理意图。
            purgeBarrier.awaitUnreferenced(unavailable.id().value(), timeout);
            DictionaryIdAllocation ids = control.reserve(
                    new DictionaryIdRequest(0, 0, 0, 0, 1, 1));
            DictionaryVersion finalVersion = DictionaryVersion.of(ids.dictionaryVersion());
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            TableDefinition discarded = lifecycle(
                    unavailable, finalVersion, TableState.RECOVERY_DISCARDED);
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), finalVersion.value(), unavailable.id().value()),
                    0L, DdlLogOperation.DISCARD_RECOVERY_UNAVAILABLE, DdlLogPhase.PREPARED,
                    binding.spaceId(), binding.path(), Optional.of(target), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(schemaDigests.digest(
                            schema, unavailable, binding.rowFormatVersion())),
                    Optional.empty(), Optional.of(schemaDigests.digest(
                            schema, discarded, binding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            cache.invalidateTable(unavailable.id(), finalVersion);
            if (!cache.awaitUnpinned(unavailable.id(), timeout)) {
                throw new DictionaryDdlException(
                        "timed out waiting pins before recovery DISCARD: " + unavailable.id().value());
            }

            // 3. raw 路径不打开文件；幂等完成才允许 durable phase 前进。
            physical.discardRecoveryUnavailable(binding, target, timeout);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);

            // 4. DD 终态晚于物理终点，重启可依据 marker 确定重试方向。
            commitUpdate(finalVersion, discarded);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
            cleanSnapshotPublisher.publish();
        }
    }

    /**
     * table X 已持有时删除 recovery-isolated canonical 文件并发布 DROPPED；可信备份不在删除所有权内。
     *
     * @param schema isolated所属且由外层schema/table MDL共同冻结的exact schema
     * @param isolated 锁内重读的 RECOVERY_UNAVAILABLE/RECOVERY_DISCARDED aggregate
     * @param binding 与 aggregate 同版本的稳定物理绑定
     * @param timeout history、pin 与物理 lease 共用的正上界
     */
    private void dropRecoveryIsolatedLocked(
            SchemaDefinition schema, TableDefinition isolated,
            TableStorageBinding binding, Duration timeout) {
        // 1. 先清空所有可能仍携带 table identity 的事务历史与 metadata pin。
        purgeBarrier.awaitUnreferenced(isolated.id().value(), timeout);
        DictionaryIdAllocation ids = control.reserve(
                new DictionaryIdRequest(0, 0, 0, 0, 1, 1));
        DictionaryVersion finalVersion = DictionaryVersion.of(ids.dictionaryVersion());
        DdlId ddlId = DdlId.of(ids.firstDdlId());
        Path deletionPath = isolated.state() == TableState.RECOVERY_DISCARDED
                ? transferPath("discarded", isolated.id(), binding)
                : binding.path();
        TableDefinition dropped = lifecycle(isolated, finalVersion, TableState.DROPPED);
        DdlLogRecord prepared = new DdlLogRecord(
                new DdlUndoMarker(ddlId.value(), finalVersion.value(), isolated.id().value()),
                0L, DdlLogOperation.DROP_RECOVERY_UNAVAILABLE, DdlLogPhase.PREPARED,
                binding.spaceId(), binding.path(),
                isolated.state() == TableState.RECOVERY_DISCARDED
                        ? Optional.of(deletionPath) : Optional.empty(), Optional.empty(),
                DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                Optional.of(schemaDigests.digest(schema, isolated, binding.rowFormatVersion())),
                Optional.empty(), Optional.of(schemaDigests.digest(
                        schema, dropped, binding.rowFormatVersion())),
                DdlControlState.OPEN, Optional.empty(), Optional.empty());
        repository.ddlLog().prepare(prepared);
        cache.invalidateTable(isolated.id(), finalVersion);
        if (!cache.awaitUnpinned(isolated.id(), timeout)) {
            throw new DictionaryDdlException(
                    "timed out waiting pins before recovery DROP: " + isolated.id().value());
        }

        // 2. RECOVERY_DISCARDED 的 canonical path 通常已缺失，raw 删除按 deleteIfExists 幂等收敛。
        physical.dropRecoveryUnavailable(binding, deletionPath, timeout);
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);

        // 3. 物理终点之后才发布 tombstone，任何中断均能从 PREPARED/ENGINE_DONE 继续。
        commitUpdate(finalVersion, dropped);
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        cleanSnapshotPublisher.publish();
    }

    /**
     * 从实例固定 incoming 路径执行 IMPORT；page0 identity 在任何物理复制前由 storage inspector 读取。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>解析 DISCARDED 表的稳定 table/space binding，构造不可由 SQL 改写的 incoming 路径。</li>
     *     <li>只读检查候选文件 page0，校验 checksum、page size、space id、类型和版本。</li>
     *     <li>进入既有 IMPORT 状态机，并在 table MDL X 下重验状态、binding 与固定路径。</li>
     *     <li>成功后 canonical 文件恢复 NORMAL 且 DD 发布 ACTIVE；失败由 DDL log 保留可恢复阶段。</li>
     * </ol>
     *
     * @param owner 独立 DDL statement owner；不得复用 Session transaction owner
     * @param name 已由 Binder 限定的逻辑表名
     * @param timeout 本条 statement 剩余正有界时间
     */
    public void importTablespace(MdlOwnerId owner, QualifiedTableName name, Duration timeout) {
        // 1、路径只由 committed binding 派生，最终 identity 在 table X 下再次核对。
        TransferTarget target = transferTarget(name);
        if (target.state() == TableState.RECOVERY_DISCARDED) {
            importRecoveryReplacement(owner, name, timeout);
            return;
        }
        Path source = transferPath("incoming", target.tableId(), target.binding());
        ensureTransferDirectory(source.getParent());
        // 2、检查阶段不复制、不打开为在线 tablespace，也不产生 redo。
        TablespaceFileIdentity identity =
                physical.inspectTablespaceFile(source, target.binding().spaceId());
        // 3、原状态机负责 MDL、DDL log、物理挂载与字典发布。
        importTablespace(owner, name, source, identity, timeout);
        // 4、所有 durable 终态均由原状态机在 clean snapshot 发布后对外可见。
    }

    /**
     * 为 ACTIVE 表创建本实例 HMAC 签名的 clean backup archive pair；该 Java facade 不新增 SQL 语法。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得 schema IX/table X 并重读 ACTIVE aggregate，阻止并发 DDL 改变定义或 binding。</li>
     *     <li>等待 persistent history 引用与 metadata pin 清零；等待期间不持 page latch/MTR/file lease。</li>
     *     <li>由 backup service 懒加载实例 identity，并调用 storage X lease 执行 drain/force/stable copy。</li>
     *     <li>返回数据+manifest archive pair；本操作不修改 DD state/version，也不清理历史备份。</li>
     * </ol>
     *
     * @param owner 独立 DDL/管理操作 MDL owner
     * @param name 必须解析为 committed ACTIVE 表
     * @param timeout MDL、history、pin、drain 与 file lease 共用的正上界
     * @return 已完成 HMAC manifest 原子发布的 archive pair
     */
    public RecoveryBackupArtifact createRecoveryBackup(
            MdlOwnerId owner, QualifiedTableName name, Duration timeout) {
        // 1. recovery backup 是 Java 管理 facade，仍使用与 DDL 相同的 schema→table 锁序。
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (name == null) {
            throw new DatabaseValidationException(
                    "recovery backup table name must not be null");
        }
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner,
                     MdlKey.table(name.canonicalKey()), MdlMode.EXCLUSIVE,
                     MdlDuration.TRANSACTION), timeout)) {
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "schema does not exist: " + name.schema().displayName()));
            TableDefinition active = repository.findTable(
                    schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "table does not exist: " + name.canonicalKey()));
            if (active.state() != TableState.ACTIVE) {
                throw new DictionaryDdlException(
                        "recovery backup source is not ACTIVE: " + active.id().value());
            }

            // 2. clean backup 不能携带尚待 purge 的 committed row history，也不能越过旧 metadata pin。
            purgeBarrier.awaitUnreferenced(active.id().value(), timeout);
            if (!cache.awaitUnpinned(active.id(), timeout)) {
                throw new DictionaryDdlException(
                        "timed out waiting pins before recovery backup: " + active.id().value());
            }

            // 3. identity 仅在此处懒创建；普通启动完全不依赖该文件。
            RecoveryBackupArtifact artifact = recoveryBackups.createBackup(active, timeout);

            // 4. 备份不推进 DD；archive 生命周期独立于后续 DROP，供管理员显式 staging。
            log.info("created trusted recovery backup: table={} space={} backup={} data={}",
                    active.id().value(), active.storageBinding().orElseThrow().spaceId().value(),
                    artifact.manifest().backupId(), artifact.dataPath());
            return artifact;
        }
    }

    /**
     * 使用固定 recovery-incoming pair 把 RECOVERY_DISCARDED 对象替换为可信 clean backup 并重新发布 ACTIVE。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得 schema IX/table X，重读 RECOVERY_DISCARDED aggregate，并在 marker 前完成 HMAC/hash/page0 验证。</li>
     *     <li>预留单一版本、写 op=10 PREPARED、建立 cache 屏障并等待旧 pin 清零。</li>
     *     <li>storage 原子复制固定 incoming 数据，挂载后把 page0 恢复 NORMAL并 force，再写 ENGINE_DONE。</li>
     *     <li>提交 ACTIVE aggregate，写 DICTIONARY_COMMITTED/COMMITTED 并发布 cache；incoming/archive 均保留。</li>
     * </ol>
     *
     * @param owner 独立管理操作 MDL owner
     * @param name 必须解析为 RECOVERY_DISCARDED 对象
     * @param timeout MDL、pin、WAL/flush 与 file lease 共用的正上界
     */
    public void importRecoveryReplacement(
            MdlOwnerId owner, QualifiedTableName name, Duration timeout) {
        // 1. 验证发生在 table X 内，避免定义摘要校验后被并发 ALTER/rename 换绑。
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (name == null) {
            throw new DatabaseValidationException(
                    "recovery replacement table name must not be null");
        }
        try (MdlTicket schemaTicket = locks.acquire(new MdlRequest(owner,
                MdlKey.schema(name.schema().canonicalName()), MdlMode.INTENTION_EXCLUSIVE,
                MdlDuration.TRANSACTION), timeout);
             MdlTicket tableTicket = locks.acquire(new MdlRequest(owner,
                     MdlKey.table(name.canonicalKey()), MdlMode.EXCLUSIVE,
                     MdlDuration.TRANSACTION), timeout)) {
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "schema does not exist: " + name.schema().displayName()));
            TableDefinition discarded = repository.findTableForRecovery(
                    schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException(
                            "table does not exist: " + name.canonicalKey()));
            if (discarded.state() != TableState.RECOVERY_DISCARDED) {
                throw new DictionaryDdlException(
                        "trusted recovery import requires RECOVERY_DISCARDED table: "
                                + discarded.id().value());
            }
            TableStorageBinding binding = discarded.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException(
                            "RECOVERY_DISCARDED table has no binding: " + discarded.id().value()));
            ValidatedRecoveryBackup backup = recoveryBackups.validateIncoming(discarded);

            // 2. 只有完整验证成功才能留下 durable op=10 意图；之后任何失败均由启动恢复前滚。
            DictionaryIdAllocation ids = control.reserve(
                    new DictionaryIdRequest(0, 0, 0, 0, 1, 1));
            DictionaryVersion activeVersion = DictionaryVersion.of(ids.dictionaryVersion());
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            TableDefinition active = lifecycle(
                    discarded, activeVersion, TableState.ACTIVE);
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), activeVersion.value(), discarded.id().value()),
                    0L, DdlLogOperation.IMPORT_RECOVERY_REPLACEMENT, DdlLogPhase.PREPARED,
                    binding.spaceId(), binding.path(), Optional.of(backup.dataPath()),
                    Optional.of(backup.fileIdentity()), DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(schemaDigests.digest(
                            schema, discarded, binding.rowFormatVersion())),
                    Optional.empty(), Optional.of(schemaDigests.digest(
                            schema, active, binding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            cache.invalidateTable(discarded.id(), activeVersion);
            if (!cache.awaitUnpinned(discarded.id(), timeout)) {
                throw new DictionaryDdlException(
                        "timed out waiting pins before trusted recovery import: "
                                + discarded.id().value());
            }

            // 3. source 保留；canonical replacement 在 NORMAL page0 force 后才形成 ENGINE_DONE。
            physical.importTablespace(
                    binding, backup.dataPath(), backup.fileIdentity(), timeout);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);

            // 4. catalog ACTIVE 是逻辑可见性的唯一发布点，晚于全部物理持久化。
            commitUpdate(activeVersion, active);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            cache.publishTable(active);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
            cleanSnapshotPublisher.publish();
        }
    }

    /**
     * 按 SQL 声明顺序执行一次通用 ALTER。selector先冻结instant/inplace/blocking方向；单index action复用
     * 已接线的Online ADD/DROP，metadata-only原地保留row format，其余能力缺口显式进入既有单shadow阻塞实现。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在副作用前冻结策略；生产单index action直接委托已实现的online协议，其余路径再取得完整MDL集合。</li>
     *     <li>依次作用 action 到 staged columns/indexes/options/name，验证主键、相对位置、名称和目标 schema。</li>
     *     <li>metadata-only 先 durable SDI 再一次提交 DD；结构变化则写 PREPARED、构建/force shadow 与 SDI，
     *     写 ENGINE_DONE 后一次提交新 binding。</li>
     *     <li>发布 cache 并回收旧空间；后置失败保留新 DD/marker，启动恢复按 committed binding 收敛。</li>
     * </ol>
     *
     * @param owner 独立 DDL statement owner；不得复用 Session transaction owner
     * @param command 已完成纯 SQL 类型/default 校验的保序 command
     * @param timeout MDL、purge/pin、rebuild、WAL 与 drop 共用的正有界时限
     * @return 唯一一次提交后可见的 ACTIVE table aggregate
     * @throws DictionaryDdlException staged action、shadow 构建或持久化失败时抛出；若 DD 已提交不得回滚
     */
    public TableDefinition alterTable(
            MdlOwnerId owner, AlterTableCommand command, Duration timeout) {
        // 1、纯策略决策早于锁、identity与IO；online分支将资源生命周期完整委托给单index协调器。
        validateOwnerTimeout(owner, timeout);
        cleanSnapshotPublisher.assertAvailable();
        if (command == null) {
            throw new DatabaseValidationException("ALTER TABLE command must not be null");
        }
        OnlineAlterDecision decision = alterStrategySelector.select(command);
        if (decision.strategy() == OnlineAlterStrategy.INPLACE_INDEX
                && command.actions().size() == 1
                && command.actions().getFirst() instanceof AlterTableAction.AddIndex add
                && onlineIndexRuntime != null) {
            // 1、单ADD不拆分aggregate；委托方法拥有完整marker/gate/row-log/取消/恢复生命周期。
            return createSecondaryIndex(owner,
                    new CreateSecondaryIndexCommand(command.table(), add.index()), timeout);
        }
        if (decision.strategy() == OnlineAlterStrategy.INPLACE_INDEX
                && command.actions().size() == 1
                && command.actions().getFirst() instanceof AlterTableAction.DropIndex drop
                && onlineIndexRuntime != null && indexRetirementBarrier != null) {
            // 1、单DROP复用retirement fence；target DD发布与segment回收仍是一个online operation。
            return dropSecondaryIndex(owner,
                    new DropSecondaryIndexCommand(command.table(), drop.name()), timeout);
        }
        if (decision.strategy() == OnlineAlterStrategy.INPLACE_INDEX
                && decision.reason() == OnlineAlterReason.GENERAL_INPLACE_MANIFEST
                && onlineAlterRuntime != null && onlineAlterRetirementBarrier != null) {
            return alterTableOnline(owner, command, decision, timeout);
        }
        if (decision.strategy() == OnlineAlterStrategy.SHADOW_REBUILD_V1
                && onlineAlterRuntime != null && onlineAlterRetirementBarrier != null) {
            return alterTableOnline(owner, command, decision, timeout);
        }
        if (decision.strategy() == OnlineAlterStrategy.BLOCKING) {
            log.info("ALTER TABLE selected explicit blocking fallback: table={} reason={} rejected={}",
                    command.table().canonicalKey(), decision.reason(),
                    decision.rejectedCapabilities());
        }
        // 1、非单index路径的锁集合完全来自逻辑名称，等待期间不持page/file/MTR资源。
        try (AlterMdlTickets ignored = acquireAlterTickets(owner, command, timeout)) {
            SchemaDefinition sourceSchema = repository.findSchema(command.table().schema())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "schema does not exist: "
                                    + command.table().schema().displayName()));
            TableDefinition active = repository.findTable(
                            sourceSchema.id(), command.table().table())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "table does not exist: " + command.table().canonicalKey()));
            if (active.state() != TableState.ACTIVE) {
                throw new DictionaryDdlException(
                        "ALTER TABLE target is not ACTIVE: " + active.id().value());
            }

            // 2、draft 中的 sourceOrdinal/default 同步随列移动，最终直接形成 storage row projection。
            AlterDraft draft = stageAlter(active, command.actions());
            ensureAlterTargetNameAvailable(active, draft.schemaId(), draft.name());
            long finalAddedIndexes = draft.indexes().stream()
                    .filter(StagedIndex::requiresIdentity).count();
            boolean structural = draft.structural();
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(
                    0, 0, Math.toIntExact(finalAddedIndexes),
                    structural ? 1 : 0,
                    structural || decision.strategy() == OnlineAlterStrategy.INSTANT_METADATA
                            ? 1 : 0,
                    1));
            DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
            List<IndexDefinition> indexes = assignAlterIndexIds(
                    draft.indexes(), ids.firstIndexId());
            List<ColumnDefinition> columns = draft.columns().stream()
                    .map(StagedColumn::definition).toList();

            if (!structural) {
                TableDefinition published = new TableDefinition(
                        active.id(), draft.schemaId(), draft.name(), version, TableState.ACTIVE,
                        columns, indexes, active.storageBinding(), draft.options());
                ensureAlterTargetNameAvailable(active, published);
                if (decision.strategy() == OnlineAlterStrategy.INSTANT_METADATA) {
                    SchemaDefinition targetSchema = repository.findSchema(draft.schemaId())
                            .orElseThrow(() -> new DictionaryObjectNotFoundException(
                                    "ALTER metadata target schema disappeared: "
                                            + draft.schemaId().value()));
                    return alterTableInstantMetadata(owner, command, sourceSchema, targetSchema,
                            active, published, DdlId.of(ids.firstDdlId()), timeout);
                }
                sdi.write(published, timeout);
                // DD append 的成功响应可能丢失；先阻断旧 cache，避免 durable 新名称/选项与旧内存版本并行服务。
                cache.invalidateTable(active.id(), version);
                commitUpdate(version, published);
                cache.publishTable(published);
                cleanSnapshotPublisher.publish();
                return published;
            }

            purgeBarrier.awaitUnreferenced(active.id().value(), timeout);
            if (!cache.awaitUnpinned(active.id(), timeout)) {
                throw new DictionaryDdlException(
                        "timed out waiting old metadata pins before ALTER rebuild: "
                                + active.id().value());
            }
            TableStorageBinding oldBinding = active.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException(
                            "ALTER source has no physical binding: " + active.id().value()));
            int targetSpaceId = ids.firstSpaceId();
            Path targetPath = tablePath(active.id(), targetSpaceId);
            ensureTablesDirectory();
            // 任何 schema/key/charset 不可实现性必须在 durable marker 与 shadow 文件之前拒绝。
            StorageTableDefinition sourceDefinition = storageDefinition(active);
            StorageTableDefinition targetDefinition = storageDefinition(
                    active.id(), targetSpaceId, targetPath, version,
                    columns, indexes);
            List<StorageColumnRewrite> rewrites = draft.columns().stream()
                    .map(column -> column.sourceOrdinal() >= 0
                            ? StorageColumnRewrite.source(column.sourceOrdinal())
                            : StorageColumnRewrite.added(column.storageDefault()))
                    .toList();
            physical.validateTableDefinition(sourceDefinition);
            physical.validateTableDefinition(targetDefinition);

            DdlId ddlId = DdlId.of(ids.firstDdlId());
            SchemaDefinition targetSchema = repository.findSchema(draft.schemaId())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "ALTER target schema disappeared before marker: "
                                    + draft.schemaId().value()));
            TableDefinition plannedTarget = new TableDefinition(
                    active.id(), draft.schemaId(), draft.name(), version, TableState.ACTIVE,
                    columns, indexes, Optional.empty(), draft.options());
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(
                            ddlId.value(), version.value(), active.id().value()),
                    targetSpaceId, DdlLogOperation.REBUILD_TABLE, DdlLogPhase.PREPARED,
                    oldBinding.spaceId(), oldBinding.path(),
                    Optional.of(targetPath), Optional.empty(),
                    DdlExecutionProtocol.ATOMIC_BLOCKING_V1,
                    Optional.of(schemaDigests.digest(
                            sourceSchema, active, oldBinding.rowFormatVersion())),
                    Optional.empty(), Optional.of(schemaDigests.digest(
                            targetSchema, plannedTarget, version.value())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            repository.ddlLog().prepare(prepared);
            faultInjector.afterAlterPrepared(prepared);

            // 3、shadow 未被 DD 引用；全部行、索引、SDI 和 force 成功后才发布 ENGINE_DONE。
            TableStorageBinding targetBinding = null;
            TableDefinition published;
            DdlLogRecord engineDone;
            try {
                targetBinding = physical.rebuildTable(
                        new StorageTableRebuildRequest(
                                sourceDefinition, oldBinding, targetDefinition, rewrites),
                        timeout);
                published = new TableDefinition(
                        active.id(), draft.schemaId(), draft.name(), version, TableState.ACTIVE,
                        columns, indexes, Optional.of(targetBinding), draft.options());
                ensureAlterTargetNameAvailable(active, published);
                sdi.write(published, timeout);
                engineDone = repository.ddlLog().transition(
                        ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            } catch (RuntimeException failure) {
                TableStorageBinding cleanupBinding = targetBinding;
                if (cleanupBinding == null
                        && failure instanceof TableRebuildException rebuildFailure) {
                    cleanupBinding = rebuildFailure.shadowBinding();
                }
                rollbackAlterShadow(ddlId, cleanupBinding, timeout, failure);
                throw failure;
            }
            // fault hook 位于补偿作用域之外，用异常模拟进程立即退出；真实普通失败已在上方完成精确 cleanup。
            // ENGINE_DONE 后 shadow 已完整可恢复；DD 提交前先阻断旧 cache，未知提交结果只能由恢复裁决。
            cache.invalidateTable(active.id(), version);
            faultInjector.afterAlterEngineDone(engineDone);

            try {
                commitUpdate(version, published);
            } catch (RuntimeException uncertain) {
                // 一旦开始 catalog append，就不能用“方法抛错”推断 DD 未提交；删除 shadow 会让 durable
                // 新 aggregate 指向缺失文件。保留 ENGINE_DONE，由启动恢复重读 committed DD 决定前滚/回滚。
                log.warn("ALTER TABLE DD publish outcome is uncertain; retaining shadow marker: "
                                + "tableId={} ddlId={} oldSpace={} newSpace={}",
                        active.id().value(), ddlId.value(), oldBinding.spaceId().value(),
                        targetBinding.spaceId().value(), uncertain);
                throw uncertain;
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            faultInjector.afterAlterDictionaryCommitted(published);
            cache.publishTable(published);

            // 4、新 DD 已是唯一真相；旧空间回收失败只能前滚恢复，绝不能把 shadow 当临时文件删除。
            physical.dropTable(oldBinding, timeout);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
            cleanSnapshotPublisher.publish();
            return published;
        }
    }

    /**
     * 通用INPLACE_INDEX与SHADOW_REBUILD_V1共享的生产协调器。两种策略使用同一manifest、journal、gate、
     * cancel/forward竞争与registry；差异只在物理prepare、candidate codec、final barrier和retired resource。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>table X下顺序stage全部action、一次预留identity/version并force immutable manifest+journal与marker。</li>
     *     <li>建立descriptor chain或shadow capture，force CAPTURING后把source X降为SU；base work按有界批次执行。</li>
     *     <li>SU升回X并seal gate；shadow额外用同一deadline等待final ReadView generation与table history。</li>
     *     <li>两遍流式reconciliation与双向验证后写target SDI、force物理结果并持久安装retirement fence。</li>
     *     <li>OPEN→FORWARD_ONLY胜出后写RECONCILED/ENGINE_DONE，一次提交target aggregate并发布cache。</li>
     *     <li>清gate并降回SU，等待旧INDEX/SPACE安全退休，物理清理后写COMMITTED并删除journal。</li>
     * </ol>
     *
     * @param owner 独立DDL statement owner
     * @param command binder产生的有序完整ALTER命令
     * @param decision 已在任何副作用前冻结的通用online策略
     * @param timeout MDL、gate、barrier、WAL与文件操作的正上界
     * @return 单次DD提交后可见的target ACTIVE aggregate
     * @throws DictionaryDdlException pre-forward失败完成精确回滚，或post-forward失败需启动恢复前滚时抛出
     */
    private TableDefinition alterTableOnline(
            MdlOwnerId owner, AlterTableCommand command,
            OnlineAlterDecision decision, Duration timeout) {
        OnlineDdlCaptureId captureId = null;
        FileOnlineAlterChangeLog changeLog = null;
        OnlineAlterDescriptorSet descriptors = null;
        TableStorageBinding shadowBinding = null;
        TableDefinition source = null;
        TableDefinition targetWithBinding = null;
        DdlId ddlId = null;
        OnlineDdlOperationTracker tracker = null;
        boolean markerPrepared = false;
        boolean targetSdiWritten = false;
        boolean forwardOnly = false;
        boolean gateCleared = false;
        boolean journalClosed = false;

        try (AlterMdlTickets tickets = acquireAlterTickets(owner, command, timeout)) {
            // 1、initial X内重读source并一次性冻结全部逻辑identity；任何格式/能力错误早于journal、marker和segment。
            MdlTicket sourceTableTicket = tickets.require(
                    MdlKey.table(command.table().canonicalKey()));
            SchemaDefinition sourceSchema = repository.findSchema(command.table().schema())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "schema does not exist: "
                                    + command.table().schema().displayName()));
            source = repository.findTable(sourceSchema.id(), command.table().table())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "table does not exist: " + command.table().canonicalKey()));
            if (source.state() != TableState.ACTIVE) {
                throw new DictionaryDdlException(
                        "online ALTER target is not ACTIVE: " + source.id().value());
            }
            AlterDraft draft = stageAlter(source, command.actions());
            ensureAlterTargetNameAvailable(source, draft.schemaId(), draft.name());
            long addedCount = draft.indexes().stream()
                    .filter(StagedIndex::requiresIdentity).count();
            boolean shadow = decision.strategy() == OnlineAlterStrategy.SHADOW_REBUILD_V1;
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(
                    0, 0, Math.toIntExact(addedCount), shadow ? 1 : 0, 1, 1));
            DictionaryVersion targetVersion = DictionaryVersion.of(ids.dictionaryVersion());
            List<IndexDefinition> targetIndexes = assignAlterIndexIds(
                    draft.indexes(), ids.firstIndexId());
            List<ColumnDefinition> targetColumns = draft.columns().stream()
                    .map(StagedColumn::definition).toList();
            TableStorageBinding sourceBinding = source.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException(
                            "online ALTER source has no binding: "
                                    + command.table().canonicalKey()));
            SchemaDefinition targetSchema = repository.findSchema(draft.schemaId())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "online ALTER target schema disappeared: "
                                    + draft.schemaId().value()));
            TableDefinition plannedTarget = new TableDefinition(
                    source.id(), draft.schemaId(), draft.name(), targetVersion,
                    TableState.ACTIVE, targetColumns, targetIndexes,
                    Optional.empty(), draft.options());
            StorageTableDefinition sourceStorage = storageDefinition(source);
            int targetSpaceId = shadow ? ids.firstSpaceId() : 0;
            Path shadowPath = shadow ? tablePath(source.id(), targetSpaceId) : null;
            StorageTableDefinition targetStorage = shadow
                    ? storageDefinition(source.id(), targetSpaceId, shadowPath,
                    targetVersion, targetColumns, targetIndexes)
                    : onlineInplaceDefinition(sourceStorage, targetIndexes);
            List<StorageColumnRewrite> rewrites = draft.columns().stream()
                    .map(column -> column.sourceOrdinal() >= 0
                            ? StorageColumnRewrite.source(column.sourceOrdinal())
                            : StorageColumnRewrite.added(column.storageDefault()))
                    .toList();
            physical.validateTableDefinition(sourceStorage);
            physical.validateTableDefinition(targetStorage);
            List<OnlineAlterIndexAddRequest> additions = onlineAlterAdditions(
                    command.actions(), targetIndexes);
            List<OnlineAlterIndexDropRequest> drops = onlineAlterDrops(
                    command.actions(), source, sourceBinding);

            ddlId = DdlId.of(ids.firstDdlId());
            captureId = OnlineDdlCaptureId.of(ddlId.value());
            DdlExecutionProtocol protocol = shadow
                    ? DdlExecutionProtocol.ONLINE_ALTER_SHADOW_V1
                    : DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1;
            DdlLogOperation operation = shadow
                    ? DdlLogOperation.REBUILD_TABLE
                    : DdlLogOperation.ALTER_TABLE_INPLACE;
            DdlSchemaDigest sourceDigest = schemaDigests.digest(
                    sourceSchema, source, sourceBinding.rowFormatVersion());
            long targetRowFormat = shadow
                    ? targetVersion.value() : sourceBinding.rowFormatVersion();
            DdlSchemaDigest targetDigest = schemaDigests.digest(
                    targetSchema, plannedTarget, targetRowFormat);
            long captureReadViewBaseline = onlineAlterRuntime.readViewBarrier()
                    .captureGeneration();
            List<OnlineAlterActionDescriptor> actionDescriptors =
                    new OnlineAlterActionPayloadCodec().encode(
                            command.actions(), source, plannedTarget, targetSchema);
            OnlineAlterManifest manifest = new OnlineAlterManifest(
                    ddlId.value(), source.id(), source.version(), targetVersion,
                    protocol, sourceDigest, targetDigest,
                    sourceBinding.rowFormatVersion(), targetRowFormat,
                    captureReadViewBaseline, actionDescriptors,
                    shadow ? Optional.of(new OnlineAlterShadowTarget(
                            cn.zhangyis.db.domain.SpaceId.of(targetSpaceId), shadowPath))
                            : Optional.empty());
            byte[] manifestBytes = new OnlineAlterManifestCodec().encode(manifest);
            byte[] manifestDigest = sha256(manifestBytes);

            tracker = onlineDdlRegistry.register(new OnlineDdlOperationIdentity(
                    ddlId, operation, source.id().value(), 0L,
                    command.table().canonicalKey(), "", source.version().value(),
                    targetVersion.value(), owner.value(), false,
                    java.util.OptionalLong.empty()), protocol);
            tracker.advanceRuntime(OnlineDdlRuntimePhase.ACTIVATING,
                    OnlineDdlWaitReason.NONE);
            if (!tracker.beginDurablePrepare()) {
                throw new OnlineDdlCancellationException(
                        "online ALTER cancelled before durable prepare: ddl=" + ddlId.value());
            }
            changeLog = onlineAlterRuntime.logFiles().create(
                    new OnlineAlterLogHeader(captureId, source.id().value(),
                            source.version().value(), targetVersion.value(),
                            sourceBinding.rowFormatVersion(), targetRowFormat,
                            protocol.stableCode(), shadow ? targetSpaceId : 0,
                            captureReadViewBaseline, manifestBytes));
            Path journalPath = changeLog.path();
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), targetVersion.value(),
                            source.id().value()),
                    shadow ? targetSpaceId : 0L, operation, DdlLogPhase.PREPARED,
                    sourceBinding.spaceId(), sourceBinding.path(),
                    Optional.of(journalPath), Optional.empty(), protocol,
                    Optional.of(sourceDigest), Optional.empty(),
                    Optional.of(targetDigest), DdlControlState.OPEN,
                    Optional.empty(), Optional.empty());
            try {
                repository.ddlLog().prepare(prepared);
            } catch (RuntimeException prepareFailure) {
                tracker.failDurablePrepare("MARKER_PREPARE_FAILED");
                throw prepareFailure;
            }
            markerPrepared = true;
            tracker.markDurablePrepared(prepared);
            faultInjector.afterGeneralAlterPrepared(prepared);
            requireOnlineDdlNotCancelled(ddlId, "after general ALTER prepare");
            onlineAlterRuntime.gate().beginActivation(
                    source.id().value(), captureId, timeout);

            cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCandidateCodec candidateCodec;
            Map<Integer, SecondaryIndexCandidateCodec> nestedCodecs = new LinkedHashMap<>();
            if (shadow) {
                var sourceClustered = new BTreeIndexMetadataFactory()
                        .createTable(sourceStorage, sourceBinding).clusteredIndex();
                candidateCodec = new ClusteredIdentityCandidateCodec(
                        sourceClustered, onlineAlterRuntime.typeRegistry());
            } else {
                descriptors = physical.beginOnlineAlterIndexDescriptors(
                        sourceBinding, ddlId.value(), targetVersion.value(), 1L,
                        additions, drops, manifestDigest, timeout);
                if (additions.isEmpty()) {
                    candidateCodec = new NoOpOnlineDdlCandidateCodec();
                } else {
                    TableStorageBinding buildingBinding = onlineInplaceBinding(
                            sourceBinding, descriptors);
                    StorageTableDefinition buildingDefinition = onlineInplaceStorageDefinition(
                            sourceStorage, appendAddedStorageIndexes(
                                    sourceStorage.indexes(), additions));
                    var buildingMetadata = new BTreeIndexMetadataFactory()
                            .createTable(buildingDefinition, buildingBinding);
                    List<OnlineAlterIndexTarget> targets = new ArrayList<>();
                    for (OnlineAlterIndexAddRequest addition : additions) {
                        var secondary = buildingMetadata.requireSecondary(
                                addition.definition().indexId());
                        SecondaryIndexCandidateCodec nested =
                                new SecondaryIndexCandidateCodec(
                                        secondary.layout(), onlineAlterRuntime.typeRegistry());
                        nestedCodecs.put(addition.actionOrdinal(), nested);
                        targets.add(new OnlineAlterIndexTarget(
                                addition.actionOrdinal(), addition.definition().indexId(), nested));
                    }
                    candidateCodec = new MultiIndexAlterCandidateCodec(targets);
                }
            }

            // 2、capture frame、descriptor/shadow owner全部durable后才向DML发布target并降级X→SU。
            changeLog.appendState(OnlineAlterLogRecordType.GENERATION_STARTED, new byte[0]);
            long capturing = changeLog.appendState(
                    OnlineAlterLogRecordType.CAPTURING, new byte[0]);
            changeLog.forceThrough(capturing, timeout);
            onlineAlterRuntime.gate().publishCapture(new OnlineAlterCaptureTarget(
                    captureId, source.id().value(), changeLog, candidateCodec));
            locks.downgrade(sourceTableTicket, MdlMode.SHARED_UPGRADABLE);
            tracker.advanceRuntime(OnlineDdlRuntimePhase.BASE_SCAN,
                    OnlineDdlWaitReason.NONE);
            if (shadow) {
                ensureTablesDirectory();
                StorageTableRebuildRequest rebuildRequest =
                        new StorageTableRebuildRequest(sourceStorage, sourceBinding,
                                targetStorage, rewrites);
                OnlineDdlOperationTracker activeTracker = tracker;
                DdlId activeDdlId = ddlId;
                FileOnlineAlterChangeLog activeLog = changeLog;
                long activeTableId = source.id().value();
                try {
                    shadowBinding = physical.rebuildTableOnline(
                            rebuildRequest, timeout, (rowsInBatch, continuation) -> {
                                requireOnlineDdlNotCancelled(
                                        activeDdlId, "after shadow base-copy batch");
                                if (activeLog.abortRequired()
                                        || onlineAlterRuntime.gate().phase(activeTableId)
                                        == OnlineDdlTablePhase.ABORTING) {
                                    throw new DictionaryDdlException(
                                            "online shadow base copy observed durable abort");
                                }
                                activeTracker.addScanBatch(
                                        rowsInBatch, Optional.empty());
                                sampleOnlineAlterDdl(
                                        activeTracker, activeTableId, activeLog);
                            });
                } catch (TableRebuildException rebuildFailure) {
                    shadowBinding = rebuildFailure.shadowBinding();
                    throw rebuildFailure;
                }
            } else {
                descriptors = physical.backfillOnlineAlterIndexes(
                        sourceStorage, sourceBinding, descriptors, additions, timeout);
            }

            // 3、final X排空source writer；shadow barrier失败时先恢复CAPTURING再交统一pre-forward abort清理。
            tracker.advanceRuntime(OnlineDdlRuntimePhase.WAITING_FINAL_MDL,
                    OnlineDdlWaitReason.METADATA_LOCK);
            requireOnlineDdlNotCancelled(ddlId, "before general ALTER final MDL wait");
            try {
                locks.upgrade(sourceTableTicket, MdlMode.EXCLUSIVE, timeout);
            } catch (MetadataLockTimeoutException waitFailure) {
                if (onlineDdlCancelled(ddlId)) {
                    throw new OnlineDdlCancellationException(
                            "online ALTER cancelled during final MDL wait: ddl="
                                    + ddlId.value(), waitFailure);
                }
                throw waitFailure;
            }
            requireOnlineDdlNotCancelled(ddlId, "after general ALTER final MDL wait");
            TableDefinition finalSource = repository.findTableForRecovery(source.id())
                    .orElseThrow(() -> new DictionaryDdlException(
                            "online ALTER source disappeared before final X"));
            requireLiveDigest(sourceDigest, schemaDigests.digest(
                    sourceSchema, finalSource, sourceBinding.rowFormatVersion()),
                    ddlId, "general ALTER source at final X");
            tracker.advanceRuntime(OnlineDdlRuntimePhase.FINALIZING,
                    OnlineDdlWaitReason.GATE_QUIESCENCE);
            onlineAlterRuntime.gate().beginSeal(captureId, timeout);
            long finalReadViewFence = captureReadViewBaseline;
            if (shadow) {
                finalReadViewFence = onlineAlterRuntime.readViewBarrier().captureGeneration();
                long started = System.nanoTime();
                long budget = boundedTimeoutNanos(timeout);
                try {
                    onlineAlterRuntime.readViewBarrier().awaitClosedThrough(
                            finalReadViewFence, Duration.ofNanos(budget));
                    purgeBarrier.awaitUnreferenced(source.id().value(),
                            remainingDuration(started, budget,
                                    "shadow table history barrier"));
                } catch (RuntimeException barrierFailure) {
                    onlineAlterRuntime.gate().resumeCapture(captureId);
                    locks.downgrade(sourceTableTicket, MdlMode.SHARED_UPGRADABLE);
                    throw new OnlineAlterFinalizationTimeoutException(
                            "online shadow final ReadView/history barrier did not converge: ddl="
                                    + ddlId.value(), barrierFailure);
                }
            }
            long appended = changeLog.highestAppendedSequence();
            if (appended > changeLog.highestForcedSequence()) {
                changeLog.forceThrough(appended, timeout);
            }
            long sealed = changeLog.appendState(
                    OnlineAlterLogRecordType.SEALED, new byte[0]);
            changeLog.forceThrough(sealed, timeout);

            // 4、两遍都从journal按批次流式读取；每批结束前所有B+Tree/LOB MTR均已释放。
            tracker.advanceRuntime(OnlineDdlRuntimePhase.RECONCILING,
                    OnlineDdlWaitReason.NONE);
            if (shadow) {
                StorageTableRebuildRequest request = new StorageTableRebuildRequest(
                        sourceStorage, sourceBinding, targetStorage, rewrites);
                shadowBinding = reconcileOnlineShadow(
                        changeLog, (OnlineClusteredIdentityCodec) candidateCodec,
                        request, shadowBinding, tracker, ddlId);
                tracker.advanceRuntime(OnlineDdlRuntimePhase.VERIFYING,
                        OnlineDdlWaitReason.NONE);
                physical.verifyOnlineShadow(request, shadowBinding,
                        onlineAlterRuntime.config().scanBatchRows());
                targetWithBinding = new TableDefinition(
                        source.id(), draft.schemaId(), draft.name(), targetVersion,
                        TableState.ACTIVE, targetColumns, targetIndexes,
                        Optional.of(shadowBinding), draft.options());
            } else {
                if (!additions.isEmpty()) {
                    descriptors = reconcileOnlineAlterIndexes(
                            changeLog, (MultiIndexAlterCandidateCodec) candidateCodec,
                            nestedCodecs, sourceStorage, sourceBinding, descriptors,
                            additions, tracker, ddlId);
                }
                tracker.advanceRuntime(OnlineDdlRuntimePhase.VERIFYING,
                        OnlineDdlWaitReason.NONE);
                for (OnlineAlterIndexAddRequest addition : additions) {
                    physical.verifyOnlineAlterIndex(sourceStorage, sourceBinding,
                            descriptors, addition,
                            onlineAlterRuntime.config().scanBatchRows());
                }
                TableStorageBinding targetBinding = onlineInplaceTargetBinding(
                        sourceBinding, targetIndexes, descriptors);
                targetWithBinding = new TableDefinition(
                        source.id(), draft.schemaId(), draft.name(), targetVersion,
                        TableState.ACTIVE, targetColumns, targetIndexes,
                        Optional.of(targetBinding), draft.options());
            }
            requireOnlineDdlNotCancelled(ddlId, "after general ALTER verification");
            sdi.write(targetWithBinding, timeout);
            targetSdiWritten = true;
            physical.makeSecondaryIndexBuildDurable(
                    shadow ? shadowBinding : sourceBinding,
                    onlineAlterRuntime.gate().terminalRedoHighWater(source.id().value()), timeout);
            byte[] readyPayload = shadow
                    ? ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                    .putLong(finalReadViewFence).array()
                    : new byte[0];
            long ready = changeLog.appendState(
                    OnlineAlterLogRecordType.READY_TO_PUBLISH, readyPayload);
            changeLog.forceThrough(ready, timeout);
            faultInjector.afterGeneralAlterReady(
                    repository.ddlLog().find(ddlId).orElseThrow());

            List<DdlRetiredResource> resources = shadow
                    ? List.of(new DdlRetiredResource(
                    DdlRetiredResourceKind.TABLESPACE, sourceBinding.spaceId().value()))
                    : drops.stream().map(drop -> new DdlRetiredResource(
                                    DdlRetiredResourceKind.INDEX,
                                    drop.binding().indexId()))
                    .sorted().toList();
            if (!resources.isEmpty()) {
                long generation = shadow ? 1L : descriptors.generation();
                DdlRetirementFence fence = onlineAlterRetirementBarrier.captureFence(
                        source.id().value(), source.version().value(), generation,
                        ddlId.value(), resources);
                DdlLogRecord fenced = repository.ddlLog().installRetirementFence(
                        ddlId, DdlLogPhase.PREPARED, DdlControlState.OPEN, fence);
                tracker.observeDurable(fenced);
            }
            DdlControlCasResult direction = repository.ddlLog().compareAndSetControl(
                    ddlId, DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.FORWARD_ONLY, Optional.empty());
            if (!direction.changed()) {
                if (direction.observedRecord().controlState()
                        == DdlControlState.CANCEL_REQUESTED) {
                    throw new OnlineDdlCancellationException(
                            "online ALTER cancellation won before forward fence: ddl="
                                    + ddlId.value());
                }
                throw new DictionaryDdlLogStateException(
                        "online ALTER observed unexpected control state: "
                                + direction.observedRecord().controlState());
            }
            forwardOnly = true;
            tracker.observeDurable(direction.observedRecord());
            faultInjector.afterGeneralAlterForwardFenced(
                    direction.observedRecord());
            tracker.advanceRuntime(OnlineDdlRuntimePhase.FORWARD_FENCED,
                    OnlineDdlWaitReason.NONE);
            long reconciled = changeLog.appendState(
                    OnlineAlterLogRecordType.RECONCILED, new byte[0]);
            changeLog.forceThrough(reconciled, timeout);
            DdlLogRecord engineDone = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            tracker.observeDurable(engineDone);
            faultInjector.afterGeneralAlterEngineDone(engineDone);

            // 5、target SDI/physical truth均已durable，DD只提交一次完整aggregate，不暴露逐action中间版本。
            tracker.advanceRuntime(OnlineDdlRuntimePhase.PUBLISHING,
                    OnlineDdlWaitReason.NONE);
            cache.invalidateTable(source.id(), targetVersion);
            commitUpdate(targetVersion, targetWithBinding);
            DdlLogRecord dictionaryCommitted = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE,
                    DdlLogPhase.DICTIONARY_COMMITTED);
            tracker.observeDurable(dictionaryCommitted);
            faultInjector.afterGeneralAlterDictionaryCommitted(targetWithBinding);
            cache.publishTable(targetWithBinding);
            onlineAlterRuntime.gate().clearCapture(captureId);
            gateCleared = true;
            changeLog.close();
            journalClosed = true;
            locks.downgrade(sourceTableTicket, MdlMode.SHARED_UPGRADABLE);

            // 6、logical publish后DML使用target metadata继续运行；这里只等待并回收source物理资源。
            tracker.advanceRuntime(OnlineDdlRuntimePhase.RETIRING,
                    OnlineDdlWaitReason.RETIREMENT_FENCE);
            DdlRetirementFence fence = dictionaryCommitted.retirementFence().orElse(null);
            if (fence != null) {
                onlineAlterRetirementBarrier.awaitSafe(fence, timeout);
            }
            if (shadow) {
                physical.dropTable(sourceBinding, timeout);
            } else {
                physical.finishOnlineAlterIndexDescriptors(
                        targetWithBinding.storageBinding().orElseThrow(), descriptors, timeout);
            }
            DdlLogRecord committed = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
            tracker.observeDurable(committed);
            onlineAlterRuntime.logFiles().delete(captureId, changeLog.path());
            cleanSnapshotPublisher.publish();
            onlineDdlRegistry.complete(ddlId, OnlineDdlTerminalResult.COMPLETED,
                    Optional.empty(), false);
            return targetWithBinding;
        } catch (RuntimeException failure) {
            // FORWARD_ONLY之后任何反向动作都会破坏单向恢复；保留marker/journal/资源并让startup按target方向续作。
            if (forwardOnly) {
                completeOnlineDdlFailed(tracker, ddlId,
                        onlineErrorCode(failure), true);
                throw failure;
            }
            rollbackOnlineAlterBeforeForward(captureId, changeLog, descriptors,
                    shadowBinding, source, targetSdiWritten, markerPrepared,
                    gateCleared, journalClosed, tracker, ddlId, timeout, failure);
            throw failure;
        }
    }

    /** 两遍流式消费多ADD candidate；第一遍删全部before/after，第二遍按source current truth确保最终entry。 */
    private OnlineAlterDescriptorSet reconcileOnlineAlterIndexes(
            FileOnlineAlterChangeLog changeLog,
            MultiIndexAlterCandidateCodec outerCodec,
            Map<Integer, SecondaryIndexCandidateCodec> nestedCodecs,
            StorageTableDefinition sourceDefinition,
            TableStorageBinding sourceBinding,
            OnlineAlterDescriptorSet descriptors,
            List<OnlineAlterIndexAddRequest> additions,
            OnlineDdlOperationTracker tracker,
            DdlId ddlId) {
        Map<Integer, OnlineAlterIndexAddRequest> additionsByOrdinal = additions.stream()
                .collect(java.util.stream.Collectors.toMap(
                        OnlineAlterIndexAddRequest::actionOrdinal,
                        java.util.function.Function.identity()));
        int batchSize = onlineAlterRuntime.config().scanBatchRows();

        // 1、delete pass只保留当前批次payload；每个结构变化与descriptor exact-CAS已在storage短MTR内完成。
        long cursor = 0L;
        while (true) {
            List<cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecord> batch =
                    changeLog.readCandidatesAfter(cursor, batchSize);
            if (batch.isEmpty()) {
                break;
            }
            requireOnlineDdlNotCancelled(ddlId,
                    "before general ALTER reconciliation delete batch");
            for (var record : batch) {
                OnlineAlterCandidate candidate = outerCodec.decode(record.payload());
                for (OnlineAlterCandidateEntry entry : candidate.entries()) {
                    SecondaryIndexCandidateCodec nested = requireNestedCodec(
                            nestedCodecs, entry);
                    OnlineIndexCandidate decoded = nested.decode(entry.payload());
                    OnlineAlterIndexAddRequest addition = requireAddition(
                            additionsByOrdinal, entry);
                    if (decoded.beforeEntry().isPresent()) {
                        descriptors = physical.removeOnlineAlterIndexEntryExact(
                                sourceDefinition, sourceBinding, descriptors, addition,
                                decoded.beforeEntry().orElseThrow());
                    }
                    if (decoded.afterEntry().isPresent()) {
                        descriptors = physical.removeOnlineAlterIndexEntryExact(
                                sourceDefinition, sourceBinding, descriptors, addition,
                                decoded.afterEntry().orElseThrow());
                    }
                }
            }
            cursor = batch.getLast().sequence();
            sampleOnlineAlterDdl(tracker, sourceBinding.tableId(), changeLog);
        }

        // 2、ensure pass rewind到sequence 0；rollback/重复identity只增加幂等点查，不会把candidate当commit event。
        cursor = 0L;
        while (true) {
            List<cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecord> batch =
                    changeLog.readCandidatesAfter(cursor, batchSize);
            if (batch.isEmpty()) {
                break;
            }
            requireOnlineDdlNotCancelled(ddlId,
                    "before general ALTER reconciliation ensure batch");
            for (var record : batch) {
                OnlineAlterCandidate candidate = outerCodec.decode(record.payload());
                for (OnlineAlterCandidateEntry entry : candidate.entries()) {
                    OnlineIndexCandidate decoded = requireNestedCodec(
                            nestedCodecs, entry).decode(entry.payload());
                    LogicalRecord identity = decoded.afterEntry().orElseGet(() ->
                            decoded.beforeEntry().orElseThrow());
                    descriptors = physical.ensureOnlineAlterIndexCurrentForEntry(
                            sourceDefinition, sourceBinding, descriptors,
                            requireAddition(additionsByOrdinal, entry), identity);
                }
            }
            cursor = batch.getLast().sequence();
            sampleOnlineAlterDdl(tracker, sourceBinding.tableId(), changeLog);
        }
        return descriptors;
    }

    /** 两遍流式消费clustered identity；delete pass清旧像，ensure pass回读source current truth。 */
    private TableStorageBinding reconcileOnlineShadow(
            FileOnlineAlterChangeLog changeLog,
            OnlineClusteredIdentityCodec codec,
            StorageTableRebuildRequest request,
            TableStorageBinding shadow,
            OnlineDdlOperationTracker tracker,
            DdlId ddlId) {
        int batchSize = onlineAlterRuntime.config().scanBatchRows();
        long cursor = 0L;

        // 1、首遍只删除全部candidate旧像；在完整delete pass结束前绝不重插current truth。
        while (true) {
            List<cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecord> batch =
                    changeLog.readCandidatesAfter(cursor, batchSize);
            if (batch.isEmpty()) {
                break;
            }
            requireOnlineDdlNotCancelled(ddlId,
                    "before shadow reconciliation delete batch");
            for (var record : batch) {
                shadow = physical.deleteOnlineShadowIdentity(
                        request, shadow, codec.decode(record.payload()));
            }
            cursor = batch.getLast().sequence();
            sampleOnlineAlterDdl(tracker, request.sourceBinding().tableId(), changeLog);
        }

        // 2、rewind后才从source current truth幂等ensure；final X下source不再变化。
        cursor = 0L;
        while (true) {
            List<cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecord> batch =
                    changeLog.readCandidatesAfter(cursor, batchSize);
            if (batch.isEmpty()) {
                break;
            }
            requireOnlineDdlNotCancelled(ddlId,
                    "before shadow reconciliation ensure batch");
            for (var record : batch) {
                shadow = physical.ensureOnlineShadowIdentityCurrent(
                        request, shadow, codec.decode(record.payload()));
            }
            cursor = batch.getLast().sequence();
            sampleOnlineAlterDdl(tracker, request.sourceBinding().tableId(), changeLog);
        }
        return shadow;
    }

    /** candidate的ordinal/index id必须同时命中冻结codec，防止manifest内目标串线。 */
    private static SecondaryIndexCandidateCodec requireNestedCodec(
            Map<Integer, SecondaryIndexCandidateCodec> codecs,
            OnlineAlterCandidateEntry entry) {
        SecondaryIndexCandidateCodec codec = codecs.get(entry.actionOrdinal());
        if (codec == null) {
            throw new DictionaryDdlException(
                    "online ALTER candidate references unknown action ordinal: "
                            + entry.actionOrdinal());
        }
        return codec;
    }

    /** candidate entry与manifest ADD request执行ordinal/index双重交叉验证。 */
    private static OnlineAlterIndexAddRequest requireAddition(
            Map<Integer, OnlineAlterIndexAddRequest> additions,
            OnlineAlterCandidateEntry entry) {
        OnlineAlterIndexAddRequest addition = additions.get(entry.actionOrdinal());
        if (addition == null || addition.definition().indexId() != entry.indexId()) {
            throw new DictionaryDdlException(
                    "online ALTER candidate index identity does not match manifest: ordinal="
                            + entry.actionOrdinal() + " index=" + entry.indexId());
        }
        return addition;
    }

    /**
     * 在FORWARD_ONLY之前统一回滚通用ALTER，并把原始异常作为主失败保留。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>把tracker、journal与gate推进到ABORTING；用户取消保持CANCELLED诊断，其余失败标记INTERNAL_FAILURE。</li>
     *     <li>等待gate中已admit的DML/candidate写入退出后，再恢复source SDI并回收descriptor或shadow space。</li>
     *     <li>仅在全部物理补偿成功时把PREPARED marker推进ROLLED_BACK并恢复source cache。</li>
     *     <li>marker终态成立后清gate、关闭并精确删除journal；任一步失败均保留未决证据供启动恢复。</li>
     *     <li>向registry发布ROLLED_BACK或FAILED_CLOSED；所有cleanup异常作为suppressed附着于原始异常。</li>
     * </ol>
     *
     * @param captureId 已建立时的通用capture identity；prepare前失败可为空
     * @param changeLog 已创建的operation-owned journal；创建前失败可为空
     * @param descriptors INPLACE已经分配的完整descriptor集合；尚未分配或SHADOW时为空
     * @param shadow SHADOW已经创建的目标binding；尚未创建或INPLACE时为空
     * @param source initial X冻结的source aggregate；冻结前失败可为空
     * @param targetSdiWritten 是否已经把未获发布权的target image写入source或shadow SDI
     * @param markerPrepared PREPARED marker是否已经durable
     * @param gateCleared 正常路径是否已经清除capture，避免重复操作owner
     * @param journalClosed 正常路径是否已经关闭journal channel
     * @param tracker 已注册的运行期tracker；注册前失败可为空
     * @param ddlId 已预留的DDL identity；预留前失败可为空
     * @param timeout 每个有界清理协作者可使用的正等待预算
     * @param original 触发回滚的原始领域异常；cleanup异常附加为suppressed且不得替换它
     */
    private void rollbackOnlineAlterBeforeForward(
            OnlineDdlCaptureId captureId,
            FileOnlineAlterChangeLog changeLog,
            OnlineAlterDescriptorSet descriptors,
            TableStorageBinding shadow,
            TableDefinition source,
            boolean targetSdiWritten,
            boolean markerPrepared,
            boolean gateCleared,
            boolean journalClosed,
            OnlineDdlOperationTracker tracker,
            DdlId ddlId,
            Duration timeout,
            RuntimeException original) {
        boolean cleanupComplete = true;
        OnlineDdlAbortReason abortReason = original instanceof OnlineDdlCancellationException
                ? OnlineDdlAbortReason.CANCELLED : OnlineDdlAbortReason.INTERNAL_FAILURE;
        // 1. 先发布abort并等待既有DML退出，禁止在candidate仍可能追加时删除journal或物理target。
        if (tracker != null) {
            tracker.advanceRuntime(OnlineDdlRuntimePhase.ABORTING,
                    OnlineDdlWaitReason.NONE);
        }
        if (changeLog != null && !journalClosed) {
            try {
                changeLog.markAbortRequired(abortReason, timeout);
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        if (captureId != null && !gateCleared && source != null
                && onlineAlterRuntime.gate().phase(source.id().value())
                != OnlineDdlTablePhase.ABSENT) {
            try {
                if (onlineAlterRuntime.gate().phase(source.id().value())
                        != OnlineDdlTablePhase.ABORTING) {
                    onlineAlterRuntime.gate().beginAbort(
                            captureId, abortReason);
                }
                onlineAlterRuntime.gate().awaitAbortQuiescence(captureId, timeout);
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        // 2. gate已静止后才恢复source持久image并删除本operation拥有的descriptor/shadow资源。
        if (targetSdiWritten && source != null) {
            try {
                sdi.write(source, timeout);
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        if (descriptors != null && source != null) {
            try {
                physical.rollbackOnlineAlterIndexDescriptors(
                        source.storageBinding().orElseThrow(), descriptors, timeout);
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        if (shadow != null) {
            try {
                physical.dropTable(shadow, timeout);
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        // 3. 物理与SDI均收敛后才发布ROLLED_BACK；否则保留PREPARED让启动恢复重试。
        if (markerPrepared && ddlId != null && cleanupComplete) {
            try {
                DdlLogRecord current = repository.ddlLog().find(ddlId).orElseThrow(() ->
                        new DictionaryDdlLogStateException(
                                "online ALTER marker disappeared during rollback: ddl="
                                        + ddlId.value()));
                if (current.phase() == DdlLogPhase.PREPARED
                        && current.controlState() != DdlControlState.FORWARD_ONLY) {
                    repository.ddlLog().transition(
                            ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
                }
                if (source != null) {
                    cache.restoreTableAfterDdlRollback(source,
                            DictionaryVersion.of(current.marker().dictionaryVersion()));
                }
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        // 4. 只有durable/physical补偿完整时才清gate并删除journal；失败时保留两者阻止误继续。
        if (captureId != null && !gateCleared && source != null && cleanupComplete
                && onlineAlterRuntime.gate().phase(source.id().value())
                != OnlineDdlTablePhase.ABSENT) {
            try {
                onlineAlterRuntime.gate().clearCapture(captureId);
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        if (changeLog != null && !journalClosed) {
            try {
                changeLog.close();
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        if (changeLog != null && captureId != null && cleanupComplete) {
            try {
                onlineAlterRuntime.logFiles().delete(captureId, changeLog.path());
            } catch (RuntimeException cleanupFailure) {
                original.addSuppressed(cleanupFailure);
                cleanupComplete = false;
            }
        }
        // 5. registry仅投影最终补偿结果，不改变durable裁决；suppressed链保留所有清理根因。
        if (tracker != null && ddlId != null) {
            onlineDdlRegistry.complete(ddlId,
                    cleanupComplete ? OnlineDdlTerminalResult.ROLLED_BACK
                            : OnlineDdlTerminalResult.FAILED_CLOSED,
                    Optional.of(onlineErrorCode(original)), false);
        }
    }

    /** 从有序actions和最终index集合建立ADD请求；任一ADD最终不可见时在side effect前拒绝。 */
    private static List<OnlineAlterIndexAddRequest> onlineAlterAdditions(
            List<AlterTableAction> actions, List<IndexDefinition> targetIndexes) {
        List<OnlineAlterIndexAddRequest> result = new ArrayList<>();
        for (int ordinal = 0; ordinal < actions.size(); ordinal++) {
            if (actions.get(ordinal) instanceof AlterTableAction.AddIndex add) {
                IndexDefinition target = targetIndexes.stream()
                        .filter(index -> index.name().equals(add.index().name()))
                        .findFirst().orElseThrow(() -> new DatabaseValidationException(
                                "online ALTER ADD INDEX is absent from final aggregate: "
                                        + add.index().name().displayName()));
                result.add(new OnlineAlterIndexAddRequest(
                        ordinal, storageIndex(target)));
            }
        }
        return List.copyOf(result);
    }

    /** 从source committed aggregate建立DROP exact binding请求；不能删除同语句临时ADD或聚簇索引。 */
    private static List<OnlineAlterIndexDropRequest> onlineAlterDrops(
            List<AlterTableAction> actions, TableDefinition source,
            TableStorageBinding sourceBinding) {
        Map<Long, IndexStorageBinding> bindings = sourceBinding.indexes().stream()
                .collect(java.util.stream.Collectors.toMap(
                        IndexStorageBinding::indexId,
                        java.util.function.Function.identity()));
        List<OnlineAlterIndexDropRequest> result = new ArrayList<>();
        for (int ordinal = 0; ordinal < actions.size(); ordinal++) {
            if (actions.get(ordinal) instanceof AlterTableAction.DropIndex drop) {
                IndexDefinition sourceIndex = source.indexes().stream()
                        .filter(index -> index.name().equals(drop.name()))
                        .findFirst().orElseThrow(() -> new DatabaseValidationException(
                                "online ALTER DROP INDEX is absent from source aggregate: "
                                        + drop.name().displayName()));
                if (sourceIndex.clustered()) {
                    throw new DatabaseValidationException(
                            "online ALTER cannot drop clustered index");
                }
                IndexStorageBinding binding = Optional.ofNullable(
                        bindings.get(sourceIndex.id().value())).orElseThrow(() ->
                        new DatabaseValidationException(
                                "online ALTER DROP INDEX has no source binding: "
                                        + sourceIndex.id().value()));
                result.add(new OnlineAlterIndexDropRequest(ordinal, binding));
            }
        }
        return List.copyOf(result);
    }

    /** INPLACE保留source row-format/space/path，只替换最终索引逻辑定义。 */
    private static StorageTableDefinition onlineInplaceDefinition(
            StorageTableDefinition source, List<IndexDefinition> indexes) {
        return onlineInplaceStorageDefinition(source,
                indexes.stream().map(DictionaryDdlService::storageIndex).toList());
    }

    /** 使用已经映射的storage index集合构造同row-format INPLACE definition。 */
    private static StorageTableDefinition onlineInplaceStorageDefinition(
            StorageTableDefinition source, List<StorageIndexDefinition> indexes) {
        return new StorageTableDefinition(source.tableId(), source.spaceId(), source.path(),
                source.schemaVersion(), source.initialSizeInPages(), source.columns(), indexes);
    }

    /** source definition后追加全部ADD定义，供capture codec解析source row到staged secondary layout。 */
    private static List<StorageIndexDefinition> appendAddedStorageIndexes(
            List<StorageIndexDefinition> source,
            List<OnlineAlterIndexAddRequest> additions) {
        List<StorageIndexDefinition> result = new ArrayList<>(source);
        additions.stream().map(OnlineAlterIndexAddRequest::definition)
                .forEach(result::add);
        return List.copyOf(result);
    }

    /** source全部binding后追加ADD descriptors，构造base-scan/candidate用临时aggregate。 */
    private static TableStorageBinding onlineInplaceBinding(
            TableStorageBinding source, OnlineAlterDescriptorSet descriptors) {
        List<IndexStorageBinding> bindings = new ArrayList<>(source.indexes());
        descriptors.descriptors().stream()
                .filter(descriptor -> descriptor.action()
                        == OnlineAlterIndexDescriptorAction.ADD)
                .map(OnlineAlterIndexDescriptor::indexBinding).forEach(bindings::add);
        return new TableStorageBinding(source.tableId(), source.spaceId(), source.path(),
                source.rowFormatVersion(), bindings, source.lobSegment());
    }

    /** 按最终DD index逻辑顺序从source与ADD descriptor组合唯一target binding，DROP自然不再可达。 */
    private static TableStorageBinding onlineInplaceTargetBinding(
            TableStorageBinding source, List<IndexDefinition> targetIndexes,
            OnlineAlterDescriptorSet descriptors) {
        Map<Long, IndexStorageBinding> byId = new LinkedHashMap<>();
        source.indexes().forEach(binding -> byId.put(binding.indexId(), binding));
        descriptors.descriptors().stream()
                .filter(descriptor -> descriptor.action()
                        == OnlineAlterIndexDescriptorAction.ADD)
                .map(OnlineAlterIndexDescriptor::indexBinding)
                .forEach(binding -> byId.put(binding.indexId(), binding));
        List<IndexStorageBinding> ordered = targetIndexes.stream()
                .map(index -> Optional.ofNullable(byId.get(index.id().value()))
                        .orElseThrow(() -> new DatabaseValidationException(
                                "online ALTER target binding misses index "
                                        + index.id().value())))
                .toList();
        return new TableStorageBinding(source.tableId(), source.spaceId(), source.path(),
                source.rowFormatVersion(), ordered, source.lobSegment());
    }

    /** SHA-256绑定manifest bytes与descriptor anchor；算法缺失属于JVM致命配置错误。 */
    private static byte[] sha256(byte[] value) {
        if (value == null) {
            throw new DatabaseValidationException(
                    "online ALTER manifest bytes must not be null");
        }
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new cn.zhangyis.db.common.exception.DatabaseFatalException(
                    "SHA-256 is unavailable for online ALTER manifest", unavailable);
        }
    }

    /** Duration过大时饱和为Condition可表达的正纳秒预算。 */
    private static long boundedTimeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    /** 从单一finalization deadline计算下一屏障剩余预算。 */
    private static Duration remainingDuration(
            long started, long budget, String stage) {
        long elapsed = System.nanoTime() - started;
        if (elapsed < 0 || elapsed >= budget) {
            throw new OnlineAlterFinalizationTimeoutException(
                    "online ALTER finalization deadline expired before " + stage);
        }
        return Duration.ofNanos(budget - elapsed);
    }

    /**
     * 用target SDI作为持久manifest执行metadata-only ALTER；target SDI先于FORWARD_ONLY，因此source+OPEN或
     * CANCEL_REQUESTED恢复可以从committed source DD重写SDI，source+FORWARD_ONLY则可从SDI重建target DD。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>冻结source/target digest并写PREPARED/OPEN marker，向统一registry发布cancel-capable identity。</li>
     *     <li>在table X下先写target SDI；取消若胜出则用source aggregate恢复SDI并写ROLLED_BACK。</li>
     *     <li>target SDI durable后竞争OPEN→FORWARD_ONLY，胜出后一次提交target DD并推进DICTIONARY_COMMITTED。</li>
     *     <li>发布target cache和COMMITTED；forward后的任一失败保留marker/SDI供启动恢复，不逆向提交source。</li>
     * </ol>
     *
     * @param owner 当前独立DDL statement owner，用于诊断但不进入持久格式
     * @param command 已冻结策略的metadata-only有序命令
     * @param sourceSchema source aggregate所属schema checkpoint
     * @param targetSchema rename后的target schema checkpoint；未跨schema时与source相同
     * @param source table X下重读的committed ACTIVE aggregate
     * @param target 已应用全部metadata action且携带同一物理binding的目标aggregate
     * @param ddlId 本次control reserve固定的正DDL identity
     * @param timeout SDI、DD与marker写入共用的正等待上界
     * @return 已提交并发布cache的target aggregate
     * @throws DictionaryDdlException marker、SDI或DD发布失败时抛出；FORWARD_ONLY后调用方只能重启前滚
     */
    private TableDefinition alterTableInstantMetadata(
            MdlOwnerId owner, AlterTableCommand command,
            SchemaDefinition sourceSchema, SchemaDefinition targetSchema,
            TableDefinition source, TableDefinition target,
            DdlId ddlId, Duration timeout) {
        TableStorageBinding binding = source.storageBinding().orElseThrow(() ->
                new DictionaryDdlException(
                        "metadata ALTER source has no physical binding: " + source.id().value()));
        OnlineDdlOperationTracker tracker = onlineDdlRegistry.register(
                new OnlineDdlOperationIdentity(ddlId, DdlLogOperation.ALTER_TABLE_INPLACE,
                        source.id().value(), 0L, command.table().canonicalKey(), "",
                        source.version().value(), target.version().value(), owner.value(), false,
                        java.util.OptionalLong.empty()),
                DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1);
        boolean markerPrepared = false;
        boolean forwardOnly = false;
        try {
            // 1、marker把两个canonical image固定到同一owner；prepare handoff允许取消在线程真正写catalog前胜出。
            tracker.advanceRuntime(OnlineDdlRuntimePhase.ACTIVATING, OnlineDdlWaitReason.NONE);
            if (!tracker.beginDurablePrepare()) {
                throw new OnlineDdlCancellationException(
                        "metadata ALTER cancelled before durable prepare: ddl=" + ddlId.value());
            }
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), target.version().value(), source.id().value()),
                    0L, DdlLogOperation.ALTER_TABLE_INPLACE, DdlLogPhase.PREPARED,
                    binding.spaceId(), binding.path(), Optional.empty(), Optional.empty(),
                    DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1,
                    Optional.of(schemaDigests.digest(
                            sourceSchema, source, binding.rowFormatVersion())),
                    Optional.empty(), Optional.of(schemaDigests.digest(
                    targetSchema, target, binding.rowFormatVersion())),
                    DdlControlState.OPEN, Optional.empty(), Optional.empty());
            try {
                repository.ddlLog().prepare(prepared);
            } catch (RuntimeException prepareFailure) {
                tracker.failDurablePrepare("MARKER_PREPARE_FAILED");
                throw prepareFailure;
            }
            markerPrepared = true;
            tracker.markDurablePrepared(prepared);
            requireOnlineDdlNotCancelled(ddlId, "after metadata ALTER prepare");

            // 2、SDI是metadata-only恢复manifest；仍为OPEN时它没有反向发布DD的权力，取消/普通失败会重写source。
            tracker.advanceRuntime(OnlineDdlRuntimePhase.PUBLISHING, OnlineDdlWaitReason.NONE);
            sdi.write(target, timeout);
            faultInjector.afterInplaceAlterTargetSdi(prepared);
            requireOnlineDdlNotCancelled(ddlId, "after metadata ALTER target SDI");

            // 3、FORWARD_ONLY只有在target SDI已durable后才能发布；恢复因此总能从page3解码完整target aggregate。
            DdlControlCasResult direction = repository.ddlLog().compareAndSetControl(
                    ddlId, DdlLogPhase.PREPARED, DdlControlState.OPEN,
                    DdlControlState.FORWARD_ONLY, Optional.empty());
            if (!direction.changed()) {
                if (direction.observedRecord().controlState()
                        == DdlControlState.CANCEL_REQUESTED) {
                    throw new OnlineDdlCancellationException(
                            "metadata ALTER cancellation won before forward fence: ddl="
                                    + ddlId.value());
                }
                throw new DictionaryDdlLogStateException(
                        "metadata ALTER observed unexpected control direction: ddl="
                                + ddlId.value());
            }
            forwardOnly = true;
            tracker.observeDurable(direction.observedRecord());
            tracker.advanceRuntime(
                    OnlineDdlRuntimePhase.FORWARD_FENCED, OnlineDdlWaitReason.NONE);
            faultInjector.afterInplaceAlterForwardFenced(direction.observedRecord());
            cache.invalidateTable(source.id(), target.version());
            commitUpdate(target.version(), target);
            DdlLogRecord dictionaryCommitted = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            tracker.observeDurable(dictionaryCommitted);
            faultInjector.afterInplaceAlterDictionaryCommitted(target);

            // 4、cache只发布committed aggregate；terminal append失败不改变target DD已经获胜的事实。
            cache.publishTable(target);
            DdlLogRecord committed = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
            tracker.observeDurable(committed);
            cleanSnapshotPublisher.publish();
            onlineDdlRegistry.complete(ddlId, OnlineDdlTerminalResult.COMPLETED,
                    Optional.empty(), false);
            log.info("completed instant metadata ALTER: table={} ddlId={} version={}",
                    command.table().canonicalKey(), ddlId.value(), target.version().value());
            return target;
        } catch (RuntimeException failure) {
            if (forwardOnly) {
                completeOnlineDdlFailed(tracker, ddlId, onlineErrorCode(failure), true);
                throw failure;
            }
            if (!markerPrepared && tracker.snapshot().terminalResult()
                    == OnlineDdlTerminalResult.FAILED_CLOSED) {
                // marker append outcome未形成可回滚证据；保持原失败并把同一FAILED_CLOSED投影移入有界history。
                completeOnlineDdlFailed(tracker, ddlId, onlineErrorCode(failure), false);
                throw failure;
            }
            // 2、未越过forward fence时committed source仍是权威；重写SDI后才允许把marker终结为ROLLED_BACK。
            if (markerPrepared) {
                try {
                    sdi.write(source, timeout);
                    DdlLogRecord current = repository.ddlLog().find(ddlId).orElseThrow(() ->
                            new DictionaryDdlLogStateException(
                                    "metadata ALTER marker disappeared: ddl=" + ddlId.value()));
                    if (current.phase() == DdlLogPhase.PREPARED
                            && current.controlState() != DdlControlState.FORWARD_ONLY) {
                        DdlLogRecord rolledBack = repository.ddlLog().transition(
                                ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
                        tracker.observeDurable(rolledBack);
                    }
                } catch (RuntimeException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                    completeOnlineDdlFailed(tracker, ddlId,
                            "METADATA_ROLLBACK_FAILED", false);
                    throw failure;
                }
            }
            onlineDdlRegistry.complete(ddlId, OnlineDdlTerminalResult.ROLLED_BACK,
                    Optional.of(onlineErrorCode(failure)), false);
            throw failure;
        }
    }

    /**
     * 在内存 staged aggregate 上顺序应用 actions；本方法不预留 identity、不写 catalog/SDI/文件。
     * sourceOrdinal 始终指向 ALTER 开始时的物理列，新增列携带已经类型化的 storage default。
     */
    private AlterDraft stageAlter(
            TableDefinition active, List<AlterTableAction> actions) {
        List<StagedColumn> columns = new ArrayList<>();
        for (ColumnDefinition column : active.columns()) {
            columns.add(new StagedColumn(column, column.ordinal(), Optional.empty()));
        }
        List<StagedIndex> indexes = active.indexes().stream()
                .map(index -> new StagedIndex(index, false))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        SchemaId schemaId = active.schemaId();
        ObjectName tableName = active.name();
        TableOptions options = active.options();
        long nextColumnId = active.columns().stream()
                .mapToLong(ColumnDefinition::columnId).max().orElse(0L) + 1L;
        long nextTemporaryIndexId = active.indexes().stream()
                .mapToLong(index -> index.id().value()).max().orElse(0L) + 1L;
        boolean structural = false;

        for (AlterTableAction action : actions) {
            switch (action) {
                case AlterTableAction.AddColumn add -> {
                    if (findColumn(columns, add.name()).isPresent()) {
                        throw new DictionaryDdlException(
                                "ALTER ADD COLUMN already exists: " + add.name().displayName());
                    }
                    ColumnTypeDefinition type = inheritCharacterDefaults(add.type(), options);
                    int position = switch (add.position().kind()) {
                        case LAST -> columns.size();
                        case FIRST -> 0;
                        case AFTER -> findColumnIndex(
                                columns, add.position().afterColumn().orElseThrow()) + 1;
                    };
                    ColumnDefinition definition = new ColumnDefinition(
                            nextColumnId++, add.name(), type, position, add.defaultDefinition());
                    columns.add(position, new StagedColumn(
                            definition, -1, add.storageDefault()));
                    columns = renumberColumns(columns);
                    structural = true;
                }
                case AlterTableAction.DropColumn drop -> {
                    int ordinal = findColumnIndex(columns, drop.name());
                    long columnId = columns.get(ordinal).definition().columnId();
                    boolean clusteredKey = indexes.stream().map(StagedIndex::definition)
                            .filter(IndexDefinition::clustered)
                            .flatMap(index -> index.keyParts().stream())
                            .anyMatch(part -> part.columnId() == columnId);
                    if (clusteredKey) {
                        throw new DictionaryDdlException(
                                "ALTER DROP COLUMN cannot remove clustered key column: "
                                        + drop.name().displayName());
                    }
                    columns.remove(ordinal);
                    if (columns.isEmpty()) {
                        throw new DictionaryDdlException(
                                "ALTER TABLE cannot remove the final column");
                    }
                    columns = renumberColumns(columns);
                    List<StagedIndex> rewritten = new ArrayList<>();
                    for (StagedIndex staged : indexes) {
                        List<IndexKeyPart> parts = staged.definition().keyParts().stream()
                                .filter(part -> part.columnId() != columnId).toList();
                        if (parts.isEmpty()) {
                            if (staged.definition().clustered()) {
                                throw new DictionaryDdlException(
                                        "clustered index became empty during DROP COLUMN");
                            }
                            continue;
                        }
                        rewritten.add(new StagedIndex(new IndexDefinition(
                                staged.definition().id(), staged.definition().name(),
                                staged.definition().unique(), staged.definition().clustered(),
                                parts), staged.requiresIdentity()));
                    }
                    indexes = rewritten;
                    structural = true;
                }
                case AlterTableAction.AddIndex add -> {
                    if (indexes.stream().anyMatch(index ->
                            index.definition().name().equals(add.index().name()))) {
                        throw new DictionaryDdlException(
                                "ALTER ADD INDEX already exists: "
                                        + add.index().name().displayName());
                    }
                    Map<ObjectName, Long> columnIds = new LinkedHashMap<>();
                    columns.forEach(column -> columnIds.put(
                            column.definition().name(), column.definition().columnId()));
                    List<IndexKeyPart> parts = add.index().keyParts().stream()
                            .map(part -> new IndexKeyPart(
                                    requireColumnId(columnIds, part.columnName()),
                                    part.order(), part.prefixBytes())).toList();
                    indexes.add(new StagedIndex(new IndexDefinition(
                            IndexId.of(nextTemporaryIndexId++), add.index().name(),
                            add.index().unique(), false, parts), true));
                    structural = true;
                }
                case AlterTableAction.DropIndex drop -> {
                    int ordinal = findIndexIndex(indexes, drop.name());
                    if (indexes.get(ordinal).definition().clustered()) {
                        throw new DictionaryDdlException(
                                "ALTER DROP INDEX cannot remove clustered primary index: "
                                        + drop.name().displayName());
                    }
                    indexes.remove(ordinal);
                    structural = true;
                }
                case AlterTableAction.Rename rename -> {
                    if (!rename.target().catalog().equals(ObjectName.of("def"))) {
                        throw new DictionaryDdlException(
                                "ALTER RENAME only supports catalog def");
                    }
                    SchemaDefinition targetSchema = repository.findSchema(rename.target().schema())
                            .orElseThrow(() -> new DictionaryObjectNotFoundException(
                                    "rename target schema does not exist: "
                                            + rename.target().schema().displayName()));
                    schemaId = targetSchema.id();
                    tableName = rename.target().table();
                }
                case AlterTableAction.Comment comment ->
                        options = new TableOptions(
                                comment.value(), options.defaultCharsetId(),
                                options.defaultCollationId());
                case AlterTableAction.DefaultCharset charset ->
                        options = new TableOptions(
                                options.comment(), charset.charsetId(), charset.collationId());
                case AlterTableAction.ConvertCharset charset -> {
                    options = new TableOptions(
                            options.comment(), charset.charsetId(), charset.collationId());
                    List<StagedColumn> converted = new ArrayList<>(columns.size());
                    for (StagedColumn column : columns) {
                        ColumnTypeDefinition before = column.definition().type();
                        ColumnTypeDefinition after = isCharacterType(before.typeId())
                                ? new ColumnTypeDefinition(
                                before.typeId(), before.unsigned(), before.nullable(),
                                before.length(), before.scale(), charset.charsetId(),
                                charset.collationId(), before.symbols()) : before;
                        converted.add(new StagedColumn(new ColumnDefinition(
                                column.definition().columnId(), column.definition().name(),
                                after, column.definition().ordinal(),
                                column.definition().defaultDefinition()),
                                column.sourceOrdinal(), column.storageDefault()));
                    }
                    columns = converted;
                    structural = true;
                }
            }
        }
        return new AlterDraft(schemaId, tableName, options,
                List.copyOf(columns), List.copyOf(indexes), structural);
    }

    /** 给最终仍存在的 ADD INDEX 分配 control 预留的连续全局 identity。 */
    private static List<IndexDefinition> assignAlterIndexIds(
            List<StagedIndex> staged, long firstIndexId) {
        long next = firstIndexId;
        List<IndexDefinition> result = new ArrayList<>(staged.size());
        for (StagedIndex index : staged) {
            IndexDefinition definition = index.definition();
            result.add(index.requiresIdentity() ? new IndexDefinition(
                    IndexId.of(next++), definition.name(), definition.unique(),
                    definition.clustered(), definition.keyParts()) : definition);
        }
        return List.copyOf(result);
    }

    /** 目标逻辑名必须为空或仍指向本 table id。 */
    private void ensureAlterTargetNameAvailable(
            TableDefinition active, SchemaId schemaId, ObjectName name) {
        repository.findTable(schemaId, name).ifPresent(existing -> {
            if (!existing.id().equals(active.id())) {
                throw new cn.zhangyis.db.dd.exception.DictionaryObjectExistsException(
                        "ALTER target table name already exists: " + name.displayName());
            }
        });
    }

    /** 已组装 published aggregate 时复用目标名称校验。 */
    private void ensureAlterTargetNameAvailable(
            TableDefinition active, TableDefinition published) {
        ensureAlterTargetNameAvailable(active, published.schemaId(), published.name());
    }

    /** 物理 shadow 成功但 DD 尚未提交时精确清理；失败保留非终态 marker 供恢复。 */
    private void rollbackAlterShadow(
            DdlId ddlId, TableStorageBinding shadow, Duration timeout,
            RuntimeException original) {
        if (shadow == null) {
            return;
        }
        try {
            physical.dropTable(shadow, timeout);
            DdlLogPhase phase = repository.ddlLog().find(ddlId).orElseThrow().phase();
            if (phase == DdlLogPhase.PREPARED || phase == DdlLogPhase.ENGINE_DONE) {
                repository.ddlLog().transition(ddlId, phase, DdlLogPhase.ROLLED_BACK);
            }
        } catch (RuntimeException cleanupFailure) {
            original.addSuppressed(cleanupFailure);
        }
    }

    /** 获取源与全部 rename 目标的 schema IX/table X，按 MdlKey 全序避免跨 schema 双向 rename 死锁。 */
    private AlterMdlTickets acquireAlterTickets(
            MdlOwnerId owner, AlterTableCommand command, Duration timeout) {
        Set<MdlKey> schemaKeys = new java.util.TreeSet<>();
        Set<MdlKey> tableKeys = new java.util.TreeSet<>();
        schemaKeys.add(MdlKey.schema(command.table().schema().canonicalName()));
        tableKeys.add(MdlKey.table(command.table().canonicalKey()));
        for (AlterTableAction action : command.actions()) {
            if (action instanceof AlterTableAction.Rename rename) {
                schemaKeys.add(MdlKey.schema(rename.target().schema().canonicalName()));
                tableKeys.add(MdlKey.table(rename.target().canonicalKey()));
            }
        }
        List<MdlTicket> acquired = new ArrayList<>();
        try {
            for (MdlKey key : schemaKeys) {
                acquired.add(locks.acquire(new MdlRequest(
                        owner, key, MdlMode.INTENTION_EXCLUSIVE,
                        MdlDuration.TRANSACTION), timeout));
            }
            for (MdlKey key : tableKeys) {
                acquired.add(locks.acquire(new MdlRequest(
                        owner, key, MdlMode.EXCLUSIVE,
                        MdlDuration.TRANSACTION), timeout));
            }
            return new AlterMdlTickets(acquired);
        } catch (RuntimeException failure) {
            closeTickets(acquired, failure);
            throw failure;
        }
    }

    /** 新字符列的 0/0 继承哨兵在 action 当时的 staged table options 下解析。 */
    private static ColumnTypeDefinition inheritCharacterDefaults(
            ColumnTypeDefinition type, TableOptions options) {
        if (!isCharacterType(type.typeId())) {
            return type;
        }
        int charset = type.charsetId() == 0
                ? options.defaultCharsetId() : type.charsetId();
        int collation = type.collationId() == 0
                ? options.defaultCollationId() : type.collationId();
        return new ColumnTypeDefinition(
                type.typeId(), type.unsigned(), type.nullable(), type.length(),
                type.scale(), charset, collation, type.symbols());
    }

    /** DD 字符类型集合；binary/blob 不参与 character set conversion。 */
    private static boolean isCharacterType(DictionaryTypeId type) {
        return switch (type) {
            case CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, JSON -> true;
            default -> false;
        };
    }

    private static Optional<StagedColumn> findColumn(
            List<StagedColumn> columns, ObjectName name) {
        return columns.stream().filter(column ->
                column.definition().name().equals(name)).findFirst();
    }

    private static int findColumnIndex(
            List<StagedColumn> columns, ObjectName name) {
        for (int index = 0; index < columns.size(); index++) {
            if (columns.get(index).definition().name().equals(name)) {
                return index;
            }
        }
        throw new DictionaryObjectNotFoundException(
                "ALTER column does not exist: " + name.displayName());
    }

    private static int findIndexIndex(
            List<StagedIndex> indexes, ObjectName name) {
        for (int index = 0; index < indexes.size(); index++) {
            if (indexes.get(index).definition().name().equals(name)) {
                return index;
            }
        }
        throw new DictionaryObjectNotFoundException(
                "ALTER index does not exist: " + name.displayName());
    }

    /** 列移动后只重建 ordinal，稳定 column id/source projection/default 不变。 */
    private static List<StagedColumn> renumberColumns(
            List<StagedColumn> columns) {
        List<StagedColumn> result = new ArrayList<>(columns.size());
        for (int ordinal = 0; ordinal < columns.size(); ordinal++) {
            StagedColumn column = columns.get(ordinal);
            ColumnDefinition definition = column.definition();
            result.add(new StagedColumn(new ColumnDefinition(
                    definition.columnId(), definition.name(), definition.type(),
                    ordinal, definition.defaultDefinition()),
                    column.sourceOrdinal(), column.storageDefault()));
        }
        return result;
    }

    /** 关闭部分取得的动态 MDL ticket，并把 cleanup failure 附到原异常。 */
    private static void closeTickets(
            List<MdlTicket> tickets, RuntimeException original) {
        for (int index = tickets.size() - 1; index >= 0; index--) {
            try {
                tickets.get(index).close();
            } catch (RuntimeException closeFailure) {
                original.addSuppressed(closeFailure);
            }
        }
    }

    /** 动态多名称 ALTER 的 RAII ticket owner。 */
    private static final class AlterMdlTickets implements AutoCloseable {
        private final List<MdlTicket> tickets;

        private AlterMdlTickets(List<MdlTicket> tickets) {
            this.tickets = List.copyOf(tickets);
        }

        /**
         * 返回本 owner 已持有的精确 MDL ticket，供在线阶段在同一 ticket 上降级或升级。
         *
         * @param key 已按全局名称顺序取得的 MDL key
         * @return 与 key 完全相等且仍由本 owner 管理的 ticket
         * @throws DictionaryDdlException 调用方请求了未取得的 key 时抛出；现有 ticket 保持持有并由 close 释放
         */
        private MdlTicket require(MdlKey key) {
            return tickets.stream()
                    .filter(ticket -> ticket.key().equals(key))
                    .findFirst()
                    .orElseThrow(() -> new DictionaryDdlException(
                            "ALTER MDL ticket is not owned for key: " + key));
        }

        @Override
        public void close() {
            RuntimeException failure = null;
            for (int index = tickets.size() - 1; index >= 0; index--) {
                try {
                    tickets.get(index).close();
                } catch (RuntimeException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    /** staged 列同时保存源物理 ordinal 或新增列 default。 */
    private record StagedColumn(
            ColumnDefinition definition, int sourceOrdinal,
            Optional<StorageDefaultValue> storageDefault) {
    }

    /** requiresIdentity 区分本语句新建索引与既有稳定索引。 */
    private record StagedIndex(
            IndexDefinition definition, boolean requiresIdentity) {
    }

    /** 一次保序 action 求值后的纯内存结果。 */
    private record AlterDraft(
            SchemaId schemaId, ObjectName name, TableOptions options,
            List<StagedColumn> columns, List<StagedIndex> indexes,
            boolean structural) {
    }

    /**
     * 校验当前状态后推进数据字典状态机；成功发布唯一终态，失败保留可回滚或可恢复的原始状态。
     *
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     */
    private void commitUpdate(DictionaryVersion version, TableDefinition table) {
        try (DictionaryTransaction transaction = repository.begin(version)) {
            transaction.updateTable(table);
            transaction.commit();
        }
    }

    private static TableDefinition lifecycle(TableDefinition before, DictionaryVersion version, TableState state) {
        return new TableDefinition(before.id(), before.schemaId(), before.name(), version, state,
                before.columns(), before.indexes(), before.storageBinding(), before.options());
    }

    /**
     * 构造只用于canonical digest的逻辑版本；物理binding被digest语法排除，避免尚未创建的segment/root污染计划值。
     *
     * @param before initial X下读取的source aggregate，提供table/schema/name/columns/options
     * @param version 已预留且将由target aggregate发布的dictionary version
     * @param state operation策略规定的target lifecycle state
     * @param indexes target按aggregate ordinal排列的完整索引集合
     * @return 不携带物理binding、但逻辑字段与未来target完全一致的不可变aggregate
     */
    private static TableDefinition logicalVersion(
            TableDefinition before, DictionaryVersion version, TableState state,
            List<IndexDefinition> indexes) {
        return new TableDefinition(before.id(), before.schemaId(), before.name(), version, state,
                before.columns(), indexes, Optional.empty(), before.options());
    }

    /**
     * 复制source索引顺序并在末尾追加新二级索引，保证digest与最终DD aggregate使用同一ordinal。
     *
     * @param source initial X下冻结的完整索引集合
     * @param added 已分配稳定identity、尚未创建物理segment的新索引
     * @return 保持source顺序并追加added的不可变target集合
     */
    private static List<IndexDefinition> appendIndex(
            List<IndexDefinition> source, IndexDefinition added) {
        List<IndexDefinition> target = new ArrayList<>(source);
        target.add(added);
        return List.copyOf(target);
    }

    /**
     * final X下比较live source与PREPARED保存的canonical checkpoint；不一致时禁止越过forward fence。
     *
     * @param expected marker中initial X时已经durable的source digest
     * @param actual final X下从当前committed source重新计算的digest
     * @param ddlId 用于定位保留marker/sidecar的DDL identity
     * @param checkpoint 诊断使用的固定checkpoint名称
     * @throws DictionaryDdlException schema ownership或version被意外改写时抛出；control仍为OPEN，调用方可安全回滚
     */
    private static void requireLiveDigest(
            DdlSchemaDigest expected, DdlSchemaDigest actual,
            DdlId ddlId, String checkpoint) {
        if (!expected.equals(actual)) {
            throw new DictionaryDdlException(
                    "DDL live schema digest mismatch: ddl=" + ddlId.value()
                            + " checkpoint=" + checkpoint);
        }
    }

    private static List<ColumnDefinition> columns(CreateTableCommand command) {
        List<ColumnDefinition> columns = new ArrayList<>(command.columns().size());
        for (int ordinal = 0; ordinal < command.columns().size(); ordinal++) {
            CreateColumnSpec spec = command.columns().get(ordinal);
            columns.add(new ColumnDefinition(ordinal + 1L, spec.name(), spec.type(), ordinal));
        }
        return List.copyOf(columns);
    }

    private static List<IndexDefinition> indexes(CreateTableCommand command, long firstIndexId,
                                                 List<ColumnDefinition> columns) {
        Map<ObjectName, Long> columnIds = new LinkedHashMap<>();
        for (ColumnDefinition column : columns) {
            columnIds.put(column.name(), column.columnId());
        }
        List<IndexDefinition> indexes = new ArrayList<>(command.indexes().size());
        for (int ordinal = 0; ordinal < command.indexes().size(); ordinal++) {
            CreateIndexSpec spec = command.indexes().get(ordinal);
            List<IndexKeyPart> parts = spec.keyParts().stream().map(part -> new IndexKeyPart(
                    requireColumnId(columnIds, part.columnName()), part.order(), part.prefixBytes())).toList();
            indexes.add(new IndexDefinition(IndexId.of(firstIndexId + ordinal), spec.name(), spec.unique(),
                    spec.clustered(), parts));
        }
        return List.copyOf(indexes);
    }

    private static long requireColumnId(Map<ObjectName, Long> columns, ObjectName name) {
        Long id = columns.get(name);
        if (id == null) {
            throw new DatabaseValidationException("index references missing CREATE column: " + name);
        }
        return id;
    }

    private static StorageTableDefinition storageDefinition(TableId tableId, int spaceId, Path path,
                                                            DictionaryVersion version, CreateTableCommand command,
                                                            List<ColumnDefinition> columns,
                                                            List<IndexDefinition> indexes) {
        List<StorageColumnDefinition> storageColumns = columns.stream().map(column ->
                new StorageColumnDefinition(column.columnId(), column.name().displayName(), column.ordinal(),
                        storageType(column.type()))).toList();
        List<StorageIndexDefinition> storageIndexes = indexes.stream().map(index ->
                new StorageIndexDefinition(index.id().value(), index.name().displayName(), index.unique(),
                        index.clustered(), index.keyParts().stream().map(part -> new StorageIndexKeyPart(
                        part.columnId(), part.order() == cn.zhangyis.db.dd.domain.IndexOrder.ASC
                                ? StorageIndexOrder.ASC : StorageIndexOrder.DESC,
                        part.prefixBytes())).toList())).toList();
        return new StorageTableDefinition(tableId.value(), cn.zhangyis.db.domain.SpaceId.of(spaceId), path,
                version.value(), command.initialSizeInPages(), storageColumns, storageIndexes);
    }

    /** 为 shadow target 组装全新 space/version 的完整 storage schema。 */
    private static StorageTableDefinition storageDefinition(
            TableId tableId, int spaceId, Path path, DictionaryVersion version,
            List<ColumnDefinition> columns, List<IndexDefinition> indexes) {
        List<StorageColumnDefinition> storageColumns = columns.stream().map(column ->
                new StorageColumnDefinition(
                        column.columnId(), column.name().displayName(), column.ordinal(),
                        storageType(column.type()))).toList();
        List<StorageIndexDefinition> storageIndexes = indexes.stream()
                .map(DictionaryDdlService::storageIndex).toList();
        return new StorageTableDefinition(
                tableId.value(), cn.zhangyis.db.domain.SpaceId.of(spaceId), path,
                version.value(), cn.zhangyis.db.domain.PageNo.of(64),
                storageColumns, storageIndexes);
    }

    /** 将 committed table/binding 映射为 shadow scan 使用的 exact source row format。 */
    private static StorageTableDefinition storageDefinition(TableDefinition table) {
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryDdlException(
                        "ALTER source has no storage binding: " + table.id().value()));
        List<StorageColumnDefinition> columns = table.columns().stream().map(column ->
                new StorageColumnDefinition(
                        column.columnId(), column.name().displayName(), column.ordinal(),
                        storageType(column.type()))).toList();
        return new StorageTableDefinition(
                table.id().value(), binding.spaceId(), binding.path(),
                binding.rowFormatVersion(), cn.zhangyis.db.domain.PageNo.of(1),
                columns, table.indexes().stream()
                .map(DictionaryDdlService::storageIndex).toList());
    }

    private static StorageColumnType storageType(ColumnTypeDefinition type) {
        return new StorageColumnType(StorageColumnTypeId.valueOf(type.typeId().name()), type.nullable(),
                type.length(), type.scale(), type.unsigned(), type.charsetId(), type.collationId(), type.symbols());
    }

    /** 把单个 DD index 映射为 storage DDL DTO；columnId/order/prefix 均保持稳定语义。 */
    private static StorageIndexDefinition storageIndex(IndexDefinition index) {
        return new StorageIndexDefinition(index.id().value(), index.name().displayName(),
                index.unique(), index.clustered(), index.keyParts().stream()
                .map(part -> new StorageIndexKeyPart(
                        part.columnId(), part.order() == cn.zhangyis.db.dd.domain.IndexOrder.ASC
                                ? StorageIndexOrder.ASC : StorageIndexOrder.DESC,
                        part.prefixBytes())).toList());
    }

    /**
     * 为 backfill 组装“已有全部索引 + 新索引”的物理 schema。initial size 对已打开表空间不参与扩展，
     * 使用最小正值仅满足稳定 DTO 不变量。
     */
    private static StorageTableDefinition storageDefinition(TableDefinition table, IndexDefinition newIndex) {
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryDdlException("CREATE INDEX table has no physical binding: " + table.id().value()));
        List<StorageColumnDefinition> columns = table.columns().stream().map(column ->
                new StorageColumnDefinition(column.columnId(), column.name().displayName(), column.ordinal(),
                        storageType(column.type()))).toList();
        List<StorageIndexDefinition> indexes = new ArrayList<>();
        table.indexes().stream().map(DictionaryDdlService::storageIndex).forEach(indexes::add);
        indexes.add(storageIndex(newIndex));
        return new StorageTableDefinition(
                table.id().value(), binding.spaceId(), binding.path(), binding.rowFormatVersion(),
                cn.zhangyis.db.domain.PageNo.of(1), columns, indexes);
    }

    private Path tablePath(TableId tableId, int spaceId) {
        return tablesDirectory.resolve("table_" + tableId.value() + "_space_" + spaceId + ".ibd");
    }

    private void ensureTablesDirectory() {
        try {
            Files.createDirectories(tablesDirectory);
        } catch (IOException e) {
            throw new DictionaryDdlException("create tables directory failed: " + tablesDirectory, e);
        }
    }

    private static void validateOwnerTimeout(MdlOwnerId owner, Duration timeout) {
        if (owner == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("DDL owner/positive timeout required");
        }
        if (owner.sessionOwner()) {
            throw new DatabaseValidationException("public DDL cannot reuse a reserved Session MDL owner");
        }
    }

    /** 限制 DISCARD/IMPORT 外部路径在实例 transfer 根内，防止 DDL marker 携带任意文件路径。 */
    private Path checkedTransferPath(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("tablespace transfer path must not be null");
        }
        Path normalized = path.toAbsolutePath().normalize();
        Path transferRoot = transferRoot();
        if (!normalized.startsWith(transferRoot) || hasSymbolicLinkComponent(normalized)) {
            throw new DatabaseValidationException(
                    "tablespace transfer path escapes controlled root or contains a symbolic link: " + normalized);
        }
        return normalized;
    }

    /** 逐级拒绝既有符号链接，避免合法字符串前缀在文件系统解析时指向实例根之外。 */
    private static boolean hasSymbolicLinkComponent(Path path) {
        Path current = path.getRoot();
        for (Path component : path) {
            current = current == null ? component : current.resolve(component);
            if (Files.exists(current, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                    && Files.isSymbolicLink(current)) {
                return true;
            }
        }
        return false;
    }

    /** 根据恢复可见 catalog 解析 table/space identity；调用方必须在后续 table X 内重新校验。 */
    private TransferTarget transferTarget(QualifiedTableName name) {
        if (name == null) {
            throw new DatabaseValidationException("tablespace transfer table name must not be null");
        }
        SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                new DictionaryObjectNotFoundException(
                        "schema does not exist: " + name.schema().displayName()));
        TableDefinition table = repository.findTableForRecovery(schema.id(), name.table()).orElseThrow(() ->
                new DictionaryObjectNotFoundException("table does not exist: " + name.canonicalKey()));
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryDdlException(
                        "tablespace transfer table has no physical binding: " + table.id().value()));
        return new TransferTarget(table.id(), table.state(), binding);
    }

    /** 形成携带 table/space identity 的固定文件名，避免管理员误把另一张表文件放入 incoming。 */
    private Path transferPath(String bucket, TableId tableId, TableStorageBinding binding) {
        return transferRoot().resolve(bucket).resolve(
                "table_" + tableId.value() + "_space_" + binding.spaceId().value() + ".ibd");
    }

    /** transfer 根与 tables 目录同属实例目录，但不允许 canonical 在线文件与交换文件相互覆盖。 */
    private Path transferRoot() {
        Path instanceRoot = tablesDirectory.getParent();
        if (instanceRoot == null) {
            throw new DatabaseValidationException(
                    "tables directory has no instance parent: " + tablesDirectory);
        }
        return instanceRoot.resolve("tablespace-transfer").toAbsolutePath().normalize();
    }

    /** 创建固定 transfer bucket；IO 失败必须阻止 DDL marker 引用尚不可用的目录。 */
    private static void ensureTransferDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
        } catch (IOException failure) {
            throw new DictionaryDdlException(
                    "create tablespace transfer directory failed: " + directory, failure);
        }
    }

    /** 路径预计算使用的不可变 identity；不是锁内权威状态。 */
    private record TransferTarget(TableId tableId, TableState state, TableStorageBinding binding) {
    }
}
