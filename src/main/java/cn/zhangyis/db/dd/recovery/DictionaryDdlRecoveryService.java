package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.ddl.DdlControlCasResult;
import cn.zhangyis.db.dd.ddl.DdlControlState;
import cn.zhangyis.db.dd.ddl.DdlBatchManifest;
import cn.zhangyis.db.dd.ddl.DdlBatchSchemaEntry;
import cn.zhangyis.db.dd.ddl.DdlBatchTableEntry;
import cn.zhangyis.db.dd.ddl.DdlExecutionProtocol;
import cn.zhangyis.db.dd.ddl.DdlSchemaDigest;
import cn.zhangyis.db.dd.ddl.DdlSchemaDigestService;
import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.ddl.OnlineIndexBuildManifest;
import cn.zhangyis.db.dd.ddl.OnlineIndexBuildManifestCodec;
import cn.zhangyis.db.dd.ddl.OnlineDdlOperationIdentity;
import cn.zhangyis.db.dd.ddl.OnlineDdlOperationRegistry;
import cn.zhangyis.db.dd.ddl.OnlineDdlOperationTracker;
import cn.zhangyis.db.dd.ddl.OnlineDdlRuntimePhase;
import cn.zhangyis.db.dd.ddl.OnlineDdlTerminalResult;
import cn.zhangyis.db.dd.ddl.OnlineDdlWaitReason;
import cn.zhangyis.db.dd.ddl.IndexRetirementBarrier;
import cn.zhangyis.db.dd.ddl.OnlineAlterManifest;
import cn.zhangyis.db.dd.ddl.OnlineAlterManifestCodec;
import cn.zhangyis.db.dd.ddl.OnlineAlterRetirementBarrier;
import cn.zhangyis.db.dd.ddl.OnlineAlterActionType;
import cn.zhangyis.db.dd.ddl.DdlRetirementFence;
import cn.zhangyis.db.dd.ddl.DdlRetiredResource;
import cn.zhangyis.db.dd.ddl.DdlRetiredResourceKind;
import cn.zhangyis.db.dd.domain.DdlId;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaState;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.repo.DictionaryControlStore;
import cn.zhangyis.db.dd.repo.DictionaryIdAllocation;
import cn.zhangyis.db.dd.repo.DictionaryIdRequest;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.sdi.SerializedDictionaryInfoService;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.storage.api.ddl.TableDdlStorageService;
import cn.zhangyis.db.storage.api.TablePurgeBarrier;
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexBuildDescriptor;
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexDropDescriptor;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import cn.zhangyis.db.storage.api.ddl.OnlineIndexScanBatch;
import cn.zhangyis.db.storage.api.ddl.OnlineAlterDescriptorSet;
import cn.zhangyis.db.storage.api.ddl.OnlineAlterIndexDescriptor;
import cn.zhangyis.db.storage.api.ddl.OnlineAlterIndexDescriptorAction;
import cn.zhangyis.db.storage.api.ddl.StorageColumnDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageColumnType;
import cn.zhangyis.db.storage.api.ddl.StorageColumnTypeId;
import cn.zhangyis.db.storage.api.ddl.StorageIndexDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageIndexKeyPart;
import cn.zhangyis.db.storage.api.ddl.StorageIndexOrder;
import cn.zhangyis.db.storage.api.ddl.StorageTableDefinition;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecord;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId;
import cn.zhangyis.db.storage.fil.online.FileOnlineIndexChangeLog;
import cn.zhangyis.db.storage.fil.online.FileOnlineAlterChangeLog;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.dd.recovery.backup.RecoveryBackupService;
import cn.zhangyis.db.dd.recovery.backup.ValidatedRecoveryBackup;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.Arrays;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * StorageEngine crash recovery 成功、开放上层流量前执行的 DDL 收敛阶段。它先用独立 DDL log 裁决
 * CREATE/DROP，再兼容续作没有 marker 的旧 DROP_PENDING，最后删除 DROPPED 残留与受控 orphan；
 * 不执行 SQL，也不猜测 ACTIVE 缺失文件。
 */
@Slf4j
public final class DictionaryDdlRecoveryService {

    /** 只为恢复发布 DROPPED 所需新字典版本提供 durable 单调分配。 */
    private final DictionaryControlStore control;
    /** committed DD snapshot 与共享物理 catalog 的 DDL marker history owner。 */
    private final PersistentDictionaryRepository repository;
    /** CREATE finish 发布和 DROP finish 永久失效 metadata 的进程内 cache。 */
    private final DictionaryObjectCache cache;
    /** 只用于有 durable binding 的 DROP_PENDING 幂等物理删除。 */
    private final TableDdlStorageService physical;
    /** DROP_PENDING 物理删除前再次校验的恢复期 history barrier。 */
    private final TablePurgeBarrier purgeBarrier;
    /** 复用 DD discovery 的受控路径边界校验，不在恢复器中放宽删除范围。 */
    private final DictionaryTablespaceDiscovery discovery;
    /** 当前实例唯一允许 DDL recovery 扫描/删除 file-per-table 文件的规范目录。 */
    private final Path tablesDirectory;
    /** 以 committed ACTIVE DD 校验/修复 page3，不接受 SDI 反向发布 catalog。 */
    private final SerializedDictionaryInfoService sdi;
    /** op=10 恢复前重验 HMAC/hash/page0 的可信备份协作者；identity IO 仍保持懒加载。 */
    private final RecoveryBackupService recoveryBackups;
    /** 非 null 时识别 CREATE_INDEX auxiliary row-log 并同步恢复；null 保持 legacy marker 行为。 */
    private final OnlineIndexRecoveryRuntime onlineIndexRuntime;
    /** 与组合根 live/control 共享的恢复诊断 registry；不参与恢复裁决。 */
    private final OnlineDdlOperationRegistry onlineDdlRegistry;
    /** 非null时恢复ONLINE_DROP_INDEX_V1的persistent history/source pin退休边界。 */
    private final IndexRetirementBarrier indexRetirementBarrier;
    /** 非null时解释通用 INPLACE/SHADOW journal；与通用 retirement barrier 成对注入。 */
    private final OnlineAlterRecoveryRuntime onlineAlterRuntime;
    /** 通用 ALTER 多资源退休屏障；恢复只使用 marker 内已冻结 fence。 */
    private final OnlineAlterRetirementBarrier onlineAlterRetirementBarrier;
    /** 与live coordinator共用TABLE_SCHEMA_V1语法的无状态checkpoint计算器。 */
    private final DdlSchemaDigestService schemaDigests = new DdlSchemaDigestService();

    /**
     * 构造不接 persistent history barrier 的低层恢复服务；只适用于没有真实 committed history 的组件测试。
     *
     * @param control         字典 id/version 的 durable 单调分配器。
     * @param repository      committed catalog 仓储。
     * @param cache           table metadata cache/invalidation 入口。
     * @param physical        物理 tablespace DROP facade。
     * @param tablesDirectory 受控表空间文件目录。
     * @throws DatabaseValidationException 任一依赖或目录为空时抛出。
     */
    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                        PersistentDictionaryRepository repository,
                                        DictionaryObjectCache cache,
                                        TableDdlStorageService physical,
                                        Path tablesDirectory) {
        this(control, repository, cache, physical, tablesDirectory, TablePurgeBarrier.NONE);
    }

    /**
     * 构造生产 DDL 恢复服务；StorageEngine 完成 history rebuild/RESUME_PURGE 后，每个 pending 物理删除前仍复核 barrier。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param control         字典 id/version 的 durable 单调分配器。
     * @param repository      committed catalog 仓储与 recovery snapshot 来源。
     * @param cache           table metadata cache/invalidation 入口。
     * @param physical        物理 tablespace DROP facade。
     * @param tablesDirectory 受控 tablespace 文件目录，构造时转为绝对规范路径。
     * @param purgeBarrier    与 StorageEngine history 投影共享 owner 的表级等待 API。
     * @throws DatabaseValidationException 任一依赖、目录或 barrier 为空时抛出。
     */
    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                        PersistentDictionaryRepository repository,
                                        DictionaryObjectCache cache,
                                        TableDdlStorageService physical,
                                        Path tablesDirectory,
                                        TablePurgeBarrier purgeBarrier) {
        this(control, repository, cache, physical, tablesDirectory, purgeBarrier, null);
    }

    /** 构造生产恢复服务，并注入 Online ADD INDEX row-log/scan runtime。 */
    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                        PersistentDictionaryRepository repository,
                                        DictionaryObjectCache cache,
                                        TableDdlStorageService physical,
                                        Path tablesDirectory,
                                        TablePurgeBarrier purgeBarrier,
                                        OnlineIndexRecoveryRuntime onlineIndexRuntime) {
        this(control, repository, cache, physical, tablesDirectory, purgeBarrier,
                onlineIndexRuntime, new OnlineDdlOperationRegistry(256));
    }

    /**
     * 构造共享 Online DDL registry 的生产恢复器；旧构造器仍使用隔离的有界 registry。
     *
     * @param control durable id/version allocator
     * @param repository committed DD 与 DDL marker repository
     * @param cache recovery cache publisher
     * @param physical recovery-safe storage DDL facade
     * @param tablesDirectory 受控 tablespace 目录
     * @param purgeBarrier persistent history barrier
     * @param onlineIndexRuntime Online ADD INDEX 恢复 runtime；无 online marker 的低层测试允许为空
     * @param onlineDdlRegistry live/recovery/control 共享的轻量 registry
     */
    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                        PersistentDictionaryRepository repository,
                                        DictionaryObjectCache cache,
                                        TableDdlStorageService physical,
                                        Path tablesDirectory,
                                        TablePurgeBarrier purgeBarrier,
                                         OnlineIndexRecoveryRuntime onlineIndexRuntime,
                                         OnlineDdlOperationRegistry onlineDdlRegistry) {
        this(control, repository, cache, physical, tablesDirectory, purgeBarrier,
                onlineIndexRuntime, onlineDdlRegistry, null);
    }

    /**
     * 构造支持Online ADD与Online DROP恢复的生产服务；旧构造继续只解释blocking DROP marker。
     *
     * @param control durable id/version allocator
     * @param repository committed DD与marker repository
     * @param cache recovery metadata cache
     * @param physical recovery-safe物理DDL facade
     * @param tablesDirectory 受控tablespace目录
     * @param purgeBarrier table DROP使用的persistent history barrier
     * @param onlineIndexRuntime Online ADD row-log恢复runtime
     * @param onlineDdlRegistry live/recovery/control共享诊断registry
     * @param indexRetirementBarrier Online DROP history/source pin退休屏障；无online DROP的低层测试允许为空
     */
    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                         PersistentDictionaryRepository repository,
                                         DictionaryObjectCache cache,
                                         TableDdlStorageService physical,
                                         Path tablesDirectory,
                                         TablePurgeBarrier purgeBarrier,
                                         OnlineIndexRecoveryRuntime onlineIndexRuntime,
                                         OnlineDdlOperationRegistry onlineDdlRegistry,
                                         IndexRetirementBarrier indexRetirementBarrier) {
        this(control, repository, cache, physical, tablesDirectory, purgeBarrier,
                onlineIndexRuntime, onlineDdlRegistry, indexRetirementBarrier,
                null, null);
    }

    /**
     * 构造同时支持单索引与通用 Online ALTER 的生产恢复器。
     *
     * @param control durable id/version allocator
     * @param repository committed DD 与 marker repository
     * @param cache recovery metadata cache
     * @param physical recovery-safe storage DDL facade
     * @param tablesDirectory 受控 tablespace 目录
     * @param purgeBarrier table DROP persistent history barrier
     * @param onlineIndexRuntime 单 ADD INDEX journal runtime；兼容测试允许为空
     * @param onlineDdlRegistry live/recovery/control 共享诊断 registry
     * @param indexRetirementBarrier 单 DROP INDEX retirement barrier；兼容测试允许为空
     * @param onlineAlterRuntime 通用 ALTER journal runtime；未启用通用协议时允许为空
     * @param onlineAlterRetirementBarrier 通用 INDEX/TABLESPACE retirement barrier；必须与 runtime 成对出现
     */
    public DictionaryDdlRecoveryService(DictionaryControlStore control,
                                         PersistentDictionaryRepository repository,
                                         DictionaryObjectCache cache,
                                         TableDdlStorageService physical,
                                         Path tablesDirectory,
                                         TablePurgeBarrier purgeBarrier,
                                         OnlineIndexRecoveryRuntime onlineIndexRuntime,
                                         OnlineDdlOperationRegistry onlineDdlRegistry,
                                         IndexRetirementBarrier indexRetirementBarrier,
                                         OnlineAlterRecoveryRuntime onlineAlterRuntime,
                                         OnlineAlterRetirementBarrier onlineAlterRetirementBarrier) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (control == null || repository == null || cache == null || physical == null || tablesDirectory == null
                || purgeBarrier == null || onlineDdlRegistry == null
                || (onlineAlterRuntime == null) != (onlineAlterRetirementBarrier == null)) {
            throw new DatabaseValidationException("dictionary DDL recovery collaborators/path must not be null");
        }
        this.control = control;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.repository = repository;
        this.cache = cache;
        this.physical = physical;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.purgeBarrier = purgeBarrier;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
        this.discovery = new DictionaryTablespaceDiscovery(repository, tablesDirectory);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.sdi = new SerializedDictionaryInfoService(physical);
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
    }

    /**
     * 按 DDL id 顺序收敛 marker，再处理旧版 pending 并清理已决和孤儿文件；任一失败阻止 DatabaseEngine OPEN。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 timeout；此时不冻结字典快照，避免 marker 恢复发布的新版本被后续步骤忽略。</li>
     *     <li>按 ddl id 消费非终态 marker，以 committed DD 决定 CREATE rollback/finish 与 DROP rollback/finish。</li>
     *     <li>重新读取 snapshot，逐表续作没有 marker 的旧版 DROP_PENDING。</li>
     *     <li>重新读取 ACTIVE snapshot，以 committed DD 校验并修复每张表的 SDI。</li>
     *     <li>全部状态与 SDI 收敛后清理 DROPPED residue 与不被 committed binding 引用的受控 orphan。</li>
     * </ol>
     *
     * @param timeout 每张 DROP_PENDING 表等待 purge barrier 与物理 DROP 的正有界时间。
     * @throws DatabaseValidationException timeout 为空、零或负数时抛出，且不修改文件/catalog。
     * @throws DictionaryRecoveryException pending metadata/binding、barrier、物理 DROP、catalog publish 或 orphan 扫描失败时抛出；
     *                                     DatabaseEngine 必须保持上层流量关闭。
     */
    public void recover(Duration timeout) {
        // 1、timeout 非正时不读取 marker、更不进入任何 catalog/file 修改。
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("dictionary recovery timeout must be positive");
        }

        // 2、DDL log 是新格式恢复的首要证据；每次 transition durable 后 repository 快照立即前进。
        for (DdlLogRecord record : repository.ddlLog().unresolved()) {
            OnlineDdlOperationTracker tracker = registerRecoveryTracker(record);
            try {
                recoverLoggedOperation(record, timeout);
                completeRecoveryTracker(record, tracker);
            } catch (RuntimeException failure) {
                if (tracker != null) {
                    onlineDdlRegistry.complete(
                            DdlId.of(record.marker().ddlOperationId()),
                            OnlineDdlTerminalResult.FAILED_CLOSED,
                            Optional.of("DDL_RECOVERY_FAILED"), true);
                }
                throw failure;
            }
        }

        // Online manifest 可能早于 marker 落盘，terminal 日志删除也可能在上次启动失败；只按 exact build owner 清理。
        cleanupOnlineIndexLogs();

        // 3、marker 恢复可能已把 DROP_PENDING 发布成 DROPPED，必须重新读取最新 snapshot。
        List<TableDefinition> pending = repository.snapshot().tables().values().stream()
                .filter(table -> table.state() == TableState.DROP_PENDING)
                .toList();
        for (TableDefinition table : pending) {
            completePendingDrop(table, timeout, Optional.empty());
        }

        // 4、pending 可能发布新版本；只对最终 ACTIVE 集合做 SDI 校验，DROP 生命周期不写即将删除的 page3。
        List<TableDefinition> active = repository.snapshot().tables().values().stream()
                .filter(table -> table.state() == TableState.ACTIVE)
                .toList();
        for (TableDefinition table : active) {
            try {
                sdi.reconcile(table, timeout);
            } catch (RuntimeException failure) {
                throw new DictionaryRecoveryException("reconcile SDI failed: table=" + table.id().value(), failure);
            }
        }

        // 5、只有 marker、legacy pending 和 ACTIVE SDI 全部收敛后才扩大到目录清理。
        cleanupDroppedAndOrphans();
    }

    /**
     * 把 cancel-capable durable marker 注册为 recovery tracker，并以当前 committed DD 版本选择 source/target 诊断阶段。
     *
     * @param record 启动扫描得到的非终态 marker
     * @return 需要跟踪的 tracker；blocking/legacy protocol 返回 {@code null}
     */
    private OnlineDdlOperationTracker registerRecoveryTracker(DdlLogRecord record) {
        if (!record.executionProtocol().cancelCapable()) {
            return null;
        }
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        Optional<TableDefinition> table = repository.findTableForRecovery(
                TableId.of(record.marker().affectedObjectId()));
        String tableName = table.map(value -> value.name().canonicalName()).orElse("");
        String indexName = table.flatMap(value -> value.indexes().stream()
                        .filter(index -> index.id().value() == record.secondaryObjectId())
                        .findFirst())
                .map(index -> index.name().canonicalName()).orElse("");
        long sourceVersion = table.map(value -> value.version().value()).orElse(0L);
        boolean indexOperation = record.operation() == DdlLogOperation.CREATE_INDEX
                || record.operation() == DdlLogOperation.DROP_INDEX;
        OnlineDdlOperationTracker tracker = onlineDdlRegistry.register(
                new OnlineDdlOperationIdentity(
                        ddlId, record.operation(), record.marker().affectedObjectId(),
                        indexOperation ? record.secondaryObjectId() : 0L,
                        tableName, indexOperation ? indexName : "", sourceVersion,
                        record.marker().dictionaryVersion(), 0, true, OptionalLong.empty()),
                record.executionProtocol());
        tracker.observeDurable(record);
        boolean target = table.isPresent()
                && table.orElseThrow().version().value() == record.marker().dictionaryVersion();
        tracker.advanceRuntime(target
                        ? OnlineDdlRuntimePhase.RECOVERING_TARGET
                        : OnlineDdlRuntimePhase.RECOVERING_SOURCE,
                OnlineDdlWaitReason.NONE);
        return tracker;
    }

    /**
     * operation 恢复返回后重读 terminal marker，先刷新 durable 投影再移入有界 history。
     *
     * @param initial 本轮恢复开始时的 marker identity
     * @param tracker cancel-capable operation tracker；blocking/legacy 为 {@code null}
     * @throws DictionaryRecoveryException 恢复方法返回但 marker 仍非终态时抛出并阻止 OPEN
     */
    private void completeRecoveryTracker(
            DdlLogRecord initial, OnlineDdlOperationTracker tracker) {
        if (tracker == null) {
            return;
        }
        DdlId ddlId = DdlId.of(initial.marker().ddlOperationId());
        DdlLogRecord terminal = repository.ddlLog().find(ddlId).orElseThrow(() ->
                new DictionaryRecoveryException(
                        "Online DDL recovery marker disappeared: ddl=" + ddlId.value()));
        if (!terminal.phase().terminal()) {
            throw new DictionaryRecoveryException(
                    "Online DDL recovery returned before terminal marker: ddl="
                            + ddlId.value() + " phase=" + terminal.phase());
        }
        tracker.observeDurable(terminal);
        OnlineDdlTerminalResult result = terminal.phase() == DdlLogPhase.COMMITTED
                ? OnlineDdlTerminalResult.COMPLETED
                : OnlineDdlTerminalResult.ROLLED_BACK;
        onlineDdlRegistry.complete(ddlId, result, Optional.empty(), false);
    }

    /**
     * 恢复通用 INPLACE_INDEX 或 SHADOW_REBUILD_V1。两种策略共享 journal/control/digest 裁决，物理差异只在
     * descriptor cleanup 与 shadow space swap。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>打开 operation-owned journal，交叉验证 header、manifest、marker、协议、版本、路径与 schema digest。</li>
     *     <li>把 committed DD 精确分类为 source 或 target；source 且未 FORWARD_ONLY 时只允许回滚物理 owner。</li>
     *     <li>FORWARD_ONLY 必须同时具有 force 覆盖的 READY/RECONCILED；source 从 target SDI 重建完整 aggregate并发布。</li>
     *     <li>target 真相建立后验证 retirement fence，等待 history/pin 安全，再回收 descriptor DROP 或旧 tablespace。</li>
     *     <li>推进 DICTIONARY_COMMITTED/COMMITTED，发布 cache；journal 仅在 terminal marker 后关闭并 exact 删除。</li>
     * </ol>
     *
     * @param record 通用 Online ALTER 的非终态 durable marker
     * @param timeout SDI、物理 cleanup 与 retirement barrier 共用的正等待上界
     * @param shadow true 表示表级 shadow swap，false 表示同空间 multi-index descriptor
     * @throws DictionaryRecoveryException 任一证据缺失、错绑或 phase/control 出现第三态时抛出并阻止 OPEN
     */
    private void recoverGeneralOnlineAlter(
            DdlLogRecord record, Duration timeout, boolean shadow) {
        // 1、journal是manifest唯一owner；marker路径只用于exact工厂校验，绝不直接交给FileChannel。
        if (onlineAlterRuntime == null || onlineAlterRetirementBarrier == null) {
            throw new DictionaryRecoveryException(
                    "general Online ALTER recovery runtime is not configured: ddl="
                            + record.marker().ddlOperationId());
        }
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        OnlineDdlCaptureId captureId = OnlineDdlCaptureId.of(ddlId.value());
        Path journalPath = record.auxiliaryPath().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "general Online ALTER marker has no journal: ddl=" + ddlId.value()));
        boolean terminal = false;
        try (FileOnlineAlterChangeLog changeLog =
                     onlineAlterRuntime.logFiles().open(captureId, journalPath)) {
            OnlineAlterManifest manifest = validateOnlineAlterEvidence(
                    record, changeLog, shadow);
            TableId tableId = manifest.tableId();
            TableDefinition current = repository.findTableForRecovery(tableId).orElseThrow(() ->
                    new DictionaryRecoveryException(
                            "general Online ALTER table is absent: " + tableId.value()));
            boolean source = current.version().equals(manifest.sourceVersion());
            boolean target = current.version().equals(manifest.targetVersion());
            if (current.state() != TableState.ACTIVE || source == target) {
                throw new DictionaryRecoveryException(
                        "general Online ALTER DD is neither exact source nor target: table="
                                + tableId.value() + " version=" + current.version().value());
            }
            Path sourcePath = checkedMarkerPath(record);
            TableStorageBinding currentBinding = current.storageBinding().orElseThrow(() ->
                    new DictionaryRecoveryException(
                            "general Online ALTER DD has no binding: table=" + tableId.value()));

            // 2、未跨forward fence时committed source是唯一真相；即使READY已落盘也必须反向清理。
            if (source && record.controlState() != DdlControlState.FORWARD_ONLY) {
                if (record.phase() != DdlLogPhase.PREPARED
                        || record.controlState() == DdlControlState.OPEN
                        && record.cancellation().isPresent()) {
                    throw new DictionaryRecoveryException(
                            "general Online ALTER rollback phase/control is invalid: ddl="
                                    + ddlId.value());
                }
                requireSchemaCheckpoint(record, current, SchemaCheckpoint.SOURCE);
                requireExactSourceBinding(record, currentBinding, sourcePath);
                if (shadow) {
                    OnlineAlterShadowIdentity shadowIdentity = shadowIdentity(manifest);
                    physical.deleteUnopenedOnlineAlterTablespace(
                            tableId.value(), shadowIdentity.spaceId(),
                            shadowIdentity.path(), timeout);
                } else {
                    Optional<OnlineAlterDescriptorSet> descriptor =
                            readGeneralAlterDescriptors(currentBinding, record, manifest);
                    if (descriptor.isPresent()) {
                        physical.rollbackOnlineAlterIndexDescriptors(
                                currentBinding, descriptor.orElseThrow(), timeout);
                    }
                    sdi.reconcile(current, timeout);
                }
                repository.ddlLog().transition(
                        ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
                cache.restoreTableAfterDdlRollback(current, manifest.targetVersion());
                terminal = true;
            } else {
                // 3、方向一旦持久前滚，READY/RECONCILED force与target SDI缺一不可，恢复不猜测重做base work。
                if (record.controlState() != DdlControlState.FORWARD_ONLY) {
                    throw new DictionaryRecoveryException(
                            "general Online ALTER target lacks FORWARD_ONLY: ddl=" + ddlId.value());
                }
                requireReconciledJournal(changeLog, manifest, shadow);
                OnlineAlterDescriptorSet descriptors = null;
                TableStorageBinding sourceBinding = source ? currentBinding : null;
                if (!shadow) {
                    descriptors = readGeneralAlterDescriptors(
                            currentBinding, record, manifest).orElseThrow(() ->
                            new DictionaryRecoveryException(
                                    "general INPLACE ALTER descriptor is absent after FORWARD_ONLY: ddl="
                                            + ddlId.value()));
                }
                if (source) {
                    requireSchemaCheckpoint(record, current, SchemaCheckpoint.SOURCE);
                    requireExactSourceBinding(record, currentBinding, sourcePath);
                    TableDefinition recoveredTarget;
                    if (shadow) {
                        OnlineAlterShadowIdentity shadowIdentity = shadowIdentity(manifest);
                        physical.mountOnlineAlterShadowForRecovery(
                                tableId.value(), shadowIdentity.spaceId(), shadowIdentity.path());
                        recoveredTarget = sdi.readUnpublished(
                                tableId.value(), shadowIdentity.spaceId(), shadowIdentity.path())
                                .orElseThrow(() -> new DictionaryRecoveryException(
                                        "online shadow target SDI is absent: ddl=" + ddlId.value()));
                    } else {
                        recoveredTarget = sdi.readUnpublished(
                                tableId.value(), currentBinding.spaceId(), currentBinding.path())
                                .orElseThrow(() -> new DictionaryRecoveryException(
                                        "general INPLACE target SDI is absent: ddl=" + ddlId.value()));
                        validateInplaceTargetBinding(
                                currentBinding, recoveredTarget, descriptors);
                    }
                    requireOnlineAlterTarget(record, manifest, recoveredTarget);
                    if (record.phase() == DdlLogPhase.PREPARED) {
                        repository.ddlLog().transition(
                                ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
                    } else if (record.phase() != DdlLogPhase.ENGINE_DONE) {
                        throw new DictionaryRecoveryException(
                                "general Online ALTER source forward phase is invalid: ddl="
                                        + ddlId.value() + " phase=" + record.phase());
                    }
                    cache.invalidateTable(tableId, manifest.targetVersion());
                    commitRecoveredTable(recoveredTarget);
                    repository.ddlLog().transition(
                            ddlId, DdlLogPhase.ENGINE_DONE,
                            DdlLogPhase.DICTIONARY_COMMITTED);
                    current = recoveredTarget;
                    currentBinding = recoveredTarget.storageBinding().orElseThrow();
                } else {
                    requireOnlineAlterTarget(record, manifest, current);
                    if (!shadow) {
                        validatePublishedInplaceTarget(current, descriptors);
                    } else {
                        OnlineAlterShadowIdentity identity = shadowIdentity(manifest);
                        if (!currentBinding.spaceId().equals(identity.spaceId())
                                || !currentBinding.path().equals(identity.path())) {
                            throw new DictionaryRecoveryException(
                                    "online shadow target DD binding differs from manifest: ddl="
                                            + ddlId.value());
                        }
                    }
                    DdlLogPhase phase = record.phase();
                    if (phase == DdlLogPhase.ENGINE_DONE) {
                        repository.ddlLog().transition(
                                ddlId, DdlLogPhase.ENGINE_DONE,
                                DdlLogPhase.DICTIONARY_COMMITTED);
                    } else if (phase != DdlLogPhase.DICTIONARY_COMMITTED) {
                        throw new DictionaryRecoveryException(
                                "general Online ALTER target phase is invalid: ddl="
                                        + ddlId.value() + " phase=" + phase);
                    }
                    sdi.reconcile(current, timeout);
                }

                // 4、fence与实际被删除资源必须逐值一致；等待失败保留marker/journal/descriptor供下次前滚。
                DdlRetirementFence fence = validateGeneralRetirementFence(
                        record, manifest, descriptors, shadow);
                if (fence != null) {
                    onlineAlterRetirementBarrier.awaitSafe(fence, timeout);
                }
                cache.publishTable(current);
                if (shadow) {
                    if (sourceBinding != null) {
                        physical.dropTable(sourceBinding, timeout);
                    } else {
                        physical.deleteUnopenedOnlineAlterTablespace(
                                tableId.value(), record.spaceId(), sourcePath, timeout);
                    }
                } else {
                    physical.finishOnlineAlterIndexDescriptors(
                            currentBinding, descriptors, timeout);
                }
                DdlLogRecord latest = repository.ddlLog().find(ddlId).orElseThrow();
                if (latest.phase() != DdlLogPhase.DICTIONARY_COMMITTED) {
                    throw new DictionaryRecoveryException(
                            "general Online ALTER cleanup reached unexpected phase: ddl="
                                    + ddlId.value() + " phase=" + latest.phase());
                }
                repository.ddlLog().transition(
                        ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
                terminal = true;
            }
        } catch (DictionaryRecoveryException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "recover general Online ALTER failed: ddl=" + ddlId.value(), failure);
        }
        // 5、Windows下必须先关闭独占FileChannel再删除；terminal=false时完整保留现场。
        if (terminal) {
            onlineAlterRuntime.logFiles().delete(captureId, journalPath);
        }
    }

    /** 校验journal header/manifest/marker三方不可变identity，并返回已解码manifest。 */
    private OnlineAlterManifest validateOnlineAlterEvidence(
            DdlLogRecord record, FileOnlineAlterChangeLog changeLog, boolean shadow) {
        OnlineAlterLogHeader header = changeLog.header();
        OnlineAlterManifest manifest;
        try {
            manifest = new OnlineAlterManifestCodec().decode(header.manifest());
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "decode general Online ALTER manifest failed: ddl="
                            + record.marker().ddlOperationId(), failure);
        }
        DdlExecutionProtocol expectedProtocol = shadow
                ? DdlExecutionProtocol.ONLINE_ALTER_SHADOW_V1
                : DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1;
        boolean markerShape = record.executionProtocol() == expectedProtocol
                && record.fileIdentity().isEmpty()
                && record.sourceSchemaDigest().isPresent()
                && record.targetSchemaDigest().isPresent()
                && (shadow ? record.operation() == DdlLogOperation.REBUILD_TABLE
                && record.secondaryObjectId() > 0
                : record.operation() == DdlLogOperation.ALTER_TABLE_INPLACE
                && record.secondaryObjectId() == 0);
        boolean identities = header.captureId().value() == record.marker().ddlOperationId()
                && header.tableId() == record.marker().affectedObjectId()
                && header.targetDictionaryVersion() == record.marker().dictionaryVersion()
                && header.executionProtocolCode() == expectedProtocol.stableCode()
                && manifest.ddlOperationId() == record.marker().ddlOperationId()
                && manifest.tableId().value() == record.marker().affectedObjectId()
                && manifest.sourceVersion().value() == header.sourceDictionaryVersion()
                && manifest.targetVersion().value() == header.targetDictionaryVersion()
                && manifest.sourceRowFormatVersion() == header.sourceRowFormatVersion()
                && manifest.targetRowFormatVersion() == header.targetRowFormatVersion()
                && manifest.freezeReadViewGeneration() == header.freezeReadViewGeneration()
                && manifest.executionProtocol() == expectedProtocol
                && manifest.sourceSchemaDigest().equals(record.sourceSchemaDigest().orElse(null))
                && manifest.targetSchemaDigest().equals(record.targetSchemaDigest().orElse(null));
        if (!markerShape || !identities) {
            throw new DictionaryRecoveryException(
                    "general Online ALTER marker/header/manifest identity mismatch: ddl="
                            + record.marker().ddlOperationId());
        }
        if (shadow) {
            OnlineAlterShadowIdentity identity = shadowIdentity(manifest);
            if (header.shadowSpaceId() != identity.spaceId().value()
                    || record.secondaryObjectId() != identity.spaceId().value()) {
                throw new DictionaryRecoveryException(
                        "online shadow space identity mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
        } else if (header.shadowSpaceId() != 0 || manifest.shadowTarget().isPresent()) {
            throw new DictionaryRecoveryException(
                    "general INPLACE manifest unexpectedly contains a shadow target");
        }
        return manifest;
    }

    /** 要求READY与RECONCILED状态均被后续force watermark覆盖；shadow同时验证final ReadView generation。 */
    private static void requireReconciledJournal(
            FileOnlineAlterChangeLog changeLog,
            OnlineAlterManifest manifest, boolean shadow) {
        OnlineAlterLogRecord ready = null;
        OnlineAlterLogRecord reconciled = null;
        for (OnlineAlterLogRecord record : changeLog.readAll()) {
            if (record.type() == OnlineAlterLogRecordType.READY_TO_PUBLISH) {
                ready = record;
            } else if (record.type() == OnlineAlterLogRecordType.RECONCILED) {
                reconciled = record;
            }
        }
        if (changeLog.abortRequired() || ready == null || reconciled == null
                || reconciled.sequence() <= ready.sequence()
                || changeLog.highestForcedSequence() < reconciled.sequence()) {
            throw new DictionaryRecoveryException(
                    "general Online ALTER lacks forced READY/RECONCILED evidence: ddl="
                            + manifest.ddlOperationId());
        }
        byte[] payload = ready.payload();
        if (shadow) {
            if (payload.length != Long.BYTES
                    || ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).getLong()
                    < manifest.freezeReadViewGeneration()) {
                throw new DictionaryRecoveryException(
                        "online shadow READY ReadView generation is invalid: ddl="
                                + manifest.ddlOperationId());
            }
        } else if (payload.length != 0) {
            throw new DictionaryRecoveryException(
                    "general INPLACE READY payload must be empty: ddl="
                            + manifest.ddlOperationId());
        }
    }

    /** 读取并校验通用descriptor owner、manifest hash以及每个ADD/DROP action identity。 */
    private Optional<OnlineAlterDescriptorSet> readGeneralAlterDescriptors(
            TableStorageBinding binding, DdlLogRecord record,
            OnlineAlterManifest manifest) {
        Optional<OnlineAlterDescriptorSet> result;
        try {
            result = physical.readOnlineAlterDescriptorSet(binding);
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "read general Online ALTER descriptors failed: ddl="
                            + record.marker().ddlOperationId(), failure);
        }
        if (result.isEmpty()) {
            return result;
        }
        OnlineAlterDescriptorSet descriptors = result.orElseThrow();
        if (descriptors.ddlOperationId() != record.marker().ddlOperationId()
                || descriptors.targetDictionaryVersion()
                != record.marker().dictionaryVersion()
                || descriptors.tableId() != record.marker().affectedObjectId()
                || !MessageDigest.isEqual(
                descriptors.manifestDigest(), sha256(manifestBytes(manifest)))) {
            throw new DictionaryRecoveryException(
                    "general Online ALTER descriptor owner/manifest mismatch: ddl="
                            + record.marker().ddlOperationId());
        }
        Map<Integer, cn.zhangyis.db.dd.ddl.OnlineAlterActionDescriptor> actions =
                new LinkedHashMap<>();
        for (var action : manifest.actions()) {
            if (action.type() == OnlineAlterActionType.ADD_INDEX
                    || action.type() == OnlineAlterActionType.DROP_INDEX) {
                actions.put(action.ordinal(), action);
            }
        }
        if (actions.size() != descriptors.descriptors().size()) {
            throw new DictionaryRecoveryException(
                    "general Online ALTER descriptor/action count mismatch: ddl="
                            + record.marker().ddlOperationId());
        }
        for (OnlineAlterIndexDescriptor descriptor : descriptors.descriptors()) {
            var action = actions.get(descriptor.actionOrdinal());
            OnlineAlterActionType expected = descriptor.action()
                    == OnlineAlterIndexDescriptorAction.ADD
                    ? OnlineAlterActionType.ADD_INDEX : OnlineAlterActionType.DROP_INDEX;
            if (action == null || action.type() != expected
                    || action.primaryObjectId() != descriptor.indexBinding().indexId()) {
                throw new DictionaryRecoveryException(
                        "general Online ALTER descriptor/action identity mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
        }
        return result;
    }

    /** 校验INPLACE target binding恰好由source稳定索引、ADD descriptor与DROP descriptor组成。 */
    private static void validateInplaceTargetBinding(
            TableStorageBinding source, TableDefinition target,
            OnlineAlterDescriptorSet descriptors) {
        TableStorageBinding published = target.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "general INPLACE target has no storage binding"));
        if (published.tableId() != source.tableId()
                || !published.spaceId().equals(source.spaceId())
                || !published.path().equals(source.path())
                || published.rowFormatVersion() != source.rowFormatVersion()
                || !published.lobSegment().equals(source.lobSegment())) {
            throw new DictionaryRecoveryException(
                    "general INPLACE target changed table-level storage identity");
        }
        Map<Long, IndexStorageBinding> expected = new LinkedHashMap<>();
        for (IndexStorageBinding binding : source.indexes()) {
            expected.put(binding.indexId(), binding);
        }
        for (OnlineAlterIndexDescriptor descriptor : descriptors.descriptors()) {
            if (descriptor.action() == OnlineAlterIndexDescriptorAction.ADD) {
                expected.put(descriptor.indexBinding().indexId(), descriptor.indexBinding());
            } else if (!descriptor.indexBinding().equals(
                    expected.remove(descriptor.indexBinding().indexId()))) {
                throw new DictionaryRecoveryException(
                        "general INPLACE DROP descriptor does not match source binding");
            }
        }
        if (published.indexes().size() != target.indexes().size()) {
            throw new DictionaryRecoveryException(
                    "general INPLACE target logical/physical index count mismatch");
        }
        for (int ordinal = 0; ordinal < target.indexes().size(); ordinal++) {
            long indexId = target.indexes().get(ordinal).id().value();
            if (!published.indexes().get(ordinal).equals(expected.remove(indexId))) {
                throw new DictionaryRecoveryException(
                        "general INPLACE target index binding differs from descriptor truth");
            }
        }
        if (!expected.isEmpty()) {
            throw new DictionaryRecoveryException(
                    "general INPLACE target omitted or added an unowned index binding");
        }
    }

    /**
     * DD 已是 target 时只从 target aggregate 与持久 descriptor 验证发布结果；source binding 已不在 catalog，
     * DROP 的旧 root/segment 由 descriptor 自身保存，不能把 target 误当 source 重建差集。
     */
    private static void validatePublishedInplaceTarget(
            TableDefinition target, OnlineAlterDescriptorSet descriptors) {
        TableStorageBinding binding = target.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "published general INPLACE target has no binding"));
        if (binding.indexes().size() != target.indexes().size()) {
            throw new DictionaryRecoveryException(
                    "published general INPLACE logical/physical index count mismatch");
        }
        Map<Long, IndexStorageBinding> published = new LinkedHashMap<>();
        for (int ordinal = 0; ordinal < target.indexes().size(); ordinal++) {
            long indexId = target.indexes().get(ordinal).id().value();
            IndexStorageBinding indexBinding = binding.indexes().get(ordinal);
            if (indexBinding.indexId() != indexId) {
                throw new DictionaryRecoveryException(
                        "published general INPLACE index ordinal identity mismatch");
            }
            published.put(indexId, indexBinding);
        }
        for (OnlineAlterIndexDescriptor descriptor : descriptors.descriptors()) {
            IndexStorageBinding actual = published.get(
                    descriptor.indexBinding().indexId());
            if (descriptor.action() == OnlineAlterIndexDescriptorAction.ADD
                    && !descriptor.indexBinding().equals(actual)
                    || descriptor.action() == OnlineAlterIndexDescriptorAction.DROP
                    && actual != null) {
                throw new DictionaryRecoveryException(
                        "published general INPLACE target differs from ADD/DROP descriptor");
            }
        }
    }

    /** 校验最终target identity/version/state与marker target digest。 */
    private void requireOnlineAlterTarget(
            DdlLogRecord record, OnlineAlterManifest manifest,
            TableDefinition target) {
        if (!target.id().equals(manifest.tableId())
                || !target.version().equals(manifest.targetVersion())
                || target.state() != TableState.ACTIVE) {
            throw new DictionaryRecoveryException(
                    "general Online ALTER target identity/version/state mismatch: ddl="
                            + record.marker().ddlOperationId());
        }
        requireSchemaCheckpoint(record, target, SchemaCheckpoint.TARGET);
    }

    /** 验证marker retirement fence与实际DROP descriptor或旧space完全一致。 */
    private static DdlRetirementFence validateGeneralRetirementFence(
            DdlLogRecord record, OnlineAlterManifest manifest,
            OnlineAlterDescriptorSet descriptors, boolean shadow) {
        List<DdlRetiredResource> resources = shadow
                ? List.of(new DdlRetiredResource(
                DdlRetiredResourceKind.TABLESPACE, record.spaceId().value()))
                : descriptors.descriptors().stream()
                .filter(descriptor -> descriptor.action()
                        == OnlineAlterIndexDescriptorAction.DROP)
                .map(descriptor -> new DdlRetiredResource(
                        DdlRetiredResourceKind.INDEX,
                        descriptor.indexBinding().indexId()))
                .sorted().toList();
        if (resources.isEmpty()) {
            if (record.retirementFence().isPresent()) {
                throw new DictionaryRecoveryException(
                        "general Online ALTER has an unexpected retirement fence");
            }
            return null;
        }
        DdlRetirementFence fence = record.retirementFence().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "general Online ALTER retirement fence is absent"));
        long generation = shadow ? 1L : descriptors.generation();
        if (fence.tableId() != manifest.tableId().value()
                || fence.sourceDictionaryVersion() != manifest.sourceVersion().value()
                || fence.sourceMetadataPinVersion() != manifest.sourceVersion().value()
                || fence.descriptorGeneration() != generation
                || fence.ownerDdlId() != manifest.ddlOperationId()
                || !fence.resources().equals(resources)) {
            throw new DictionaryRecoveryException(
                    "general Online ALTER retirement fence identity/resources mismatch");
        }
        return fence;
    }

    /** marker source space/path必须仍等于committed source binding。 */
    private static void requireExactSourceBinding(
            DdlLogRecord record, TableStorageBinding binding, Path sourcePath) {
        if (!binding.spaceId().equals(record.spaceId())
                || !binding.path().equals(sourcePath)
                || binding.tableId() != record.marker().affectedObjectId()) {
            throw new DictionaryRecoveryException(
                    "general Online ALTER source binding differs from marker: ddl="
                            + record.marker().ddlOperationId());
        }
    }

    /** 从manifest取出并校验受控shadow exact identity。 */
    private OnlineAlterShadowIdentity shadowIdentity(OnlineAlterManifest manifest) {
        var target = manifest.shadowTarget().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "online shadow manifest has no target"));
        Path path = discovery.checkedPath(target.path());
        Path expected = tablesDirectory.resolve("table_" + manifest.tableId().value()
                + "_space_" + target.spaceId().value() + ".ibd")
                .toAbsolutePath().normalize();
        if (!path.equals(expected)) {
            throw new DictionaryRecoveryException(
                    "online shadow manifest path is not exact operation target: " + path);
        }
        return new OnlineAlterShadowIdentity(target.spaceId(), path);
    }

    /** deterministic重新编码用于校验descriptor保存的SHA-256 anchor。 */
    private static byte[] manifestBytes(OnlineAlterManifest manifest) {
        return new OnlineAlterManifestCodec().encode(manifest);
    }

    /** SHA-256是通用descriptor anchor的固定算法；JRE缺失视为恢复环境不可继续。 */
    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException failure) {
            throw new DictionaryRecoveryException(
                    "SHA-256 is unavailable for Online ALTER recovery", failure);
        }
    }

    /** 未发布shadow的marker级物理身份。 */
    private record OnlineAlterShadowIdentity(
            cn.zhangyis.db.domain.SpaceId spaceId, Path path) {
    }

    /**
     * 根据 operation 分派单条 durable marker；分派不修改状态，具体裁决仍由 operation 方法完成。
     *
     * @param record 进程启动时从 durable history 重建出的非终态记录。
     * @param timeout DROP finish 等待 purge barrier 与物理删除的正有界时间。
     * @throws DictionaryRecoveryException operation 无法与 committed DD 安全对应时抛出并阻止 OPEN。
     */
    private void recoverLoggedOperation(DdlLogRecord record, Duration timeout) {
        switch (record.operation()) {
            case CREATE_TABLE -> recoverCreate(record);
            case CREATE_INDEX -> recoverCreateIndex(record, timeout);
            case DROP_INDEX -> recoverDropIndex(record, timeout);
            case DROP_TABLE -> recoverDrop(record, timeout);
            case DISCARD_TABLESPACE -> recoverDiscard(record, timeout);
            case IMPORT_TABLESPACE -> recoverImport(record, timeout);
            case REBUILD_TABLE -> recoverRebuild(record, timeout);
            case DISCARD_RECOVERY_UNAVAILABLE -> recoverUnavailableDiscard(record, timeout);
            case DROP_RECOVERY_UNAVAILABLE -> recoverUnavailableDrop(record, timeout);
            case IMPORT_RECOVERY_REPLACEMENT -> recoverReplacementImport(record, timeout);
            case ALTER_TABLE_INPLACE -> recoverInplaceAlter(record, timeout);
            case DROP_TABLE_BATCH -> recoverBatchDrop(record, timeout, false);
            case DROP_SCHEMA_CASCADE -> recoverBatchDrop(record, timeout, true);
        }
    }

    /**
     * 依据 v5 manifest 原子恢复多表 DROP 或 DROP SCHEMA CASCADE。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 operation/protocol/manifest 形状，并逐项核对 DD identity、binding、受控路径和 exact digest。</li>
     *     <li>把整个集合分类为全 ACTIVE、全 DROP_PENDING 或全 DROPPED；任一混合/第三状态 fail-closed。</li>
     *     <li>全 ACTIVE 且仅 PREPARED 时回滚 marker；全 pending 时等待 history/cache 屏障并幂等删除全部文件。</li>
     *     <li>物理阶段 durable 后在一个 DD transaction 中发布全部 table tombstone，并按需同时发布 schema tombstone。</li>
     *     <li>全 DROPPED 终态只补缺失 phase；任何 phase 与全局状态不匹配都拒绝开放流量。</li>
     * </ol>
     *
     * @param record 非终态 DROP_TABLE_BATCH 或 DROP_SCHEMA_CASCADE marker
     * @param timeout 每张表 history、cache pin 与物理删除共用的正等待上限
     * @param cascadeSchema 当前 operation 是否必须携带并最终发布 schema tombstone
     * @throws DictionaryRecoveryException manifest、DD、路径、摘要、phase 或全局状态不一致时抛出
     */
    private void recoverBatchDrop(
            DdlLogRecord record,
            Duration timeout,
            boolean cascadeSchema) {
        // 1. marker 只保存相对表路径的 manifest；任一辅助资源字段都属于不同恢复协议。
        DdlLogOperation expected = cascadeSchema
                ? DdlLogOperation.DROP_SCHEMA_CASCADE
                : DdlLogOperation.DROP_TABLE_BATCH;
        if (record.operation() != expected
                || record.executionProtocol()
                != DdlExecutionProtocol.BATCH_DROP_V1
                || record.secondaryObjectId() != 0L
                || record.auxiliaryPath().isPresent()
                || record.fileIdentity().isPresent()
                || record.sourceSchemaDigest().isPresent()
                || record.intermediateSchemaDigest().isPresent()
                || record.targetSchemaDigest().isPresent()
                || record.retirementFence().isPresent()) {
            throw new DictionaryRecoveryException(
                    "batch DROP marker shape is invalid: ddl="
                            + record.marker().ddlOperationId());
        }
        DdlBatchManifest manifest = record.batchManifest()
                .orElseThrow(() -> new DictionaryRecoveryException(
                        "batch DROP marker lacks manifest"));
        if (cascadeSchema != manifest.schema().isPresent()
                || !cascadeSchema && manifest.tables().isEmpty()) {
            throw new DictionaryRecoveryException(
                    "batch DROP manifest operation shape is invalid");
        }
        Path expectedPrimaryPath = manifest.tables().isEmpty()
                ? tablesDirectory
                : tablesDirectory.resolve(
                manifest.tables().getFirst().relativePath())
                .toAbsolutePath().normalize();
        if (!record.path().equals(expectedPrimaryPath)) {
            throw new DictionaryRecoveryException(
                    "batch DROP marker primary path differs from manifest");
        }

        ArrayList<TableDefinition> currentTables =
                new ArrayList<>(manifest.tables().size());
        BatchTableState aggregateState = null;
        for (DdlBatchTableEntry entry : manifest.tables()) {
            TableDefinition table = repository.findTableForRecovery(
                    entry.tableId()).orElseThrow(() ->
                    new DictionaryRecoveryException(
                            "batch DROP table is absent: "
                                    + entry.tableId().value()));
            SchemaDefinition owner = repository.snapshot().schemas()
                    .get(table.schemaId());
            if (owner == null) {
                throw new DictionaryRecoveryException(
                        "batch DROP table owner schema is absent: "
                                + table.id().value());
            }
            TableStorageBinding binding = table.storageBinding()
                    .orElseThrow(() -> new DictionaryRecoveryException(
                            "batch DROP table has no binding: "
                                    + table.id().value()));
            Path path = checkedBatchPath(entry, binding);
            if (binding.rowFormatVersion()
                    != entry.rowFormatVersion()) {
                throw new DictionaryRecoveryException(
                        "batch DROP row format differs from manifest: "
                                + table.id().value());
            }
            BatchTableState state = batchTableState(
                    owner, table, entry, binding.rowFormatVersion());
            if (aggregateState != null && aggregateState != state) {
                throw new DictionaryRecoveryException(
                        "batch DROP committed DD contains mixed lifecycle states");
            }
            if (state == BatchTableState.DROPPED
                    && Files.exists(path)) {
                throw new DictionaryRecoveryException(
                        "batch DROP tombstone still has physical file: "
                                + path);
            }
            aggregateState = state;
            currentTables.add(table);
        }
        SchemaDefinition currentSchema = validateBatchSchema(
                manifest.schema(), cascadeSchema);
        if ((cascadeSchema
                && aggregateState == BatchTableState.DROP_PENDING
                && currentSchema.state() != SchemaState.ACTIVE)
                || (cascadeSchema
                && aggregateState == BatchTableState.DROPPED
                && currentSchema.state() != SchemaState.DROPPED)) {
            throw new DictionaryRecoveryException(
                    "DROP SCHEMA manifest has non-atomic schema/table states");
        }

        // 2. ACTIVE 集合只有在 PREPARED 且 schema 也 ACTIVE 时能证明尚未越过 DD 提交边界。
        DdlId ddlId = DdlId.of(
                record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();
        if (aggregateState == BatchTableState.ACTIVE
                || (aggregateState == null
                && currentSchema.state() == SchemaState.ACTIVE
                && phase == DdlLogPhase.PREPARED)) {
            if (phase != DdlLogPhase.PREPARED
                    || (cascadeSchema
                    && currentSchema.state() != SchemaState.ACTIVE)) {
                throw new DictionaryRecoveryException(
                        "batch DROP ACTIVE set has forward-only marker phase");
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED,
                    DdlLogPhase.ROLLED_BACK);
            return;
        }
        if (aggregateState == null
                && currentSchema.state() == SchemaState.ACTIVE
                && phase != DdlLogPhase.DICTIONARY_COMMITTED
                && phase != DdlLogPhase.ENGINE_DONE) {
            throw new DictionaryRecoveryException(
                    "empty schema DROP state/phase is inconsistent");
        }

        // 3. pending 集合必须已有 DD commit phase；恢复重新等待持久 history 和旧 metadata pin。
        if (aggregateState == BatchTableState.DROP_PENDING) {
            if (phase != DdlLogPhase.DICTIONARY_COMMITTED
                    && phase != DdlLogPhase.ENGINE_DONE) {
                throw new DictionaryRecoveryException(
                        "batch DROP pending set has invalid marker phase: "
                                + phase);
            }
            for (int index = 0; index < currentTables.size();
                 index++) {
                TableDefinition table = currentTables.get(index);
                DdlBatchTableEntry entry =
                        manifest.tables().get(index);
                TableStorageBinding binding =
                        table.storageBinding().orElseThrow();
                Path path = checkedBatchPath(entry, binding);
                purgeBarrier.awaitUnreferenced(
                        table.id().value(), timeout);
                if (!cache.awaitUnpinned(table.id(), timeout)) {
                    throw new DictionaryRecoveryException(
                            "timed out waiting batch DROP metadata pin: "
                                    + table.id().value());
                }
                if (phase == DdlLogPhase.ENGINE_DONE
                        && Files.exists(path)) {
                    throw new DictionaryRecoveryException(
                            "batch DROP ENGINE_DONE still has file: "
                                    + path);
                }
                if (Files.exists(path)) {
                    physical.dropTable(binding, timeout);
                }
            }
            if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
                repository.ddlLog().transition(
                        ddlId, phase, DdlLogPhase.ENGINE_DONE);
                phase = DdlLogPhase.ENGINE_DONE;
            }
        } else if (aggregateState == BatchTableState.DROPPED
                && phase != DdlLogPhase.ENGINE_DONE) {
            throw new DictionaryRecoveryException(
                    "batch DROP tombstone set has invalid marker phase: "
                            + phase);
        }
        if (aggregateState == null
                && phase == DdlLogPhase.DICTIONARY_COMMITTED) {
            repository.ddlLog().transition(
                    ddlId, phase, DdlLogPhase.ENGINE_DONE);
            phase = DdlLogPhase.ENGINE_DONE;
        }

        // 4. pending 或空级联前滚共享 marker target version；schema 与所有表只在这一事务同时成为 tombstone。
        if (aggregateState == BatchTableState.DROP_PENDING
                || (aggregateState == null
                && currentSchema.state() == SchemaState.ACTIVE)) {
            DictionaryVersion targetVersion = DictionaryVersion.of(
                    record.marker().dictionaryVersion() + 1);
            ArrayList<TableDefinition> dropped =
                    new ArrayList<>(currentTables.size());
            for (int index = 0; index < currentTables.size();
                 index++) {
                TableDefinition table = currentTables.get(index);
                TableDefinition target = new TableDefinition(
                        table.id(), table.schemaId(), table.name(),
                        targetVersion, TableState.DROPPED,
                        table.columns(), table.indexes(),
                        table.storageBinding(), table.options());
                requireBatchDigest(
                        manifest.tables().get(index)
                                .targetSchemaDigest(),
                        schemaDigests.digest(
                                repository.snapshot().schemas()
                                        .get(table.schemaId()),
                                target,
                                manifest.tables().get(index)
                                        .rowFormatVersion()),
                        "target table", table.id().value());
                dropped.add(target);
            }
            SchemaDefinition schemaTarget = cascadeSchema
                    ? schemaTarget(
                    currentSchema,
                    manifest.schema().orElseThrow(),
                    targetVersion)
                    : null;
            commitRecoveredBatch(
                    targetVersion, schemaTarget, dropped);
            for (TableDefinition table : dropped) {
                cache.invalidateTable(
                        table.id(), targetVersion);
            }
            aggregateState = currentTables.isEmpty()
                    ? null : BatchTableState.DROPPED;
            currentSchema = schemaTarget == null
                    ? currentSchema : schemaTarget;
        }

        // 5. terminal append 晚于原子 DD target；append outcome 不确定时下次仍从全 tombstone/ENGINE_DONE 补齐。
        if (phase != DdlLogPhase.ENGINE_DONE
                || (cascadeSchema
                && currentSchema.state() != SchemaState.DROPPED)) {
            throw new DictionaryRecoveryException(
                    "batch DROP cannot reach terminal from observed state");
        }
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.ENGINE_DONE,
                DdlLogPhase.COMMITTED);
    }

    /** 把一张 manifest 表按 exact digest 分类为三种且仅三种可恢复状态。 */
    private BatchTableState batchTableState(
            SchemaDefinition schema,
            TableDefinition table,
            DdlBatchTableEntry entry,
            long rowFormatVersion) {
        DdlSchemaDigest observed = schemaDigests.digest(
                schema, table, rowFormatVersion);
        return switch (table.state()) {
            case ACTIVE -> {
                requireBatchDigest(
                        entry.sourceSchemaDigest(), observed,
                        "source table", table.id().value());
                yield BatchTableState.ACTIVE;
            }
            case DROP_PENDING -> {
                requireBatchDigest(
                        entry.pendingSchemaDigest(), observed,
                        "pending table", table.id().value());
                yield BatchTableState.DROP_PENDING;
            }
            case DROPPED -> {
                requireBatchDigest(
                        entry.targetSchemaDigest(), observed,
                        "target table", table.id().value());
                yield BatchTableState.DROPPED;
            }
            default -> throw new DictionaryRecoveryException(
                    "batch DROP table has unsupported state: table="
                            + table.id().value() + " state="
                            + table.state());
        };
    }

    /** 校验级联 schema 的 identity、名称和 source/target exact digest；非级联返回 {@code null}。 */
    private SchemaDefinition validateBatchSchema(
            Optional<DdlBatchSchemaEntry> expected,
            boolean cascadeSchema) {
        if (!cascadeSchema) {
            return null;
        }
        DdlBatchSchemaEntry entry = expected.orElseThrow();
        SchemaDefinition schema = repository.snapshot().schemas()
                .get(entry.schemaId());
        if (schema == null
                || !schema.name().canonicalName()
                .equals(entry.canonicalName())) {
            throw new DictionaryRecoveryException(
                    "DROP SCHEMA manifest identity/name differs from DD");
        }
        DdlSchemaDigest observed = schemaDigests.digest(schema);
        requireBatchDigest(
                schema.state() == SchemaState.ACTIVE
                        ? entry.sourceSchemaDigest()
                        : entry.targetSchemaDigest(),
                observed, "schema", schema.id().value());
        return schema;
    }

    /** 从 source schema 构造并校验 manifest 声明的 target tombstone。 */
    private SchemaDefinition schemaTarget(
            SchemaDefinition source,
            DdlBatchSchemaEntry entry,
            DictionaryVersion targetVersion) {
        SchemaDefinition target = new SchemaDefinition(
                source.id(), source.name(),
                source.defaultCharsetId(),
                source.defaultCollationId(),
                targetVersion, SchemaState.DROPPED);
        requireBatchDigest(
                entry.targetSchemaDigest(),
                schemaDigests.digest(target),
                "target schema", target.id().value());
        return target;
    }

    /** 校验 manifest 相对路径、table/space identity 与 committed binding 逐项相同。 */
    private Path checkedBatchPath(
            DdlBatchTableEntry entry,
            TableStorageBinding binding) {
        Path expected = tablesDirectory.resolve(
                entry.relativePath()).toAbsolutePath().normalize();
        Path checked = discovery.checkedPath(expected);
        Path exact = tablesDirectory.resolve(
                "table_" + entry.tableId().value()
                        + "_space_" + entry.spaceId().value()
                        + ".ibd").toAbsolutePath().normalize();
        if (!checked.equals(exact)
                || binding.tableId() != entry.tableId().value()
                || !binding.spaceId().equals(entry.spaceId())
                || !binding.path().toAbsolutePath().normalize()
                .equals(exact)) {
            throw new DictionaryRecoveryException(
                    "batch DROP path/binding identity mismatch: table="
                            + entry.tableId().value());
        }
        return exact;
    }

    /** 常量时间摘要值对象比较失败时附带 checkpoint 与对象 identity。 */
    private static void requireBatchDigest(
            DdlSchemaDigest expected,
            DdlSchemaDigest observed,
            String checkpoint,
            long objectId) {
        if (!expected.equals(observed)) {
            throw new DictionaryRecoveryException(
                    "batch DROP " + checkpoint
                            + " digest mismatch: object=" + objectId);
        }
    }

    /** 用一个 catalog transaction 发布级联 schema 与全部 table target。 */
    private void commitRecoveredBatch(
            DictionaryVersion version,
            SchemaDefinition schema,
            List<TableDefinition> tables) {
        try (DictionaryTransaction transaction =
                     repository.begin(version)) {
            if (schema != null) {
                transaction.updateSchema(schema);
            }
            for (TableDefinition table : tables) {
                transaction.updateTable(table);
            }
            transaction.commit();
        }
    }

    /** 批量 DROP 恢复只承认三种稳定的全局 table 生命周期。 */
    private enum BatchTableState {
        ACTIVE,
        DROP_PENDING,
        DROPPED
    }

    /**
     * 依据committed DD、control与同space target SDI恢复metadata-only原地ALTER。SDI只有在FORWARD_ONLY后才有
     * 反向发布资格；OPEN/CANCEL_REQUESTED始终由source DD覆盖，避免损坏catalog提交裁决边界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验protocol/path/binding并按marker target version把committed DD分类为source或target。</li>
     *     <li>source+OPEN/CANCEL重写source SDI并ROLLED_BACK；不读取SDI内容决定方向。</li>
     *     <li>source+FORWARD_ONLY从exact binding SDI解码target，以target digest门禁后提交DD与phase。</li>
     *     <li>target DD只接受FORWARD_ONLY和target digest，修复SDI/cache后补DICTIONARY_COMMITTED/COMMITTED。</li>
     * </ol>
     *
     * @param record durable ALTER_TABLE_INPLACE非终态marker
     * @param timeout SDI重写与恢复发布共用的正等待上界
     * @throws DictionaryRecoveryException protocol、digest、SDI、DD版本或phase出现第三态时抛出并阻止OPEN
     */
    private void recoverInplaceAlter(DdlLogRecord record, Duration timeout) {
        if (record.executionProtocol() == DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1
                && record.auxiliaryPath().isPresent()) {
            recoverGeneralOnlineAlter(record, timeout, false);
            return;
        }
        // 1、原地ALTER不允许auxiliary/secondary identity；物理binding仍由marker与committed DD双重见证。
        if (record.executionProtocol() != DdlExecutionProtocol.ONLINE_ALTER_INPLACE_V1
                || record.secondaryObjectId() != 0L || record.auxiliaryPath().isPresent()
                || record.fileIdentity().isPresent() || record.retirementFence().isPresent()) {
            throw new DictionaryRecoveryException(
                    "ALTER_TABLE_INPLACE marker shape is invalid: ddl="
                            + record.marker().ddlOperationId());
        }
        Path path = checkedMarkerPath(record);
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition current = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException(
                        "ALTER_TABLE_INPLACE table is absent: " + tableId.value()));
        TableStorageBinding binding = matchingRecoveryBinding(record, current, path);
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        long targetVersion = record.marker().dictionaryVersion();
        boolean source = current.version().value() < targetVersion;
        boolean target = current.version().value() == targetVersion;
        if ((!source && !target) || current.state() != TableState.ACTIVE) {
            throw new DictionaryRecoveryException(
                    "ALTER_TABLE_INPLACE DD version/state is neither source nor target: table="
                            + tableId.value() + " version=" + current.version().value());
        }

        // 2、未跨forward fence时只信committed source；reconcile会覆盖可能已durable的target SDI。
        if (source && record.controlState() != DdlControlState.FORWARD_ONLY) {
            if (record.phase() != DdlLogPhase.PREPARED
                    || record.controlState() == DdlControlState.OPEN
                    && record.cancellation().isPresent()) {
                throw new DictionaryRecoveryException(
                        "ALTER_TABLE_INPLACE rollback phase/control is invalid: ddl="
                                + ddlId.value());
            }
            requireSchemaCheckpoint(record, current, SchemaCheckpoint.SOURCE);
            sdi.reconcile(current, timeout);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            cache.restoreTableAfterDdlRollback(
                    current, DictionaryVersion.of(targetVersion));
            return;
        }

        // 3、source前滚必须从exact page3 SDI取得完整target aggregate；缺失/错绑/digest漂移均保留现场。
        if (source) {
            if (record.phase() != DdlLogPhase.PREPARED
                    || record.controlState() != DdlControlState.FORWARD_ONLY) {
                throw new DictionaryRecoveryException(
                        "ALTER_TABLE_INPLACE forward source phase/control is invalid: ddl="
                                + ddlId.value());
            }
            TableDefinition recoveredTarget;
            try {
                recoveredTarget = sdi.read(binding).orElseThrow(() ->
                        new DictionaryRecoveryException(
                                "ALTER_TABLE_INPLACE target SDI is absent: ddl=" + ddlId.value()));
            } catch (DictionaryRecoveryException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw new DictionaryRecoveryException(
                        "ALTER_TABLE_INPLACE target SDI cannot be decoded: ddl="
                                + ddlId.value(), failure);
            }
            if (!recoveredTarget.id().equals(tableId)
                    || recoveredTarget.version().value() != targetVersion
                    || recoveredTarget.state() != TableState.ACTIVE) {
                throw new DictionaryRecoveryException(
                        "ALTER_TABLE_INPLACE target SDI identity/version is invalid: ddl="
                                + ddlId.value());
            }
            requireSchemaCheckpoint(record, recoveredTarget, SchemaCheckpoint.TARGET);
            cache.invalidateTable(tableId, recoveredTarget.version());
            commitRecoveredTable(recoveredTarget);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            current = recoveredTarget;
        }

        // 4、DD target是不可逆提交真相；PREPARED覆盖“DD成功但phase响应丢失”，其余只逐边补terminal。
        if (record.controlState() != DdlControlState.FORWARD_ONLY) {
            throw new DictionaryRecoveryException(
                    "ALTER_TABLE_INPLACE target DD lacks FORWARD_ONLY: ddl=" + ddlId.value());
        }
        requireSchemaCheckpoint(record, current, SchemaCheckpoint.TARGET);
        sdi.reconcile(current, timeout);
        DdlLogPhase phase = repository.ddlLog().find(ddlId).orElseThrow().phase();
        if (phase == DdlLogPhase.PREPARED) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        if (phase != DdlLogPhase.DICTIONARY_COMMITTED) {
            throw new DictionaryRecoveryException(
                    "ALTER_TABLE_INPLACE target phase is invalid: ddl=" + ddlId.value()
                            + " phase=" + phase);
        }
        cache.publishTable(current);
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
    }

    /**
     * 重放不依赖 page0 的隔离对象 DISCARD，严格维持 physical→DD 的 create-like 阶段顺序。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 marker canonical/transfer 路径、table/space binding 与允许的 DD 起止状态。</li>
     *     <li>PREPARED 时调用 raw 幂等移动；源缺失视为已完成，两端同时存在由 storage fail-closed。</li>
     *     <li>ENGINE_DONE 时使用 marker 已预留版本提交 RECOVERY_DISCARDED，再写 DICTIONARY_COMMITTED。</li>
     *     <li>发布 cache 隔离屏障并写 COMMITTED；已提交终态只允许补齐后置 phase。</li>
     * </ol>
     *
     * @param record durable DISCARD_RECOVERY_UNAVAILABLE marker
     * @param timeout 物理独占 lease 的正等待上界
     * @throws DictionaryRecoveryException DD、路径、binding 或 phase 不能唯一收敛时抛出
     */
    private void recoverUnavailableDiscard(DdlLogRecord record, Duration timeout) {
        // 1. canonical path 只能来自 tables 根，quarantine 只能来自固定 transfer/discarded 根。
        Path path = checkedMarkerPath(record);
        Path quarantine = record.auxiliaryPath().map(this::checkedTransferPath).orElseThrow(() ->
                new DictionaryRecoveryException(
                        "recovery DISCARD marker has no quarantine path"));
        TableDefinition table = recoveryObject(record);
        TableStorageBinding binding = matchingRecoveryBinding(record, table, path);
        if (table.state() != TableState.RECOVERY_UNAVAILABLE
                && table.state() != TableState.RECOVERY_DISCARDED) {
            throw new DictionaryRecoveryException(
                    "recovery DISCARD target has invalid state: " + table.state());
        }
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();
        requireSchemaCheckpoint(record, table,
                table.state() == TableState.RECOVERY_UNAVAILABLE
                        ? SchemaCheckpoint.SOURCE : SchemaCheckpoint.TARGET);

        // 2. DD 尚未到终态时物理动作必须可重复，且不能打开损坏文件。
        if (table.state() == TableState.RECOVERY_UNAVAILABLE && phase == DdlLogPhase.PREPARED) {
            physical.discardRecoveryUnavailable(binding, quarantine, timeout);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            phase = DdlLogPhase.ENGINE_DONE;
        }

        // 3. ENGINE_DONE 是提交 DD 的唯一入口，版本直接来自 durable marker 而不重新 reserve。
        if (table.state() == TableState.RECOVERY_UNAVAILABLE && phase == DdlLogPhase.ENGINE_DONE) {
            DictionaryVersion version = DictionaryVersion.of(
                    record.marker().dictionaryVersion());
            TableDefinition discarded = lifecycle(
                    table, version, TableState.RECOVERY_DISCARDED);
            commitRecoveredTable(discarded);
            cache.invalidateTable(table.id(), version);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }

        // 4. 若 catalog 已是终态，只接受与 physical-first 顺序一致的后置 phase。
        TableDefinition current = repository.findTableForRecovery(table.id()).orElseThrow();
        if (current.state() != TableState.RECOVERY_DISCARDED
                || phase == DdlLogPhase.PREPARED) {
            throw new DictionaryRecoveryException(
                    "recovery DISCARD phase/DD state mismatch: phase=" + phase
                            + " state=" + current.state());
        }
        requireSchemaCheckpoint(record, current, SchemaCheckpoint.TARGET);
        if (phase == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        cache.invalidateTable(current.id(), current.version());
        if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        }
    }

    /**
     * 重放 recovery-isolated DROP；raw deleteIfExists 与 tombstone 提交均按 durable phase 幂等收敛。
     *
     * @param record durable DROP_RECOVERY_UNAVAILABLE marker
     * @param timeout 物理独占 lease 的正等待上界
     * @throws DictionaryRecoveryException 状态、binding 或 phase 无法唯一解释时抛出
     */
    private void recoverUnavailableDrop(DdlLogRecord record, Duration timeout) {
        // 1. tombstone 前仅允许两种隔离态；canonical path 必须仍受 tables 根约束。
        Path path = checkedMarkerPath(record);
        TableDefinition table = recoveryObject(record);
        TableStorageBinding binding = matchingRecoveryBinding(record, table, path);
        if (table.state() != TableState.RECOVERY_UNAVAILABLE
                && table.state() != TableState.RECOVERY_DISCARDED
                && table.state() != TableState.DROPPED) {
            throw new DictionaryRecoveryException(
                    "recovery DROP target has invalid state: " + table.state());
        }
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();
        Path deletionPath = record.auxiliaryPath()
                .map(this::checkedTransferPath).orElse(path);
        if (table.state() == TableState.RECOVERY_DISCARDED
                && record.auxiliaryPath().isEmpty()) {
            throw new DictionaryRecoveryException(
                    "RECOVERY_DISCARDED drop marker lacks discarded-file path");
        }
        requireSchemaCheckpoint(record, table,
                table.state() == TableState.DROPPED
                        ? SchemaCheckpoint.TARGET : SchemaCheckpoint.SOURCE);

        // 2. PREPARED 只执行无 page0 的幂等删除；RECOVERY_DISCARDED 缺少 canonical 文件属于正常终点。
        if (table.state() != TableState.DROPPED && phase == DdlLogPhase.PREPARED) {
            physical.dropRecoveryUnavailable(binding, deletionPath, timeout);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            phase = DdlLogPhase.ENGINE_DONE;
        }

        // 3. 物理完成后用 marker 版本发布 DROPPED，保留 binding 作为审计 tombstone。
        if (table.state() != TableState.DROPPED && phase == DdlLogPhase.ENGINE_DONE) {
            DictionaryVersion version = DictionaryVersion.of(
                    record.marker().dictionaryVersion());
            TableDefinition dropped = lifecycle(table, version, TableState.DROPPED);
            commitRecoveredTable(dropped);
            cache.invalidateTable(table.id(), version);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }

        // 4. 已有 tombstone 时只补允许的 post-DD phase；PREPARED 绝不能越过缺失的 ENGINE_DONE。
        TableDefinition current = repository.findTableForRecovery(table.id()).orElseThrow();
        if (current.state() != TableState.DROPPED || phase == DdlLogPhase.PREPARED) {
            throw new DictionaryRecoveryException(
                    "recovery DROP phase/DD state mismatch: phase=" + phase
                            + " state=" + current.state());
        }
        requireSchemaCheckpoint(record, current, SchemaCheckpoint.TARGET);
        if (phase == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        cache.invalidateTable(current.id(), current.version());
        if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        }
    }

    /**
     * 重放可信 replacement import。PREPARED 必须重新验证固定 incoming pair；ENGINE_DONE 重启后重新挂载
     * 已恢复为 NORMAL 的 canonical 文件，随后才允许提交 ACTIVE。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 marker path/space/identity 与 RECOVERY_DISCARDED 或已提交 ACTIVE aggregate。</li>
     *     <li>PREPARED 时重新验证 HMAC/hash/page0 并幂等覆盖 canonical，完成 NORMAL force 后写 ENGINE_DONE。</li>
     *     <li>重启时若直接看到 ENGINE_DONE，则只读挂载已完成 replacement；再用 marker 版本提交 ACTIVE。</li>
     *     <li>补齐 DICTIONARY_COMMITTED、发布 ACTIVE cache并写 COMMITTED；来源 incoming pair 始终保留。</li>
     * </ol>
     *
     * @param record durable IMPORT_RECOVERY_REPLACEMENT marker
     * @param timeout 物理 import、flush 与 lease 的正上界
     * @throws DictionaryRecoveryException 可信证据、阶段或 DD state 无法唯一收敛时抛出
     */
    private void recoverReplacementImport(DdlLogRecord record, Duration timeout) {
        // 1. marker canonical path 与 DD binding 必须一致，file identity 是初次 HMAC 验证后的 durable 摘要。
        Path path = checkedMarkerPath(record);
        TableDefinition table = recoveryObject(record);
        TableStorageBinding binding = matchingRecoveryBinding(record, table, path);
        TablespaceFileIdentity markerIdentity = record.fileIdentity().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "trusted replacement marker has no file identity"));
        Path source = record.auxiliaryPath().map(this::checkedTransferPath).orElseThrow(() ->
                new DictionaryRecoveryException(
                        "trusted replacement marker has no incoming data path"));
        if (table.state() != TableState.RECOVERY_DISCARDED
                && table.state() != TableState.ACTIVE) {
            throw new DictionaryRecoveryException(
                    "trusted replacement target has invalid DD state: " + table.state());
        }
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();
        boolean importedNow = false;
        requireSchemaCheckpoint(record, table,
                table.state() == TableState.RECOVERY_DISCARDED
                        ? SchemaCheckpoint.SOURCE : SchemaCheckpoint.TARGET);

        // 2. PREPARED 绝不只信 marker identity；重新验证仍在固定 incoming 的完整签名 pair 后才覆盖 canonical。
        if (table.state() == TableState.RECOVERY_DISCARDED
                && phase == DdlLogPhase.PREPARED) {
            Path expectedSource = recoveryBackups.incomingDataPath(table);
            if (!source.equals(expectedSource)) {
                throw new DictionaryRecoveryException(
                        "trusted replacement marker source is not fixed incoming path: " + source);
            }
            ValidatedRecoveryBackup backup = recoveryBackups.validateIncoming(table);
            if (!backup.fileIdentity().equals(markerIdentity)) {
                throw new DictionaryRecoveryException(
                        "trusted replacement marker identity changed since validation");
            }
            physical.importTablespace(
                    binding, source, markerIdentity, timeout);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            phase = DdlLogPhase.ENGINE_DONE;
            importedNow = true;
        }

        // 3. ENGINE_DONE 跨重启后句柄已关闭；若不是本次 import 打开的，就按 NORMAL page0 只读重挂载。
        if (table.state() == TableState.RECOVERY_DISCARDED
                && phase == DdlLogPhase.ENGINE_DONE) {
            if (!importedNow) {
                physical.mountRecoveryReplacement(binding);
            }
            DictionaryVersion version = DictionaryVersion.of(
                    record.marker().dictionaryVersion());
            TableDefinition active = lifecycle(table, version, TableState.ACTIVE);
            commitRecoveredTable(active);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            table = active;
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }

        // 4. catalog 已提交但 phase append 结果不确定时允许从 ENGINE_DONE 补齐，其余越级组合拒绝。
        TableDefinition current = repository.findTableForRecovery(table.id()).orElseThrow();
        if (current.state() != TableState.ACTIVE
                || current.version().value() != record.marker().dictionaryVersion()
                || phase == DdlLogPhase.PREPARED) {
            throw new DictionaryRecoveryException(
                    "trusted replacement phase/DD state mismatch: phase=" + phase
                            + " state=" + current.state());
        }
        requireSchemaCheckpoint(record, current, SchemaCheckpoint.TARGET);
        if (phase == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        cache.publishTable(current);
        if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        }
    }

    /** marker affected id 必须命中一个恢复可见 aggregate。 */
    private TableDefinition recoveryObject(DdlLogRecord record) {
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        return repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException(
                        "recovery-object DDL target table is absent: " + tableId.value()));
    }

    /** DD binding 是 marker path/space 的第二份独立身份见证。 */
    private static TableStorageBinding matchingRecoveryBinding(
            DdlLogRecord record, TableDefinition table, Path checkedPath) {
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "recovery-object table has no binding: " + table.id().value()));
        if (!binding.spaceId().equals(record.spaceId())
                || !binding.path().toAbsolutePath().normalize().equals(checkedPath)) {
            throw new DictionaryRecoveryException(
                    "recovery-object marker/DD binding mismatch: table=" + table.id().value());
        }
        return binding;
    }

    /** 以 marker 已预留的严格后继版本提交恢复终态，不在重启时消耗新的字典版本。 */
    private void commitRecoveredTable(TableDefinition table) {
        try (DictionaryTransaction tx = repository.begin(table.version())) {
            tx.updateTable(table);
            tx.commit();
        }
    }

    /** 只替换字典版本与生命周期，保持隔离前的逻辑定义和物理 identity 完全不变。 */
    private static TableDefinition lifecycle(
            TableDefinition before, DictionaryVersion version, TableState state) {
        return new TableDefinition(
                before.id(), before.schemaId(), before.name(), version, state,
                before.columns(), before.indexes(), before.storageBinding(), before.options());
    }

    /** recovery已经用operation/state/version/index presence分类出的schema checkpoint。 */
    private enum SchemaCheckpoint {
        /** operation开始时的committed aggregate。 */
        SOURCE,
        /** DROP/DISCARD/IMPORT的durable pending aggregate。 */
        INTERMEDIATE,
        /** operation最终发布的committed aggregate或tombstone。 */
        TARGET
    }

    /**
     * 对已经由状态机分类的DD aggregate执行canonical digest门禁；任何不匹配都保留marker、descriptor和文件。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>legacy marker没有checkpoint能力，保持原有exact identity恢复分支。</li>
     *     <li>按分类读取marker中对应的必需digest；缺失表示v4 history损坏，不能降级。</li>
     *     <li>从同一committed table解析schema与row-format，使用TABLE_SCHEMA_V1重算内容证明。</li>
     *     <li>常量时间比较失败即抛出fatal recovery异常，调用方不得在异常后执行清理或phase推进。</li>
     * </ol>
     *
     * @param record 已通过codec、path和operation状态分类的非终态marker
     * @param table 被分类为source/intermediate/target之一的committed aggregate
     * @param checkpoint 本分支选择的唯一checkpoint类型
     * @throws DictionaryRecoveryException schema/binding缺失、checkpoint缺失或digest不一致时抛出并阻止OPEN
     */
    private void requireSchemaCheckpoint(
            DdlLogRecord record, TableDefinition table, SchemaCheckpoint checkpoint) {
        // 1、v1-v3没有digest字段；兼容恢复只依赖原有version/path/binding严格判断。
        if (record.executionProtocol() == DdlExecutionProtocol.LEGACY_PHASE_ONLY) {
            return;
        }

        // 2、production v4策略保证被分类checkpoint存在；缺失不能解释为“无需校验”。
        Optional<DdlSchemaDigest> selected = switch (checkpoint) {
            case SOURCE -> record.sourceSchemaDigest();
            case INTERMEDIATE -> record.intermediateSchemaDigest();
            case TARGET -> record.targetSchemaDigest();
        };
        DdlSchemaDigest expected = selected.orElseThrow(() ->
                new DictionaryRecoveryException(
                        "DDL schema checkpoint is absent: ddl="
                                + record.marker().ddlOperationId() + " checkpoint=" + checkpoint));

        // 3、row-format来自当前已分类aggregate的binding；digest本身不读取root/segment等可变物理字段。
        SchemaDefinition schema = repository.findSchema(table.schemaId()).orElseThrow(() ->
                new DictionaryRecoveryException(
                        "DDL schema checkpoint owner is absent: ddl="
                                + record.marker().ddlOperationId() + " schema="
                                + table.schemaId().value()));
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "DDL schema checkpoint table has no row format: ddl="
                                + record.marker().ddlOperationId() + " table="
                                + table.id().value()));
        DdlSchemaDigest actual = schemaDigests.digest(
                schema, table, binding.rowFormatVersion());

        // 4、失败前没有任何phase、catalog、descriptor或文件副作用，现场留给人工诊断/修复。
        if (!expected.equals(actual)) {
            throw new DictionaryRecoveryException(
                    "DDL schema digest mismatch: ddl=" + record.marker().ddlOperationId()
                            + " operation=" + record.operation()
                            + " checkpoint=" + checkpoint + " table=" + table.id().value());
        }
    }

    /** 只允许 marker auxiliary path 位于固定 tablespace-transfer 根，禁止借 catalog 删除任意路径。 */
    private Path checkedTransferPath(Path path) {
        if (path == null) {
            throw new DictionaryRecoveryException("DDL transfer path must not be null");
        }
        Path root = tablesDirectory.getParent().resolve("tablespace-transfer")
                .toAbsolutePath().normalize();
        Path normalized = path.toAbsolutePath().normalize();
        if (!normalized.startsWith(root) || hasSymbolicLinkComponent(normalized)) {
            throw new DictionaryRecoveryException(
                    "DDL transfer path escapes controlled root or is a symbolic link: " + normalized);
        }
        return normalized;
    }

    /** 逐级拒绝既有符号链接；即使最终文件尚不存在，也不能通过已替换的父目录逃逸实例根。 */
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

    /**
     * 依据 committed DD 裁决 shadow rebuild：旧 binding 仍被引用则删除 shadow 并回滚 marker；新 binding
     * 已提交则保留 shadow、终结 marker，旧文件由统一 orphan cleanup 删除。任何第三种 identity 阻止 OPEN。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从 marker 读取旧/新受控路径，并从 committed DD 读取当前唯一 binding。</li>
     *     <li>DD 仍引用旧 binding 时删除 exact shadow path，并把 PREPARED/ENGINE_DONE 终结为 ROLLED_BACK。</li>
     *     <li>DD 已引用新 binding 时拒绝 PREPARED phase loss，补齐 ENGINE_DONE→DICTIONARY_COMMITTED。</li>
     *     <li>发布 committed table cache、补 COMMITTED；旧路径留给全局 orphan cleanup 在 ACTIVE 对账后删除。</li>
     * </ol>
     *
     * @param record 非终态 REBUILD_TABLE marker
     * @throws DictionaryRecoveryException marker、DD binding、路径或 phase 无法形成旧/新二选一裁决时抛出，
     *                                     调用方必须保持普通流量关闭
     */
    private void recoverRebuild(DdlLogRecord record, Duration timeout) {
        if (record.executionProtocol() == DdlExecutionProtocol.ONLINE_ALTER_SHADOW_V1) {
            recoverGeneralOnlineAlter(record, timeout, true);
            return;
        }
        // 1、marker 两条路径都必须经过实例 tables 目录守门；DD binding 是交换是否提交的唯一裁决点。
        Path oldPath = checkedMarkerPath(record);
        Path shadowPath = record.auxiliaryPath().map(discovery::checkedPath).orElseThrow(() ->
                new DictionaryRecoveryException("REBUILD marker has no shadow path"));
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException(
                        "REBUILD marker target table is absent: " + tableId.value()));
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "REBUILD marker target has no binding: " + tableId.value()));
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        boolean committedOld = binding.spaceId().equals(record.spaceId())
                && binding.path().equals(oldPath);
        boolean committedNew = binding.spaceId().value() == record.secondaryObjectId()
                && binding.path().equals(shadowPath);
        if (committedOld) {
            // 2、旧 DD 证明交换未提交；删除只针对 marker exact shadow，绝不触碰仍被引用的旧文件。
            if (record.phase() == DdlLogPhase.DICTIONARY_COMMITTED) {
                throw new DictionaryRecoveryException(
                        "REBUILD marker says dictionary committed but DD retains old binding");
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.SOURCE);
            if (Files.exists(shadowPath)) {
                delete(shadowPath, "rolled-back ALTER shadow tablespace");
            }
            repository.ddlLog().transition(
                    ddlId, record.phase(), DdlLogPhase.ROLLED_BACK);
            // ALTER 在 DD 提交前建立了本地 publication barrier；旧 DD 获胜后必须显式恢复旧 aggregate 准入。
            cache.restoreTableAfterDdlRollback(
                    table, DictionaryVersion.of(record.marker().dictionaryVersion()));
            return;
        }
        // 3、新 DD 与 PREPARED 不可同时成立；它表示 durable phase history 丢失，不能猜测前滚。
        if (!committedNew || record.phase() == DdlLogPhase.PREPARED) {
            throw new DictionaryRecoveryException(
                    "REBUILD marker/DD binding identity cannot be reconciled: table="
                            + tableId.value());
        }
        requireSchemaCheckpoint(record, table, SchemaCheckpoint.TARGET);
        if (record.phase() == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
        }
        DdlLogPhase current = repository.ddlLog().find(ddlId).orElseThrow().phase();
        if (current == DdlLogPhase.DICTIONARY_COMMITTED) {
            // 4、cache 只发布 committed DD；旧 path 的删除晚于全部 ACTIVE SDI reconcile。
            cache.publishTable(table);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        }
        // oldPath 尚未被 discovery 打开，统一 orphan cleanup 会在所有 marker/SDI 收敛后安全删除。
    }

    /**
     * 执行数据字典恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DictionaryRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    private void recoverDiscard(DdlLogRecord record, Duration timeout) {
        // 1、path与state/version先完成source/intermediate/target分类；digest失败前不推进phase或移动文件。
        Path path = checkedMarkerPath(record);
        Path quarantine = record.auxiliaryPath().map(this::checkedTransferPath).orElseThrow(() ->
                new DictionaryRecoveryException("DISCARD marker has no quarantine path"));
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException("DISCARD marker target table is absent: " + tableId.value()));
        validateBinding(table, record);
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();
        if (table.state() == TableState.ACTIVE) {
            if (phase != DdlLogPhase.PREPARED
                    || table.version().value() >= record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException("DISCARD ACTIVE phase/version mismatch");
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.SOURCE);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            return;
        }

        // 2、pending是dictionary-first提交点；物理移动幂等完成后才写ENGINE_DONE。
        if (table.state() == TableState.DISCARD_PENDING) {
            if (table.version().value() != record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException("DISCARD_PENDING version mismatch");
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.INTERMEDIATE);
            if (phase == DdlLogPhase.PREPARED) {
                repository.ddlLog().transition(
                        ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
                phase = DdlLogPhase.DICTIONARY_COMMITTED;
            }
            if (phase != DdlLogPhase.DICTIONARY_COMMITTED
                    && phase != DdlLogPhase.ENGINE_DONE) {
                throw new DictionaryRecoveryException("DISCARD_PENDING phase mismatch: " + phase);
            }
            if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
                physical.discardTablespace(table.storageBinding().orElseThrow(), quarantine, timeout);
                repository.ddlLog().transition(
                        ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            } else if (Files.exists(path)) {
                throw new DictionaryRecoveryException(
                        "DISCARD ENGINE_DONE still has canonical file: ddl=" + ddlId.value());
            }

            // 3、target版本已由live reserve固定为pending+1，恢复不得另行消耗control version。
            DictionaryVersion targetVersion = DictionaryVersion.of(
                    record.marker().dictionaryVersion() + 1);
            TableDefinition discarded = lifecycle(
                    table, targetVersion, TableState.DISCARDED);
            commitRecoveredTable(discarded);
            requireSchemaCheckpoint(record, discarded, SchemaCheckpoint.TARGET);
            cache.invalidateTable(table.id(), targetVersion);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            return;
        }

        // 4、target DD已存在时只允许补terminal，不能重复移动或重新分配版本。
        if (table.state() != TableState.DISCARDED
                || table.version().value() != record.marker().dictionaryVersion() + 1
                || phase != DdlLogPhase.ENGINE_DONE) {
            throw new DictionaryRecoveryException(
                    "DISCARD target state/version/phase mismatch: state=" + table.state()
                            + " phase=" + phase);
        }
        requireSchemaCheckpoint(record, table, SchemaCheckpoint.TARGET);
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
    }

    /**
     * 执行数据字典恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     */
    private void recoverImport(DdlLogRecord record, Duration timeout) {
        // 1、路径、identity与DD state/version共同分类当前aggregate；digest通过前不得覆盖canonical文件。
        Path target = checkedMarkerPath(record);
        Path source = record.auxiliaryPath().map(this::checkedTransferPath).orElseThrow(() ->
                new DictionaryRecoveryException("IMPORT marker has no source path"));
        TablespaceFileIdentity identity = record.fileIdentity().orElseThrow(() ->
                new DictionaryRecoveryException("IMPORT marker has no file identity"));
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException("IMPORT marker target table is absent"));
        validateBinding(table, record);
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();
        if (table.state() == TableState.DISCARDED) {
            if (phase != DdlLogPhase.PREPARED
                    || table.version().value() >= record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException("IMPORT DISCARDED phase/version mismatch");
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.SOURCE);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            return;
        }

        // 2、pending先证明中间aggregate，再幂等完成物理import并推进ENGINE_DONE。
        if (table.state() == TableState.IMPORT_PENDING) {
            if (table.version().value() != record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException("IMPORT_PENDING version mismatch");
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.INTERMEDIATE);
            if (phase == DdlLogPhase.PREPARED) {
                repository.ddlLog().transition(
                        ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
                phase = DdlLogPhase.DICTIONARY_COMMITTED;
            }
            if (phase != DdlLogPhase.DICTIONARY_COMMITTED
                    && phase != DdlLogPhase.ENGINE_DONE) {
                throw new DictionaryRecoveryException("IMPORT_PENDING phase mismatch: " + phase);
            }
            if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
                physical.importTablespace(
                        table.storageBinding().orElseThrow(), source, identity, timeout);
                repository.ddlLog().transition(
                        ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            } else if (!Files.exists(target)) {
                throw new DictionaryRecoveryException(
                        "IMPORT ENGINE_DONE canonical file is absent: ddl=" + ddlId.value());
            }

            // 3、target固定使用marker+1版本，保证与PREPARED中持久化的target digest一致。
            DictionaryVersion targetVersion = DictionaryVersion.of(
                    record.marker().dictionaryVersion() + 1);
            TableDefinition active = lifecycle(table, targetVersion, TableState.ACTIVE);
            commitRecoveredTable(active);
            requireSchemaCheckpoint(record, active, SchemaCheckpoint.TARGET);
            cache.publishTable(active);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            return;
        }

        // 4、ACTIVE target只允许补terminal；其它状态或版本说明出现不可解释第三态。
        if (table.state() != TableState.ACTIVE
                || table.version().value() != record.marker().dictionaryVersion() + 1
                || phase != DdlLogPhase.ENGINE_DONE) {
            throw new DictionaryRecoveryException(
                    "IMPORT target state/version/phase mismatch: state=" + table.state()
                            + " phase=" + phase);
        }
        requireSchemaCheckpoint(record, table, SchemaCheckpoint.TARGET);
        cache.publishTable(table);
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
    }

    /**
     * 恢复 CREATE INDEX：旧 DD 回滚 staged segments，新 DD 保留 binding 并只清 footer/补 marker。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 marker 对应 ACTIVE table，校验受控 path/space，并按 secondary id 同时定位逻辑 index 与物理 binding。</li>
     *     <li>读取可选 page3 descriptor并与 marker 的 ddl/version/table/index identity 交叉校验；任何错配阻止 OPEN。</li>
     *     <li>DD 不含 index 时只接受 PREPARED/ENGINE_DONE：有 descriptor 就完整 drop 两个 segment，无则直接回滚 marker。</li>
     *     <li>DD 含 exact index/binding 时只接受 ENGINE_DONE/DICTIONARY_COMMITTED，按 DD 真相补 phase、发布 cache并清 footer。</li>
     *     <li>descriptor 收敛后写 COMMITTED；SDI 仍由 recover() 后续 ACTIVE reconcile 以最新 DD 修复。</li>
     * </ol>
     *
     * @param record 非终态 CREATE_INDEX marker
     * @param timeout segment rollback 或 footer clear 的正有界持久化时限
     * @throws DictionaryRecoveryException DD/marker/footer/binding/phase 组合不可安全解释时抛出
     */
    private void recoverCreateIndex(DdlLogRecord record, Duration timeout) {
        boolean onlineProtocol = record.executionProtocol() == DdlExecutionProtocol.ONLINE_INDEX_V1
                || record.executionProtocol() == DdlExecutionProtocol.LEGACY_PHASE_ONLY
                && record.auxiliaryPath().isPresent();
        if (onlineProtocol) {
            if (record.auxiliaryPath().isEmpty()) {
                throw new DictionaryRecoveryException(
                        "online CREATE INDEX protocol lacks row-log identity: ddl="
                                + record.marker().ddlOperationId());
            }
            if (onlineIndexRuntime == null) {
                throw new DictionaryRecoveryException(
                        "online CREATE INDEX marker requires recovery runtime: ddl="
                                + record.marker().ddlOperationId());
            }
            recoverOnlineCreateIndex(record, timeout);
            return;
        }
        // 1. CREATE INDEX 修改既有表，不允许 table/binding/path 在 durable catalog 中消失或换绑。
        checkedMarkerPath(record);
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException("CREATE INDEX marker target table is absent: ddl="
                        + record.marker().ddlOperationId() + " table=" + tableId.value()));
        if (table.state() != TableState.ACTIVE) {
            throw new DictionaryRecoveryException("CREATE INDEX target table is not ACTIVE: table="
                    + tableId.value() + " state=" + table.state());
        }
        validateBinding(table, record);
        var tableBinding = table.storageBinding().orElseThrow();
        var logicalIndex = table.indexes().stream()
                .filter(index -> index.id().value() == record.secondaryObjectId()).findFirst();
        var physicalIndex = tableBinding.indexes().stream()
                .filter(index -> index.indexId() == record.secondaryObjectId()).findFirst();
        if (logicalIndex.isPresent() != physicalIndex.isPresent()) {
            throw new DictionaryRecoveryException(
                    "CREATE INDEX logical/physical binding presence mismatch: index="
                            + record.secondaryObjectId());
        }
        // index presence、version与phase先把当前aggregate分类为source/target；digest只证明该分类内容未漂移。
        if (logicalIndex.isEmpty()) {
            if (table.version().value() >= record.marker().dictionaryVersion()
                    || record.phase() == DdlLogPhase.DICTIONARY_COMMITTED) {
                throw new DictionaryRecoveryException(
                        "CREATE INDEX marker claims/loses committed dictionary index: ddl="
                                + record.marker().ddlOperationId());
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.SOURCE);
        } else {
            if (table.version().value() < record.marker().dictionaryVersion()
                    || record.phase() == DdlLogPhase.PREPARED) {
                throw new DictionaryRecoveryException(
                        "committed CREATE INDEX has impossible table version/phase: ddl="
                                + record.marker().ddlOperationId());
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.TARGET);
        }
        // 2. footer 只证明 staged 资源所有权；每个 identity 都必须与 marker 完全一致。
        Optional<SecondaryIndexBuildDescriptor> descriptor;
        try {
            descriptor = physical.readSecondaryIndexBuild(tableBinding);
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "read CREATE INDEX build descriptor failed: table=" + tableId.value(), failure);
        }
        if (descriptor.isPresent()) {
            SecondaryIndexBuildDescriptor staged = descriptor.orElseThrow();
            if (staged.ddlOperationId() != record.marker().ddlOperationId()
                    || staged.dictionaryVersion() != record.marker().dictionaryVersion()
                    || staged.tableId() != tableId.value()
                    || staged.indexBinding().indexId() != record.secondaryObjectId()) {
                throw new DictionaryRecoveryException(
                        "CREATE INDEX marker/build descriptor identity mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
        }

        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        if (logicalIndex.isEmpty()) {
            // 3. 目标 table aggregate 没有新 index，说明 DD 提交点未越过；ENGINE_DONE 也只能回收物理 build。
            if (descriptor.isPresent()) {
                try {
                    physical.rollbackSecondaryIndexBuild(
                            tableBinding, descriptor.orElseThrow(), timeout);
                } catch (RuntimeException failure) {
                    throw new DictionaryRecoveryException(
                            "rollback CREATE INDEX staged resources failed: ddl=" + ddlId.value(), failure);
                }
            }
            repository.ddlLog().transition(ddlId, record.phase(), DdlLogPhase.ROLLED_BACK);
            cache.restoreTableAfterDdlRollback(
                    table, DictionaryVersion.of(record.marker().dictionaryVersion()));
            log.info("rolled back CREATE INDEX during recovery: table={} index={} ddlId={}",
                    tableId.value(), record.secondaryObjectId(), ddlId.value());
            return;
        }

        // 4. 新 index 已进入 committed DD；binding 必须与 footer 的 root/segments 相同，root level 可由构建后刷新。
        IndexStorageBinding committedBinding = physicalIndex.orElseThrow();
        if (descriptor.isPresent() && !samePhysicalIndex(
                descriptor.orElseThrow().indexBinding(), committedBinding)) {
            throw new DictionaryRecoveryException(
                    "committed CREATE INDEX binding differs from build descriptor: ddl=" + ddlId.value());
        }
        DdlLogPhase phase = record.phase();
        if (phase == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        if (phase != DdlLogPhase.DICTIONARY_COMMITTED) {
            throw new DictionaryRecoveryException(
                    "committed CREATE INDEX has unsupported phase: ddl=" + ddlId.value()
                            + " phase=" + phase);
        }
        cache.publishTable(table);
        if (descriptor.isPresent()) {
            try {
                physical.clearSecondaryIndexBuild(tableBinding, descriptor.orElseThrow(), timeout);
            } catch (RuntimeException failure) {
                throw new DictionaryRecoveryException(
                        "clear committed CREATE INDEX descriptor failed: ddl=" + ddlId.value(), failure);
            }
        }

        // 5. terminal marker 晚于 footer clear；崩溃后重复进入时 descriptor 为空但 DD/binding 足以继续终结。
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        log.info("finished CREATE INDEX during recovery: table={} index={} ddlId={}",
                tableId.value(), record.secondaryObjectId(), ddlId.value());
    }

    /**
     * 同步恢复带受控 row-log 的 Online CREATE INDEX。恢复在 StorageEngine recovery 完成且用户流量尚未开放时
     * 执行，因此 PREPARED 可以丢弃旧 generation 后从空 staged tree 重扫；ENGINE_DONE 则只能验证并前滚。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 marker、ACTIVE DD 与 page3 descriptor 三方交叉校验 table/index/build identity；失败不释放资源。</li>
     *     <li>从 build id 推导受控 row-log 路径，校验 header/manifest/version/row format；ENGINE_DONE 的损坏证据
     *     一律 fail-closed，只有旧 DD+PREPARED 允许精确回滚。</li>
     *     <li>旧 DD+PREPARED 若已有 abort 则回收 staged；否则回收旧 generation、截断日志并从聚簇树有界重建。</li>
     *     <li>ENGINE_DONE 或刚重建完成时验证 durable RECONCILED，再按 SDI→DD→cache→footer 顺序发布 target。</li>
     *     <li>新 DD 已存在时验证 exact definition/binding，并只补齐 phase、cache 与 footer；终态后关闭并删除日志。</li>
     * </ol>
     *
     * @param record auxiliary path 指向 build-owned row-log 的非终态 CREATE_INDEX marker
     * @param timeout 批次重建、WAL/表空间 force、SDI/footer 持久化的正等待上界
     * @throws DictionaryRecoveryException 任一身份、持久证据或阶段组合不能唯一裁决时抛出并阻止实例 OPEN
     */
    private void recoverOnlineCreateIndex(DdlLogRecord record, Duration timeout) {
        // 1. 表空间路径与 ACTIVE aggregate 是稳定锚点；page3 只证明尚未发布资源的物理所有权。
        checkedMarkerPath(record);
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        OnlineIndexBuildId buildId = OnlineIndexBuildId.of(ddlId.value());
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException("online CREATE INDEX target table is absent: ddl="
                        + ddlId.value() + " table=" + tableId.value()));
        if (table.state() != TableState.ACTIVE) {
            throw new DictionaryRecoveryException(
                    "online CREATE INDEX target table is not ACTIVE: table=" + tableId.value()
                            + " state=" + table.state());
        }
        validateBinding(table, record);
        TableStorageBinding oldOrPublishedBinding = table.storageBinding().orElseThrow();
        Optional<IndexDefinition> logicalIndex = table.indexes().stream()
                .filter(index -> index.id().value() == record.secondaryObjectId()).findFirst();
        Optional<IndexStorageBinding> physicalIndex = oldOrPublishedBinding.indexes().stream()
                .filter(index -> index.indexId() == record.secondaryObjectId()).findFirst();
        if (logicalIndex.isPresent() != physicalIndex.isPresent()) {
            throw new DictionaryRecoveryException(
                    "online CREATE INDEX logical/physical binding presence mismatch: index="
                            + record.secondaryObjectId());
        }
        // descriptor与row-log读取之前先完成source/target分类和内容证明，mismatch必须保留全部现场。
        if (logicalIndex.isEmpty()) {
            if (table.version().value() >= record.marker().dictionaryVersion()
                    || record.phase() == DdlLogPhase.DICTIONARY_COMMITTED) {
                throw new DictionaryRecoveryException(
                        "online CREATE INDEX source DD/version/phase is inconsistent: ddl="
                                + ddlId.value());
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.SOURCE);
        } else {
            if (table.version().value() != record.marker().dictionaryVersion()
                    || record.phase() == DdlLogPhase.PREPARED) {
                throw new DictionaryRecoveryException(
                        "online CREATE INDEX target DD/version/phase is inconsistent: ddl="
                                + ddlId.value());
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.TARGET);
        }
        Optional<SecondaryIndexBuildDescriptor> descriptor = readOnlineBuildDescriptor(
                oldOrPublishedBinding, record, tableId);
        Path rowLogPath = record.auxiliaryPath().orElseThrow();
        if (record.executionProtocol() == DdlExecutionProtocol.ONLINE_INDEX_V1) {
            if (logicalIndex.isPresent() && record.controlState() != DdlControlState.FORWARD_ONLY) {
                throw new DictionaryRecoveryException(
                        "published online CREATE INDEX lacks FORWARD_ONLY fence: ddl="
                                + ddlId.value());
            }
            if (logicalIndex.isEmpty()
                    && record.controlState() == DdlControlState.CANCEL_REQUESTED) {
                rollbackOnlinePrepared(record, table, oldOrPublishedBinding, descriptor, timeout);
                onlineIndexRuntime.logFiles().delete(buildId, rowLogPath);
                return;
            }
        }

        // 2. row-log 是 PREPARED 重建命令和 ENGINE_DONE 完整性的第二份 durable 证据。
        FileOnlineIndexChangeLog changeLog;
        try {
            changeLog = onlineIndexRuntime.logFiles().open(buildId, rowLogPath);
        } catch (RuntimeException openFailure) {
            if (logicalIndex.isPresent()) {
                finishPublishedOnlineIndexWithoutRowLog(
                        record, table, oldOrPublishedBinding, descriptor,
                        logicalIndex.orElseThrow(), physicalIndex.orElseThrow(), timeout);
                onlineIndexRuntime.logFiles().delete(buildId, rowLogPath);
                log.warn("finished committed online CREATE INDEX despite unreadable terminal row-log: ddl={}",
                        ddlId.value());
                return;
            }
            if (record.phase() == DdlLogPhase.PREPARED && logicalIndex.isEmpty()
                    && record.controlState() != DdlControlState.FORWARD_ONLY) {
                rollbackOnlinePrepared(record, table, oldOrPublishedBinding, descriptor, timeout);
                onlineIndexRuntime.logFiles().delete(buildId, rowLogPath);
                log.warn("rolled back PREPARED online CREATE INDEX with unreadable row-log: ddl={}",
                        ddlId.value());
                return;
            }
            throw new DictionaryRecoveryException(
                    "open online CREATE INDEX row-log failed after forward-only boundary: ddl="
                            + ddlId.value(), openFailure);
        }

        boolean deleteLog = false;
        try {
            OnlineIndexBuildManifest manifest = validateOnlineManifest(
                    record, table, oldOrPublishedBinding, changeLog.header());

            // 3. 旧 DD 尚未引用 staged segments；PREPARED 可安全重建或按 durable abort 回滚。
            if (logicalIndex.isEmpty()) {
                if (table.version().value() != manifest.sourceVersion().value()
                        || physicalIndex.isPresent()
                        || record.phase() == DdlLogPhase.DICTIONARY_COMMITTED) {
                    throw new DictionaryRecoveryException(
                            "online CREATE INDEX old DD/version/phase is inconsistent: ddl="
                                    + ddlId.value());
                }
                if (record.phase() == DdlLogPhase.PREPARED && changeLog.abortRequired()) {
                    if (record.controlState() == DdlControlState.FORWARD_ONLY) {
                        throw new DictionaryRecoveryException(
                                "online CREATE INDEX row-log requests abort after FORWARD_ONLY: ddl="
                                        + ddlId.value());
                    }
                    rollbackOnlinePrepared(record, table, oldOrPublishedBinding, descriptor, timeout);
                    deleteLog = true;
                } else {
                    SecondaryIndexBuildDescriptor staged = descriptor.orElse(null);
                    if (record.phase() == DdlLogPhase.PREPARED) {
                        staged = rebuildOnlinePrepared(record, table, oldOrPublishedBinding,
                                staged, manifest, changeLog, timeout);
                    } else if (record.phase() != DdlLogPhase.ENGINE_DONE) {
                        throw new DictionaryRecoveryException(
                                "online CREATE INDEX old DD has unsupported phase: ddl="
                                        + ddlId.value() + " phase=" + record.phase());
                    }
                    if (staged == null || !hasDurableReconciled(changeLog)) {
                        throw new DictionaryRecoveryException(
                                "online CREATE INDEX ENGINE_DONE lacks descriptor/RECONCILED evidence: ddl="
                                        + ddlId.value());
                    }
                    publishRecoveredOnlineIndex(record, table, oldOrPublishedBinding,
                            staged, manifest, timeout);
                    deleteLog = true;
                }
            } else {
                // 4. 新 DD 是越过提交点的权威事实；只能验证 exact target 并补齐后置清理，禁止 staged rollback。
                finishPublishedOnlineIndex(record, table, oldOrPublishedBinding,
                        descriptor, logicalIndex.orElseThrow(), physicalIndex.orElseThrow(),
                        manifest, changeLog, timeout);
                deleteLog = true;
            }
        } catch (DictionaryRecoveryException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "recover online CREATE INDEX failed: ddl=" + ddlId.value(), failure);
        } finally {
            // 5. FileChannel 不跨恢复步骤泄漏；只有前述状态完全收敛才在 close 后删除 Windows 上的日志文件。
            changeLog.close();
        }
        if (deleteLog) {
            onlineIndexRuntime.logFiles().delete(buildId, rowLogPath);
        }
    }

    /** 读取 page3 owner 并与 durable marker 的全部稳定 identity 交叉校验。 */
    private Optional<SecondaryIndexBuildDescriptor> readOnlineBuildDescriptor(
            TableStorageBinding binding, DdlLogRecord record, TableId tableId) {
        Optional<SecondaryIndexBuildDescriptor> descriptor;
        try {
            descriptor = physical.readSecondaryIndexBuild(binding);
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "read online CREATE INDEX descriptor failed: table=" + tableId.value(), failure);
        }
        descriptor.ifPresent(staged -> {
            if (staged.ddlOperationId() != record.marker().ddlOperationId()
                    || staged.dictionaryVersion() != record.marker().dictionaryVersion()
                    || staged.tableId() != tableId.value()
                    || staged.indexBinding().indexId() != record.secondaryObjectId()) {
                throw new DictionaryRecoveryException(
                        "online CREATE INDEX marker/descriptor identity mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
        });
        return descriptor;
    }

    /** 解码 manifest，并拒绝 marker/header/DD 任一身份或版本分叉。 */
    private static OnlineIndexBuildManifest validateOnlineManifest(
            DdlLogRecord record, TableDefinition table, TableStorageBinding binding,
            OnlineIndexLogHeader header) {
        OnlineIndexBuildManifest manifest;
        try {
            manifest = new OnlineIndexBuildManifestCodec().decode(header.manifest());
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "decode online CREATE INDEX manifest failed: ddl="
                            + record.marker().ddlOperationId(), failure);
        }
        if (header.buildId().value() != record.marker().ddlOperationId()
                || header.tableId() != table.id().value()
                || header.indexId() != record.secondaryObjectId()
                || header.targetDictionaryVersion() != record.marker().dictionaryVersion()
                || header.rowFormatVersion() != binding.rowFormatVersion()
                || manifest.buildId().value() != header.buildId().value()
                || manifest.tableId().value() != header.tableId()
                || manifest.sourceVersion().value() != header.sourceDictionaryVersion()
                || manifest.targetVersion().value() != header.targetDictionaryVersion()
                || manifest.index().id().value() != header.indexId()) {
            throw new DictionaryRecoveryException(
                    "online CREATE INDEX marker/header/manifest identity mismatch: ddl="
                            + record.marker().ddlOperationId());
        }
        return manifest;
    }

    /** PREPARED 尚未越过物理完成点时，精确回收 footer/segments 并发布 ROLLED_BACK。 */
    private void rollbackOnlinePrepared(DdlLogRecord record, TableDefinition table,
                                        TableStorageBinding binding,
                                        Optional<SecondaryIndexBuildDescriptor> descriptor,
                                        Duration timeout) {
        if (record.phase() != DdlLogPhase.PREPARED) {
            throw new DictionaryRecoveryException(
                    "only PREPARED online CREATE INDEX can roll back: ddl="
                            + record.marker().ddlOperationId());
        }
        descriptor.ifPresent(staged -> physical.rollbackSecondaryIndexBuild(binding, staged, timeout));
        repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()),
                DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
        cache.restoreTableAfterDdlRollback(
                table, DictionaryVersion.of(record.marker().dictionaryVersion()));
    }

    /**
     * 用户流量关闭期把 PREPARED 恢复为全新 generation：旧 tree 与 candidate 都丢弃，随后只从当前聚簇
     * committed truth 有界重建，最后持久化 RECONCILED 并推进 ENGINE_DONE。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 page3 descriptor 精确回收旧 staged segments，不让失败 generation 与新树共享 owner。</li>
     *     <li>截断 row-log 到 immutable manifest，force 后建立新的 GENERATION_STARTED/CAPTURING 区间。</li>
     *     <li>创建新 staged tree 并分批扫描当前聚簇真相；每批释放页资源后再进入下一批。</li>
     *     <li>封闭 generation，执行双向验证与数据/row-log force，最后才把 marker 推进 ENGINE_DONE。</li>
     * </ol>
     */
    private SecondaryIndexBuildDescriptor rebuildOnlinePrepared(
            DdlLogRecord record, TableDefinition table, TableStorageBinding binding,
            SecondaryIndexBuildDescriptor previous, OnlineIndexBuildManifest manifest,
            FileOnlineIndexChangeLog changeLog, Duration timeout) {
        // 1. 旧 descriptor 可能只包含半次 base scan；先完整回收，避免两个 generation 共享 segment owner。
        if (previous != null) {
            physical.rollbackSecondaryIndexBuild(binding, previous, timeout);
        }
        // 2. truncate+force 后旧 candidate 不再属于新 tree；状态 frame 证明当前 generation 已开始。
        changeLog.resetToManifest(timeout);
        changeLog.appendState(OnlineIndexLogRecordType.GENERATION_STARTED, new byte[0]);
        long capturing = changeLog.appendState(OnlineIndexLogRecordType.CAPTURING, new byte[0]);
        changeLog.forceThrough(capturing, timeout);

        // 3. 新 root/segments 与 footer durable 后，按 exclusive continuation 批次重建且批次间不保留页资源。
        StorageTableDefinition definition = storageDefinition(table, manifest.index());
        SecondaryIndexBuildDescriptor staged = physical.beginSecondaryIndexBuild(
                binding, record.marker().ddlOperationId(), manifest.targetVersion().value(),
                storageIndex(manifest.index()), timeout);
        Optional<cn.zhangyis.db.storage.record.page.SearchKey> continuation = Optional.empty();
        while (true) {
            OnlineIndexScanBatch batch = physical.scanSecondaryIndexBuildBatch(
                    definition, binding, staged, continuation,
                    onlineIndexRuntime.config().scanBatchRows());
            for (var row : batch.rows()) {
                staged = physical.ensureSecondaryIndexLiveForBaseScan(
                        definition, binding, staged, row);
            }
            if (batch.complete()) {
                break;
            }
            continuation = batch.continuation();
        }

        // 4. 恢复期没有并发 DML；仍显式封闭 generation，再做双向验证、WAL/file force 与 RECONCILED force。
        long sealed = changeLog.appendState(OnlineIndexLogRecordType.SEALED, new byte[0]);
        changeLog.forceThrough(sealed, timeout);
        physical.verifySecondaryIndexBuild(
                definition, binding, staged, onlineIndexRuntime.config().scanBatchRows());
        physical.makeSecondaryIndexBuildDurable(binding, Lsn.of(0), timeout);
        ensureRecoveryForwardOnly(record);
        long reconciled = changeLog.appendState(OnlineIndexLogRecordType.RECONCILED, new byte[0]);
        changeLog.forceThrough(reconciled, timeout);
        repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()),
                DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
        return staged;
    }

    /**
     * 在恢复重建已经完成物理验证与force之后持久封闭取消窗口；legacy marker保持旧phase-only协议。
     *
     * @param record 本轮恢复开始时分类为source/PREPARED的完整marker
     * @throws DictionaryRecoveryException cancel意外胜出或CAS观察到不可解释状态时抛出并保留物理证据
     */
    private void ensureRecoveryForwardOnly(DdlLogRecord record) {
        if (record.executionProtocol() == DdlExecutionProtocol.LEGACY_PHASE_ONLY
                || record.controlState() == DdlControlState.FORWARD_ONLY) {
            return;
        }
        if (record.controlState() == DdlControlState.CANCEL_REQUESTED) {
            throw new DictionaryRecoveryException(
                    "online CREATE INDEX cancellation became durable during recovery: ddl="
                            + record.marker().ddlOperationId());
        }
        DdlControlCasResult result = repository.ddlLog().compareAndSetControl(
                DdlId.of(record.marker().ddlOperationId()), DdlLogPhase.PREPARED,
                DdlControlState.OPEN, DdlControlState.FORWARD_ONLY, Optional.empty());
        if (!result.changed()
                && result.observedRecord().controlState() != DdlControlState.FORWARD_ONLY) {
            throw new DictionaryRecoveryException(
                    "online CREATE INDEX forward fence CAS lost during recovery: ddl="
                            + record.marker().ddlOperationId() + " control="
                            + result.observedRecord().controlState());
        }
    }

    /** 检查当前 generation 的 RECONCILED 后存在覆盖其 sequence 的 durable force watermark。 */
    private static boolean hasDurableReconciled(FileOnlineIndexChangeLog changeLog) {
        List<cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecord> records = changeLog.readAll();
        long generation = records.stream().mapToLong(
                cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecord::generation).max().orElse(0);
        long reconciled = records.stream()
                .filter(record -> record.generation() == generation
                        && record.type() == OnlineIndexLogRecordType.RECONCILED)
                .mapToLong(cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecord::sequence)
                .max().orElse(0);
        if (reconciled == 0 || changeLog.abortRequired()) {
            return false;
        }
        return records.stream().anyMatch(record -> record.generation() == generation
                && record.type() == OnlineIndexLogRecordType.FORCE_WATERMARK
                && record.sequence() > reconciled
                && forceTarget(record.payload()) >= reconciled);
    }

    /** FORCE_WATERMARK payload 固定为一个 big-endian target sequence；其它长度不构成恢复证据。 */
    private static long forceTarget(byte[] payload) {
        if (payload.length != Long.BYTES) {
            return 0;
        }
        return ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    /**
     * ENGINE_DONE+旧 DD 的唯一合法动作是把 manifest target 与 staged binding 原子发布为新 aggregate。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>重新双向验证 staged tree 并强制数据文件，防止仅凭 marker 发布不完整物理结果。</li>
     *     <li>从旧 aggregate 精确追加 manifest index/binding，先写 SDI 再提交目标 DD version。</li>
     *     <li>推进 DICTIONARY_COMMITTED、发布 cache，并按 exact descriptor 清除 page3 build footer。</li>
     *     <li>物理与逻辑发布均完成后写 COMMITTED；任一步失败保留非终态 marker 供下次恢复。</li>
     * </ol>
     */
    private void publishRecoveredOnlineIndex(
            DdlLogRecord record, TableDefinition table, TableStorageBinding binding,
            SecondaryIndexBuildDescriptor staged, OnlineIndexBuildManifest manifest,
            Duration timeout) {
        // 1. ENGINE_DONE 仍不是 catalog 发布证据；先验证并 force 当前 staged tree。
        StorageTableDefinition definition = storageDefinition(table, manifest.index());
        physical.verifySecondaryIndexBuild(
                definition, binding, staged, onlineIndexRuntime.config().scanBatchRows());
        physical.makeSecondaryIndexBuildDurable(binding, Lsn.of(0), timeout);

        // 2. 只从恢复 manifest 冻结的 index 构造 exact 新 aggregate，维持 SDI 先于 DD 的发布顺序。
        List<IndexDefinition> indexes = new ArrayList<>(table.indexes());
        indexes.add(manifest.index());
        List<IndexStorageBinding> bindings = new ArrayList<>(binding.indexes());
        bindings.add(staged.indexBinding());
        TableStorageBinding publishedBinding = new TableStorageBinding(
                binding.tableId(), binding.spaceId(), binding.path(), binding.rowFormatVersion(),
                bindings, binding.lobSegment());
        TableDefinition published = new TableDefinition(
                table.id(), table.schemaId(), table.name(), manifest.targetVersion(), TableState.ACTIVE,
                table.columns(), indexes, Optional.of(publishedBinding), table.options());
        sdi.write(published, timeout);
        cache.invalidateTable(table.id(), manifest.targetVersion());
        commitRecoveredTable(published);

        // 3. DD 已提交后只允许前滚：发布 cache，并清除不再需要的 page3 构建 owner。
        repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()),
                DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
        cache.publishTable(published);
        physical.clearSecondaryIndexBuild(binding, staged, timeout);

        // 4. 所有可观察发布结果已经一致，最后写 terminal marker；失败时下一次恢复继续前滚。
        repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()),
                DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        log.info("published online CREATE INDEX during recovery: table={} index={} ddl={}",
                table.id().value(), manifest.index().id().value(), record.marker().ddlOperationId());
    }

    /**
     * 新 DD 已 durable 时验证 exact target，并幂等补齐 phase、cache、footer 与 terminal marker。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>交叉校验 manifest、DD version、logical index、physical binding 与 RECONCILED force 证据。</li>
     *     <li>若 marker 仍为 ENGINE_DONE，则只向前补写 DICTIONARY_COMMITTED。</li>
     *     <li>发布 committed table cache，并按 exact descriptor 幂等清除 page3 footer。</li>
     *     <li>最后写 COMMITTED；验证或清理失败时保持 marker，禁止回滚已提交 DD。</li>
     * </ol>
     */
    private void finishPublishedOnlineIndex(
            DdlLogRecord record, TableDefinition table, TableStorageBinding binding,
            Optional<SecondaryIndexBuildDescriptor> descriptor, IndexDefinition logicalIndex,
            IndexStorageBinding physicalIndex, OnlineIndexBuildManifest manifest,
            FileOnlineIndexChangeLog changeLog, Duration timeout) {
        // 1. committed DD 只有与 durable row-log、manifest 和物理 binding 全部一致时才可完成恢复。
        if (table.version().value() != manifest.targetVersion().value()
                || !logicalIndex.equals(manifest.index())
                || record.phase() == DdlLogPhase.PREPARED
                || !hasDurableReconciled(changeLog)) {
            throw new DictionaryRecoveryException(
                    "published online CREATE INDEX does not match durable target: ddl="
                            + record.marker().ddlOperationId());
        }
        if (descriptor.isPresent()
                && !samePhysicalIndex(descriptor.orElseThrow().indexBinding(), physicalIndex)) {
            throw new DictionaryRecoveryException(
                    "published online CREATE INDEX binding differs from descriptor: ddl="
                            + record.marker().ddlOperationId());
        }
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();

        // 2. DD 已存在时 ENGINE_DONE 只能前滚，绝不因 phase 落后撤销 catalog。
        if (phase == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        if (phase != DdlLogPhase.DICTIONARY_COMMITTED) {
            throw new DictionaryRecoveryException(
                    "published online CREATE INDEX has unsupported phase: ddl="
                            + ddlId.value() + " phase=" + phase);
        }

        // 3. 发布 exact committed cache，并在 descriptor 存在时精确清除 footer。
        cache.publishTable(table);
        if (descriptor.isPresent()) {
            physical.clearSecondaryIndexBuild(binding, descriptor.orElseThrow(), timeout);
        }

        // 4. cache/footer 均已收敛后写 terminal marker；row-log 删除由外层统一清理。
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
    }

    /**
     * committed DD 已包含目标时，row-log 不再拥有提交裁决权。恢复仍用 marker version、exact DD binding 和可选
     * page3 descriptor 验证物理结果；验证通过后补齐清理，避免日志介质损坏反向删除已提交索引。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 committed DD version/index identity，并用可选 descriptor 交叉核对 physical binding。</li>
     *     <li>从已发布 binding 派生旧 aggregate，按 committed DD 的目标树执行完整双向物理验证。</li>
     *     <li>把 ENGINE_DONE 幂等推进到 DICTIONARY_COMMITTED，随后发布 cache 并清除可选 footer。</li>
     *     <li>最后写 COMMITTED；损坏 row-log 仅失去辅助证据，不得推翻已验证的 committed DD。</li>
     * </ol>
     */
    private void finishPublishedOnlineIndexWithoutRowLog(
            DdlLogRecord record, TableDefinition table, TableStorageBinding publishedBinding,
            Optional<SecondaryIndexBuildDescriptor> descriptor, IndexDefinition logicalIndex,
            IndexStorageBinding physicalIndex, Duration timeout) {
        // 1. 只有 marker 与 committed DD 精确指向同一目标时，才允许绕过不可读 row-log。
        if (table.version().value() != record.marker().dictionaryVersion()
                || logicalIndex.id().value() != record.secondaryObjectId()
                || record.phase() == DdlLogPhase.PREPARED) {
            throw new DictionaryRecoveryException(
                    "unreadable row-log cannot be ignored for uncommitted online CREATE INDEX: ddl="
                            + record.marker().ddlOperationId());
        }
        descriptor.ifPresent(staged -> {
            if (!samePhysicalIndex(staged.indexBinding(), physicalIndex)) {
                throw new DictionaryRecoveryException(
                        "committed online index binding differs from descriptor: ddl="
                                + record.marker().ddlOperationId());
            }
        });

        // 2. 去掉目标 binding 得到构建前 aggregate，再以 committed tree 执行 source/target 双向验证。
        List<IndexStorageBinding> oldIndexes = publishedBinding.indexes().stream()
                .filter(index -> index.indexId() != record.secondaryObjectId()).toList();
        TableStorageBinding oldBinding = new TableStorageBinding(
                publishedBinding.tableId(), publishedBinding.spaceId(), publishedBinding.path(),
                publishedBinding.rowFormatVersion(), oldIndexes, publishedBinding.lobSegment());
        SecondaryIndexBuildDescriptor verificationDescriptor = descriptor.orElseGet(() ->
                new SecondaryIndexBuildDescriptor(
                        record.marker().ddlOperationId(), record.marker().dictionaryVersion(),
                        table.id().value(), physicalIndex));
        physical.verifySecondaryIndexBuild(
                storageDefinition(table), oldBinding, verificationDescriptor,
                onlineIndexRuntime.config().scanBatchRows());

        // 3. DD 已发布后只补齐 phase、cache 和 footer，任何失败都保留可重试非终态 marker。
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();
        if (phase == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        if (phase != DdlLogPhase.DICTIONARY_COMMITTED) {
            throw new DictionaryRecoveryException(
                    "committed online CREATE INDEX has unsupported phase: ddl="
                            + ddlId.value() + " phase=" + phase);
        }
        cache.publishTable(table);
        if (descriptor.isPresent()) {
            physical.clearSecondaryIndexBuild(
                    publishedBinding, descriptor.orElseThrow(), timeout);
        }

        // 4. exact 物理结果和 cache 已收敛后写 terminal；外层随后精确删除损坏日志文件。
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
    }

    /** 把 DD column/index definition 映射为恢复期 staged scan 的 exact storage schema。 */
    private static StorageTableDefinition storageDefinition(
            TableDefinition table, IndexDefinition newIndex) {
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "online CREATE INDEX table has no storage binding: " + table.id().value()));
        List<StorageColumnDefinition> columns = table.columns().stream().map(column ->
                new StorageColumnDefinition(
                        column.columnId(), column.name().displayName(), column.ordinal(),
                        storageType(column.type()))).toList();
        List<StorageIndexDefinition> indexes = new ArrayList<>();
        table.indexes().stream().map(DictionaryDdlRecoveryService::storageIndex).forEach(indexes::add);
        indexes.add(storageIndex(newIndex));
        return new StorageTableDefinition(
                table.id().value(), binding.spaceId(), binding.path(), binding.rowFormatVersion(),
                PageNo.of(1), columns, indexes,
                hasAutoIncrement(table));
    }

    /** 把已经发布全部 indexes 的 DD aggregate 映射为物理验证 schema。 */
    private static StorageTableDefinition storageDefinition(TableDefinition table) {
        TableStorageBinding binding = table.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "online CREATE INDEX table has no storage binding: " + table.id().value()));
        List<StorageColumnDefinition> columns = table.columns().stream().map(column ->
                new StorageColumnDefinition(
                        column.columnId(), column.name().displayName(), column.ordinal(),
                        storageType(column.type()))).toList();
        return new StorageTableDefinition(
                table.id().value(), binding.spaceId(), binding.path(), binding.rowFormatVersion(),
                PageNo.of(1), columns,
                table.indexes().stream().map(DictionaryDdlRecoveryService::storageIndex).toList(),
                hasAutoIncrement(table));
    }

    /** @return exact DD aggregate 是否声明唯一 AUTO_INCREMENT 列。 */
    private static boolean hasAutoIncrement(TableDefinition table) {
        return table.columns().stream().anyMatch(column ->
                column.generation()
                        == cn.zhangyis.db.dd.domain.ColumnGeneration.AUTO_INCREMENT);
    }

    /** 保持 DD type 的 nullable/length/collation/symbol 语义，不在恢复期推断或升级行格式。 */
    private static StorageColumnType storageType(ColumnTypeDefinition type) {
        return new StorageColumnType(StorageColumnTypeId.valueOf(type.typeId().name()), type.nullable(),
                type.length(), type.scale(), type.unsigned(), type.charsetId(), type.collationId(), type.symbols());
    }

    /** 保持 index id/name/unique/key order/prefix 的完整持久定义。 */
    private static StorageIndexDefinition storageIndex(IndexDefinition index) {
        return new StorageIndexDefinition(index.id().value(), index.name().displayName(),
                index.unique(), index.clustered(), index.keyParts().stream()
                .map(part -> new StorageIndexKeyPart(
                        part.columnId(), part.order() == cn.zhangyis.db.dd.domain.IndexOrder.ASC
                        ? StorageIndexOrder.ASC : StorageIndexOrder.DESC,
                        part.prefixBytes())).toList());
    }

    /**
     * 恢复 DROP INDEX：旧 DD 仍含目标时只回滚 descriptor，新 DD 已删除目标时完成 segment 回收。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 marker 对应 ACTIVE table，校验 path/space，并按 secondary id 同时定位当前逻辑定义与物理 binding。</li>
     *     <li>读取可选 DROP descriptor，与 marker 的 ddl/version/table/index identity 交叉校验；旧 DD 存在目标时
     *     还必须精确等于其 committed binding。</li>
     *     <li>旧 DD 仍含目标只接受 PREPARED：有 descriptor 就只清 footer，不释放 segment，随后写 ROLLED_BACK。</li>
     *     <li>目标已从精确新版本 DD 消失时接受 PREPARED/DICTIONARY_COMMITTED/ENGINE_DONE；PREPARED 覆盖
     *     “DD durable、phase append 未完成”窗口，先补字典阶段再发布 cache 和完成物理回收。</li>
     *     <li>descriptor 已随 segment 原子清除时跳过物理动作，补 ENGINE_DONE/COMMITTED；任何不可解释组合阻止 OPEN。</li>
     * </ol>
     *
     * @param record 非终态 DROP_INDEX marker
     * @param timeout footer rollback 或 segment finish 的正有界持久化时限
     * @throws DictionaryRecoveryException DD/marker/footer/binding/phase 组合无法安全裁决时抛出
     */
    private void recoverDropIndex(DdlLogRecord record, Duration timeout) {
        if (record.executionProtocol() == DdlExecutionProtocol.ONLINE_DROP_INDEX_V1) {
            recoverOnlineDropIndex(record, timeout);
            return;
        }
        // 1. DROP INDEX 不改变 table/space/path；当前 aggregate 必须仍是同一 ACTIVE 物理表。
        checkedMarkerPath(record);
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException("DROP INDEX marker target table is absent: ddl="
                        + record.marker().ddlOperationId() + " table=" + tableId.value()));
        if (table.state() != TableState.ACTIVE) {
            throw new DictionaryRecoveryException("DROP INDEX target table is not ACTIVE: table="
                    + tableId.value() + " state=" + table.state());
        }
        validateBinding(table, record);
        var tableBinding = table.storageBinding().orElseThrow();
        var logicalIndex = table.indexes().stream()
                .filter(index -> index.id().value() == record.secondaryObjectId()).findFirst();
        var physicalIndex = tableBinding.indexes().stream()
                .filter(index -> index.indexId() == record.secondaryObjectId()).findFirst();
        if (logicalIndex.isPresent() != physicalIndex.isPresent()) {
            throw new DictionaryRecoveryException(
                    "DROP INDEX logical/physical binding presence mismatch: index="
                            + record.secondaryObjectId());
        }
        if (logicalIndex.isPresent() && logicalIndex.orElseThrow().clustered()) {
            throw new DictionaryRecoveryException(
                    "DROP INDEX marker targets clustered index: " + record.secondaryObjectId());
        }
        // DROP的source仍包含目标index，target已删除它；version/phase完成分类后才读取或清理descriptor。
        if (logicalIndex.isPresent()) {
            if (record.phase() != DdlLogPhase.PREPARED
                    || table.version().value() >= record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException(
                        "uncommitted DROP INDEX has impossible table version/phase: ddl="
                                + record.marker().ddlOperationId());
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.SOURCE);
        } else {
            if (table.version().value() != record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException(
                        "committed DROP INDEX dictionary version mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.TARGET);
        }

        // 2. descriptor 只授予当前 marker 处理精确 segment 的权限，任何 identity 错配都不得继续启动。
        Optional<SecondaryIndexDropDescriptor> descriptor;
        try {
            descriptor = physical.readSecondaryIndexDrop(tableBinding);
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "read DROP INDEX descriptor failed: table=" + tableId.value(), failure);
        }
        if (descriptor.isPresent()) {
            SecondaryIndexDropDescriptor staged = descriptor.orElseThrow();
            if (staged.ddlOperationId() != record.marker().ddlOperationId()
                    || staged.dictionaryVersion() != record.marker().dictionaryVersion()
                    || staged.tableId() != tableId.value()
                    || staged.indexBinding().indexId() != record.secondaryObjectId()) {
                throw new DictionaryRecoveryException(
                        "DROP INDEX marker/descriptor identity mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
        }

        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        if (logicalIndex.isPresent()) {
            // 3. 旧 DD 是未越过提交点的权威证据；rollback 只能清 descriptor，索引必须继续可达。
            if (descriptor.isPresent()
                    && !descriptor.orElseThrow().indexBinding().equals(physicalIndex.orElseThrow())) {
                throw new DictionaryRecoveryException(
                        "DROP INDEX descriptor differs from committed old binding: ddl="
                                + ddlId.value());
            }
            if (descriptor.isPresent()) {
                try {
                    physical.rollbackSecondaryIndexDrop(
                            tableBinding, descriptor.orElseThrow(), timeout);
                } catch (RuntimeException failure) {
                    throw new DictionaryRecoveryException(
                            "rollback DROP INDEX descriptor failed: ddl=" + ddlId.value(), failure);
                }
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            cache.restoreTableAfterDdlRollback(
                    table, DictionaryVersion.of(record.marker().dictionaryVersion()));
            log.info("rolled back DROP INDEX during recovery: table={} index={} ddlId={}",
                    tableId.value(), record.secondaryObjectId(), ddlId.value());
            return;
        }

        // 4. 目标缺失只有在 table 精确进入 marker version 时才证明 DD commit；PREPARED 允许 marker append 丢响应窗口。
        DdlLogPhase phase = record.phase();
        if (phase == DdlLogPhase.PREPARED) {
            if (descriptor.isEmpty()) {
                throw new DictionaryRecoveryException(
                        "committed DROP INDEX PREPARED marker has no physical descriptor: ddl="
                                + ddlId.value());
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        if (phase != DdlLogPhase.DICTIONARY_COMMITTED && phase != DdlLogPhase.ENGINE_DONE) {
            throw new DictionaryRecoveryException(
                    "committed DROP INDEX has unsupported phase: ddl=" + ddlId.value()
                            + " phase=" + phase);
        }
        cache.publishTable(table);
        if (phase == DdlLogPhase.ENGINE_DONE) {
            if (descriptor.isPresent()) {
                throw new DictionaryRecoveryException(
                        "DROP INDEX ENGINE_DONE still owns a physical descriptor: ddl="
                                + ddlId.value());
            }
        } else {
            // descriptor 为空表示 segment/footer 原子 MTR 已完成、仅 ENGINE_DONE append 尚未发生。
            if (descriptor.isPresent()) {
                try {
                    physical.finishSecondaryIndexDrop(
                            tableBinding, descriptor.orElseThrow(), timeout);
                } catch (RuntimeException failure) {
                    throw new DictionaryRecoveryException(
                            "finish DROP INDEX physical reclaim failed: ddl=" + ddlId.value(), failure);
                }
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
        }

        // 5. terminal marker 晚于 segment/footer 收敛；重复启动只会从 ENGINE_DONE 补这一条 transition。
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
        log.info("finished DROP INDEX during recovery: table={} index={} ddlId={}",
                tableId.value(), record.secondaryObjectId(), ddlId.value());
    }

    /**
     * 恢复ONLINE_DROP_INDEX_V1。source DD在OPEN/CANCEL时回滚，在FORWARD_ONLY时按marker派生并发布target；
     * target DD只等待持久retirement fence后回收segment，绝不反向恢复索引可见性。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验protocol、retirement协作者、ACTIVE table/path/space与logical/physical index presence，按当前DD分类source/target。</li>
     *     <li>读取page3 descriptor并与marker identity交叉验证；需要前滚的分支还必须验证一次性fence的owner、source version、
     *     descriptor generation与唯一INDEX资源。</li>
     *     <li>source+OPEN/CANCEL只清descriptor并写ROLLED_BACK；source+FORWARD从旧aggregate精确删除同ordinal definition/binding，
     *     校验target digest后提交target SDI/DD。</li>
     *     <li>target PREPARED补DICTIONARY_COMMITTED并发布cache；若descriptor仍在，则从恢复重建的history与空/重建cache等待fence后回收。</li>
     *     <li>descriptor absence视为segment/footer单MTR已经完成，随后只补ENGINE_DONE/COMMITTED；第三态始终fail-closed。</li>
     * </ol>
     *
     * @param record 非终态ONLINE_DROP_INDEX_V1 marker
     * @param timeout SDI、descriptor与retirement等待的正有界时限
     * @throws DictionaryRecoveryException marker/DD/descriptor/fence无法唯一分类，或前滚资源未安全时抛出并阻止OPEN
     */
    private void recoverOnlineDropIndex(DdlLogRecord record, Duration timeout) {
        // 1. Online DROP恢复必须使用事务恢复完成后重建的barrier；缺失协作者不能退化为blocking回收。
        if (indexRetirementBarrier == null) {
            throw new DictionaryRecoveryException(
                    "online DROP INDEX recovery has no retirement barrier: ddl="
                            + record.marker().ddlOperationId());
        }
        checkedMarkerPath(record);
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition current = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException(
                        "online DROP INDEX target table is absent: ddl="
                                + record.marker().ddlOperationId()));
        if (current.state() != TableState.ACTIVE) {
            throw new DictionaryRecoveryException(
                    "online DROP INDEX target table is not ACTIVE: table=" + tableId.value());
        }
        validateBinding(current, record);
        TableStorageBinding currentBinding = current.storageBinding().orElseThrow();
        int logicalOrdinal = -1;
        for (int ordinal = 0; ordinal < current.indexes().size(); ordinal++) {
            if (current.indexes().get(ordinal).id().value() == record.secondaryObjectId()) {
                logicalOrdinal = ordinal;
                break;
            }
        }
        int physicalOrdinal = -1;
        for (int ordinal = 0; ordinal < currentBinding.indexes().size(); ordinal++) {
            if (currentBinding.indexes().get(ordinal).indexId() == record.secondaryObjectId()) {
                physicalOrdinal = ordinal;
                break;
            }
        }
        if ((logicalOrdinal >= 0) != (physicalOrdinal >= 0) || logicalOrdinal != physicalOrdinal) {
            throw new DictionaryRecoveryException(
                    "online DROP INDEX logical/physical ordinal presence mismatch: ddl="
                            + record.marker().ddlOperationId());
        }
        boolean sourcePresent = logicalOrdinal >= 0;
        if (sourcePresent && current.indexes().get(logicalOrdinal).clustered()) {
            throw new DictionaryRecoveryException(
                    "online DROP INDEX marker targets clustered index: "
                            + record.secondaryObjectId());
        }
        if (sourcePresent) {
            if (record.phase() != DdlLogPhase.PREPARED
                    || current.version().value() >= record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException(
                        "online DROP INDEX source version/phase is impossible: ddl="
                                + record.marker().ddlOperationId());
            }
            requireSchemaCheckpoint(record, current, SchemaCheckpoint.SOURCE);
        } else {
            if (current.version().value() != record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException(
                        "online DROP INDEX target version mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
            requireSchemaCheckpoint(record, current, SchemaCheckpoint.TARGET);
        }

        // 2. descriptor/fence共同授权旧segment；任何owner、generation或resource错绑都保留资源并阻止启动。
        Optional<SecondaryIndexDropDescriptor> descriptor;
        try {
            descriptor = physical.readSecondaryIndexDrop(currentBinding);
        } catch (RuntimeException failure) {
            throw new DictionaryRecoveryException(
                    "read online DROP INDEX descriptor failed: table=" + tableId.value(), failure);
        }
        if (descriptor.isPresent()) {
            SecondaryIndexDropDescriptor staged = descriptor.orElseThrow();
            if (staged.ddlOperationId() != record.marker().ddlOperationId()
                    || staged.dictionaryVersion() != record.marker().dictionaryVersion()
                    || staged.tableId() != tableId.value()
                    || staged.indexBinding().indexId() != record.secondaryObjectId()) {
                throw new DictionaryRecoveryException(
                        "online DROP INDEX marker/descriptor identity mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
            if (sourcePresent && !staged.indexBinding()
                    .equals(currentBinding.indexes().get(physicalOrdinal))) {
                throw new DictionaryRecoveryException(
                        "online DROP INDEX descriptor differs from source binding: ddl="
                                + record.marker().ddlOperationId());
            }
        }
        boolean mustForward = record.controlState() == DdlControlState.FORWARD_ONLY;
        DdlRetirementFence fence = null;
        if (mustForward || !sourcePresent) {
            fence = requireOnlineDropFence(record, descriptor);
            if (sourcePresent && fence.sourceDictionaryVersion() != current.version().value()) {
                throw new DictionaryRecoveryException(
                        "online DROP INDEX fence/source version mismatch: ddl="
                                + record.marker().ddlOperationId());
            }
        } else if (record.retirementFence().isPresent()) {
            // OPEN/CANCEL可能崩溃在install fence与direction CAS之间；仍校验证据但方向保持可回滚。
            fence = requireOnlineDropFence(record, descriptor);
        }

        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        if (sourcePresent && !mustForward) {
            // 3. OPEN/CANCEL没有越过不可回退点；descriptor缺失覆盖prepare后、stage前崩溃窗口。
            if (descriptor.isPresent()) {
                try {
                    physical.rollbackSecondaryIndexDrop(
                            currentBinding, descriptor.orElseThrow(), timeout);
                } catch (RuntimeException failure) {
                    throw new DictionaryRecoveryException(
                            "rollback online DROP INDEX descriptor failed: ddl="
                                    + ddlId.value(), failure);
                }
            }
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            cache.restoreTableAfterDdlRollback(
                    current, DictionaryVersion.of(record.marker().dictionaryVersion()));
            log.info("rolled back online DROP INDEX during recovery: table={} index={} ddlId={}",
                    tableId.value(), record.secondaryObjectId(), ddlId.value());
            return;
        }

        if (sourcePresent) {
            // FORWARD_ONLY必须已有descriptor/fence；从source exact ordinal派生target，不依赖进程内plan或旧cache。
            if (descriptor.isEmpty() || fence == null) {
                throw new DictionaryRecoveryException(
                        "online DROP INDEX forward source lacks descriptor/fence: ddl="
                                + ddlId.value());
            }
            List<IndexDefinition> targetIndexes = new ArrayList<>(current.indexes());
            targetIndexes.remove(logicalOrdinal);
            List<IndexStorageBinding> targetBindings = new ArrayList<>(currentBinding.indexes());
            targetBindings.remove(physicalOrdinal);
            TableStorageBinding targetBinding = new TableStorageBinding(
                    currentBinding.tableId(), currentBinding.spaceId(), currentBinding.path(),
                    currentBinding.rowFormatVersion(), targetBindings, currentBinding.lobSegment());
            TableDefinition target = new TableDefinition(
                    current.id(), current.schemaId(), current.name(),
                    DictionaryVersion.of(record.marker().dictionaryVersion()), TableState.ACTIVE,
                    current.columns(), targetIndexes, Optional.of(targetBinding), current.options());
            requireSchemaCheckpoint(record, target, SchemaCheckpoint.TARGET);
            sdi.write(target, timeout);
            cache.invalidateTable(current.id(), target.version());
            commitRecoveredTable(target);
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            cache.publishTable(target);
            current = target;
            currentBinding = targetBinding;
        }

        // 4. target DD只接受FORWARD_ONLY；PREPARED覆盖DD durable后phase append丢失，descriptor absence覆盖cleanup已提交。
        if (record.controlState() != DdlControlState.FORWARD_ONLY || fence == null) {
            throw new DictionaryRecoveryException(
                    "online DROP INDEX target lacks FORWARD_ONLY/fence: ddl=" + ddlId.value());
        }
        DdlLogPhase phase = sourcePresent
                ? DdlLogPhase.DICTIONARY_COMMITTED : record.phase();
        if (phase == DdlLogPhase.PREPARED) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        if (phase != DdlLogPhase.DICTIONARY_COMMITTED && phase != DdlLogPhase.ENGINE_DONE) {
            throw new DictionaryRecoveryException(
                    "online DROP INDEX target phase is unsupported: ddl="
                            + ddlId.value() + " phase=" + phase);
        }
        cache.publishTable(current);
        if (phase == DdlLogPhase.ENGINE_DONE) {
            if (descriptor.isPresent()) {
                throw new DictionaryRecoveryException(
                        "online DROP INDEX ENGINE_DONE still owns descriptor: ddl="
                                + ddlId.value());
            }
        } else if (descriptor.isPresent()) {
            try {
                indexRetirementBarrier.awaitIndexSafe(fence, timeout);
                physical.finishSecondaryIndexDrop(
                        currentBinding, descriptor.orElseThrow(), timeout);
            } catch (RuntimeException failure) {
                throw new DictionaryRecoveryException(
                        "finish online DROP INDEX retirement failed: ddl="
                                + ddlId.value(), failure);
            }
        }

        // 5. footer absence是物理单MTR的幂等完成证据；只补phase，不重新访问已经释放的segment identity。
        if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
        }
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
        log.info("finished online DROP INDEX during recovery: table={} index={} ddlId={}",
                tableId.value(), record.secondaryObjectId(), ddlId.value());
    }

    /** 校验单Online DROP marker中的fence与可选descriptor属于同一index/owner/generation。 */
    private static DdlRetirementFence requireOnlineDropFence(
            DdlLogRecord record, Optional<SecondaryIndexDropDescriptor> descriptor) {
        DdlRetirementFence fence = record.retirementFence().orElseThrow(() ->
                new DictionaryRecoveryException(
                        "online DROP INDEX marker has no retirement fence: ddl="
                                + record.marker().ddlOperationId()));
        boolean resourceMatches = fence.resources().size() == 1
                && fence.resources().getFirst().kind() == DdlRetiredResourceKind.INDEX
                && fence.resources().getFirst().resourceId() == record.secondaryObjectId();
        boolean descriptorMatches = descriptor.isEmpty()
                || fence.descriptorGeneration() == descriptor.orElseThrow().dictionaryVersion();
        if (fence.tableId() != record.marker().affectedObjectId()
                || fence.ownerDdlId() != record.marker().ddlOperationId()
                || fence.sourceMetadataPinVersion() != fence.sourceDictionaryVersion()
                || fence.sourceDictionaryVersion() >= record.marker().dictionaryVersion()
                || fence.descriptorGeneration() != record.marker().dictionaryVersion()
                || !resourceMatches || !descriptorMatches) {
            throw new DictionaryRecoveryException(
                    "online DROP INDEX retirement fence identity mismatch: ddl="
                            + record.marker().ddlOperationId());
        }
        return fence;
    }

    /** root page 与两个 segment 是物理所有权 identity；root level 允许构建期间由 0 增长。 */
    private static boolean samePhysicalIndex(IndexStorageBinding staged, IndexStorageBinding committed) {
        return staged.indexId() == committed.indexId()
                && staged.rootPageId().equals(committed.rootPageId())
                && staged.leafSegment().equals(committed.leafSegment())
                && staged.nonLeafSegment().equals(committed.nonLeafSegment());
    }

    /**
     * 恢复 CREATE：没有 committed table 就精确回滚 marker path；存在 ACTIVE 则验证 binding 后补齐提交阶段。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 marker path 必须与 table/space identity 推导出的受控文件名完全一致。</li>
     *     <li>读取包含非 ACTIVE 生命周期的 recovery table；不存在时只允许 PREPARED/ENGINE_DONE，并删除精确 path。</li>
     *     <li>存在时只接受同版本 ACTIVE，复核 space/path 与文件存在性，禁止用 marker 掩盖 committed table 损坏。</li>
     *     <li>按 DD 已提交这一更强证据补齐 DICTIONARY_COMMITTED、cache 和 COMMITTED。</li>
     * </ol>
     *
     * @param record 非终态 CREATE marker。
     * @throws DictionaryRecoveryException path、DD lifecycle、binding、版本或物理文件与 marker 不一致时抛出。
     */
    private void recoverCreate(DdlLogRecord record) {
        // 1、即使 catalog payload 位于受控目录，文件名也必须与 marker identity 唯一对应。
        Path path = checkedMarkerPath(record);
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        Optional<TableDefinition> found = repository.findTableForRecovery(tableId);

        // 2、没有 committed DD 就没有 CREATE 提交裁决点；清理 exact path 后终止为 ROLLED_BACK。
        if (found.isEmpty()) {
            if (record.phase() == DdlLogPhase.DICTIONARY_COMMITTED) {
                throw new DictionaryRecoveryException("CREATE marker claims committed DD but table is absent: ddl="
                        + record.marker().ddlOperationId() + " table=" + tableId.value());
            }
            delete(path, "rolled-back CREATE tablespace");
            repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()),
                    record.phase(), DdlLogPhase.ROLLED_BACK);
            return;
        }

        // 3、ACTIVE DD 是 CREATE 成功的唯一字典裁决；identity 不一致时不得删除或继续开放流量。
        TableDefinition active = found.orElseThrow();
        if (active.state() != TableState.ACTIVE
                || active.version().value() != record.marker().dictionaryVersion()) {
            throw new DictionaryRecoveryException("CREATE marker does not match ACTIVE dictionary version: ddl="
                    + record.marker().ddlOperationId() + " tableState=" + active.state()
                    + " tableVersion=" + active.version().value());
        }
        validateBinding(active, record);
        if (!Files.exists(path)) {
            throw new DictionaryRecoveryException("committed CREATE tablespace is missing: table="
                    + tableId.value() + " path=" + path);
        }
        if (record.phase() == DdlLogPhase.PREPARED) {
            throw new DictionaryRecoveryException("CREATE PREPARED marker cannot coexist with ACTIVE DD: ddl="
                    + record.marker().ddlOperationId());
        }
        requireSchemaCheckpoint(record, active, SchemaCheckpoint.TARGET);

        // 4、ENGINE_DONE 后 ACTIVE DD 已证明 dictionary commit；补 marker、cache，再写不可推进终态。
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        if (record.phase() == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(
                    ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.DICTIONARY_COMMITTED);
        }
        cache.publishTable(active);
        repository.ddlLog().transition(
                ddlId, DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.COMMITTED);
        log.info("recovered CREATE DDL marker: table={} ddlId={}", tableId.value(), ddlId.value());
    }

    /**
     * 恢复 DROP：ACTIVE+PREPARED 回滚；DROP_PENDING 续作；DROPPED 以更强 DD 证据补齐 marker。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 marker 对应的 committed table 并验证稳定 binding；DROP 不允许目标表从 durable history 消失。</li>
     *     <li>ACTIVE 仅与 PREPARED 组合合法，此时未越过 DROP_PENDING 提交点，直接写 ROLLED_BACK。</li>
     *     <li>DROP_PENDING 必须使用 marker dictionary version，随后通过统一 finish 流程复核 barrier、删除和发布。</li>
     *     <li>DROPPED 证明字典与物理流程均越过提交点，按合法直接后继补写到 COMMITTED。</li>
     * </ol>
     *
     * @param record 非终态 DROP marker。
     * @param timeout DROP_PENDING finish 的正有界等待时间。
     * @throws DictionaryRecoveryException table/binding/version/phase 组合不可解释时抛出。
     */
    private void recoverDrop(DdlLogRecord record, Duration timeout) {
        // 1、DROP 的 durable marker 总是引用一个先前 committed table；缺失或换绑均属于 catalog 损坏。
        checkedMarkerPath(record);
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException("DROP marker target table is absent: ddl="
                        + record.marker().ddlOperationId() + " table=" + tableId.value()));
        validateBinding(table, record);

        // 2、PREPARED+ACTIVE 表示 DROP_PENDING 没有提交，物理文件仍属于原表，只回滚 marker。
        if (table.state() == TableState.ACTIVE) {
            if (record.phase() != DdlLogPhase.PREPARED) {
                throw new DictionaryRecoveryException("DROP marker crossed commit phase but DD is ACTIVE: ddl="
                        + record.marker().ddlOperationId() + " phase=" + record.phase());
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.SOURCE);
            repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()),
                    DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            return;
        }

        // 3、DROP_PENDING 版本就是 marker 的字典提交点；统一 finish 负责阶段补写与资源边界。
        if (table.state() == TableState.DROP_PENDING) {
            if (table.version().value() != record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException("DROP_PENDING version does not match marker: ddl="
                        + record.marker().ddlOperationId() + " markerVersion="
                        + record.marker().dictionaryVersion() + " tableVersion=" + table.version().value());
            }
            requireSchemaCheckpoint(record, table, SchemaCheckpoint.INTERMEDIATE);
            completePendingDrop(table, timeout, Optional.of(record));
            return;
        }

        // 4、DROPPED 是比中间 marker 更强的 durable 证据；按状态机逐边补写，不跳过审计阶段。
        if (table.version().value() <= record.marker().dictionaryVersion()) {
            throw new DictionaryRecoveryException("DROPPED version must advance beyond DROP marker: ddl="
                    + record.marker().ddlOperationId());
        }
        requireSchemaCheckpoint(record, table, SchemaCheckpoint.TARGET);
        finishDroppedMarker(record);
    }

    /**
     * 对已经 durable 的 DROPPED 表按 operation 状态机逐边补齐终态。
     *
     * @param record 当前非终态 DROP marker。
     * @throws DictionaryRecoveryException record 阶段不能沿 DROP 状态机解释时抛出并阻止 OPEN。
     */
    private void finishDroppedMarker(DdlLogRecord record) {
        DdlId ddlId = DdlId.of(record.marker().ddlOperationId());
        DdlLogPhase phase = record.phase();
        if (phase == DdlLogPhase.PREPARED) {
            repository.ddlLog().transition(ddlId, phase, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
            repository.ddlLog().transition(ddlId, phase, DdlLogPhase.ENGINE_DONE);
            phase = DdlLogPhase.ENGINE_DONE;
        }
        if (phase != DdlLogPhase.ENGINE_DONE) {
            throw new DictionaryRecoveryException("DROPPED table has unsupported DDL phase: ddl="
                    + ddlId.value() + " phase=" + phase);
        }
        repository.ddlLog().transition(ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
    }

    /**
     * 续作一张 DROP_PENDING 表。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验 pending storage binding/path；有 marker 时先用 DD 证据把 PREPARED 补到 DICTIONARY_COMMITTED。</li>
     *     <li>在物理删除前等待恢复出的 persistent history 引用归零；超时保持 DROP_PENDING 和文件。</li>
     *     <li>文件仍存在时调用 storage DDL facade；删除收敛后把 marker 补到 ENGINE_DONE。</li>
     *     <li>有marker时使用live已预留的pending+1版本；只有无marker legacy pending才预留新版本并发布DROPPED。</li>
     *     <li>有 marker 时从 ENGINE_DONE 写入 COMMITTED；任一 append outcome 不确定都由下次启动重建裁决。</li>
     * </ol>
     *
     * @param pending committed catalog 中状态为 DROP_PENDING、仍携带 durable storage binding 的表版本。
     * @param timeout 当前恢复阶段为 barrier 与物理 DROP 提供的正有界等待时间。
     * @param logged  新格式 DROP marker；旧 catalog recovery 传空并保留原 lifecycle 兼容路径。
     * @throws DictionaryRecoveryException binding/path 缺失、barrier 等待失败、物理删除或 DROPPED publish 失败时抛出。
     */
    private void completePendingDrop(TableDefinition pending, Duration timeout, Optional<DdlLogRecord> logged) {
        // 1、DROP_PENDING binding 在物理删除前仍是 purge/rollback resolver 的权威 metadata。
        var binding = pending.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException("DROP_PENDING table has no storage binding: "
                        + pending.id().value()));
        Path path = discovery.checkedPath(binding.path());
        DdlId ddlId = logged.map(record -> DdlId.of(record.marker().ddlOperationId())).orElse(null);
        DdlLogPhase phase = logged.map(DdlLogRecord::phase).orElse(null);
        if (phase == DdlLogPhase.PREPARED) {
            repository.ddlLog().transition(ddlId, phase, DdlLogPhase.DICTIONARY_COMMITTED);
            phase = DdlLogPhase.DICTIONARY_COMMITTED;
        }
        if (phase != null && phase != DdlLogPhase.DICTIONARY_COMMITTED
                && phase != DdlLogPhase.ENGINE_DONE) {
            throw new DictionaryRecoveryException("DROP_PENDING table has unsupported DDL phase: ddl="
                    + ddlId.value() + " phase=" + phase);
        }

        // 2、即使 storage RESUME_PURGE 已运行，也必须按当前恢复投影再次复核，禁止越过残留引用。
        purgeBarrier.awaitUnreferenced(pending.id().value(), timeout);

        // 3、崩溃前文件可能已经删除；存在时才续作幂等物理 DROP，随后 durable 记录物理阶段。
        if (phase == DdlLogPhase.ENGINE_DONE && Files.exists(path)) {
            throw new DictionaryRecoveryException("DROP ENGINE_DONE marker still has physical file: ddl="
                    + ddlId.value() + " path=" + path);
        }
        if (Files.exists(path)) {
            try {
                physical.dropTable(binding, timeout);
            } catch (RuntimeException e) {
                throw new DictionaryRecoveryException("resume physical DROP failed: table="
                        + pending.id().value(), e);
            }
        }
        if (phase == DdlLogPhase.DICTIONARY_COMMITTED) {
            repository.ddlLog().transition(ddlId, phase, DdlLogPhase.ENGINE_DONE);
            phase = DdlLogPhase.ENGINE_DONE;
        }

        // 4、有marker时target版本已经参与digest并在live reserve中固定；legacy pending才需要恢复期新版本。
        DictionaryVersion version = logged
                .map(record -> DictionaryVersion.of(record.marker().dictionaryVersion() + 1))
                .orElseGet(() -> DictionaryVersion.of(control.reserve(
                        new DictionaryIdRequest(0, 0, 0, 0, 0, 1)).dictionaryVersion()));
        TableDefinition dropped = new TableDefinition(pending.id(), pending.schemaId(), pending.name(), version,
                TableState.DROPPED, pending.columns(), pending.indexes(), pending.storageBinding(),
                pending.options());
        try (DictionaryTransaction transaction = repository.begin(version)) {
            transaction.updateTable(dropped);
            transaction.commit();
        }
        logged.ifPresent(record ->
                requireSchemaCheckpoint(record, dropped, SchemaCheckpoint.TARGET));
        cache.invalidateTable(pending.id(), version);
        // 5、terminal marker 晚于 DROPPED publish；若 append 报错，下一次 recovery 从 DD truth 继续终结。
        if (phase == DdlLogPhase.ENGINE_DONE) {
            repository.ddlLog().transition(ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
        }
        log.info("recovered pending DROP: table={} ddlId={} version={}", pending.id().value(),
                ddlId == null ? "legacy" : ddlId.value(), version.value());
    }

    /**
     * 校验 marker 只能指向由自身 table/space identity 推导出的受控 file-per-table 路径。
     *
     * @param record 待用于文件裁决的 durable marker。
     * @return 已通过目录边界与 exact filename 双重校验的规范路径。
     * @throws DictionaryRecoveryException 路径越界或 identity 与文件名不一致时抛出。
     */
    private Path checkedMarkerPath(DdlLogRecord record) {
        Path actual = discovery.checkedPath(record.path());
        Path expected = tablesDirectory.resolve("table_" + record.marker().affectedObjectId()
                + "_space_" + record.spaceId().value() + ".ibd").toAbsolutePath().normalize();
        if (!actual.equals(expected)) {
            throw new DictionaryRecoveryException("DDL marker path does not match table/space identity: ddl="
                    + record.marker().ddlOperationId() + " actual=" + actual + " expected=" + expected);
        }
        return actual;
    }

    /**
     * 校验 committed table 的稳定物理 binding 与 marker 完全一致。
     *
     * @param table marker 对应的 recovery table 版本。
     * @param record 携带预留 space/path 的 durable DDL marker。
     * @throws DictionaryRecoveryException binding 缺失、space 或受控路径不一致时抛出。
     */
    private void validateBinding(TableDefinition table, DdlLogRecord record) {
        var binding = table.storageBinding().orElseThrow(() ->
                new DictionaryRecoveryException("DDL marker table has no storage binding: "
                        + table.id().value()));
        Path boundPath = discovery.checkedPath(binding.path());
        Path markerPath = checkedMarkerPath(record);
        if (!binding.spaceId().equals(record.spaceId()) || !boundPath.equals(markerPath)) {
            throw new DictionaryRecoveryException("DDL marker does not match table storage binding: ddl="
                    + record.marker().ddlOperationId() + " table=" + table.id().value());
        }
    }

    /**
     * 清理 committed DROPPED 表空间残留与未被 ACTIVE/DROP_PENDING binding 引用的受控命名 CREATE orphan。
     *
     * @throws DictionaryRecoveryException 路径越界、目录扫描或文件删除失败时抛出，恢复保持 fail-closed。
     */
    private void cleanupDroppedAndOrphans() {
        Set<Path> live = new HashSet<>();
        for (TableDefinition table : repository.snapshot().tables().values()) {
            table.storageBinding().ifPresent(binding -> {
                Path path = discovery.checkedPath(binding.path());
                if (table.state() == TableState.ACTIVE || table.state() == TableState.DROP_PENDING
                        || table.state() == TableState.RECOVERY_UNAVAILABLE
                        || table.state() == TableState.RECOVERY_DISCARDED) {
                    live.add(path);
                } else {
                    delete(path, "DROPPED tablespace residue");
                }
            });
        }
        if (!Files.exists(tablesDirectory)) {
            return;
        }
        try (DirectoryStream<Path> files = Files.newDirectoryStream(tablesDirectory, "table_*_space_*.ibd")) {
            for (Path file : files) {
                Path normalized = discovery.checkedPath(file);
                if (!live.contains(normalized)) {
                    delete(normalized, "uncommitted CREATE orphan");
                }
            }
        } catch (IOException e) {
            throw new DictionaryRecoveryException("scan dictionary tables directory failed: " + tablesDirectory, e);
        }
    }

    /**
     * 清理受控 online-ddl 目录中的无 marker manifest 孤儿和 terminal row-log。任何 page3 owner 或非终态
     * marker 都优先于文件名，不能因目录扫描而释放仍可恢复的物理证据。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从最新 DD snapshot 的可打开 binding 读取全部 page3 build owner，形成 ddl id 集合。</li>
     *     <li>按受控文件工厂枚举 exact build id，不接受任意路径、symlink 或非规范数字文件名。</li>
     *     <li>无 marker 且无 descriptor 时按“manifest 先于 marker”窗口删除；存在 descriptor 则 fail-closed。</li>
     *     <li>terminal online marker 必须路径一致且 descriptor 已清，随后幂等删除；其它组合阻止 OPEN。</li>
     * </ol>
     *
     * @throws DictionaryRecoveryException 文件与 marker/page3 owner 不能唯一对应或精确删除失败时抛出
     */
    private void cleanupOnlineIndexLogs() {
        if (onlineIndexRuntime == null) {
            return;
        }
        List<OnlineIndexBuildId> existingLogs = onlineIndexRuntime.logFiles().existingBuildIds();
        if (existingLogs.isEmpty()) {
            return;
        }
        // 1. footer 是 staged segment owner；即使 marker 缺失，也不得把其 row-log 当普通孤儿删除。
        Set<Long> descriptorOwners = new HashSet<>();
        for (TableDefinition table : repository.snapshot().tables().values()) {
            if (table.state() != TableState.ACTIVE || table.storageBinding().isEmpty()) {
                continue;
            }
            try {
                physical.readSecondaryIndexBuild(table.storageBinding().orElseThrow())
                        .ifPresent(descriptor -> descriptorOwners.add(descriptor.ddlOperationId()));
            } catch (RuntimeException failure) {
                throw new DictionaryRecoveryException(
                        "scan online CREATE INDEX descriptor owner failed: table="
                                + table.id().value(), failure);
            }
        }

        // 2. factory 已把目录约束与 exact filename 收口；本层只裁决 DD owner，不接收路径输入。
        for (OnlineIndexBuildId buildId : existingLogs) {
            Optional<DdlLogRecord> marker = repository.ddlLog().find(DdlId.of(buildId.value()));
            Path path = onlineIndexRuntime.logFiles().pathFor(buildId);

            // 3. marker 前崩溃只留下 durable manifest；没有 page3 owner 才能证明该文件不可达。
            if (marker.isEmpty()) {
                if (descriptorOwners.contains(buildId.value())) {
                    throw new DictionaryRecoveryException(
                            "online index row-log has descriptor owner but no DDL marker: build="
                                    + buildId.value());
                }
                onlineIndexRuntime.logFiles().delete(buildId, path);
                log.warn("deleted ownerless online CREATE INDEX manifest: build={}", buildId.value());
                continue;
            }

            // 4. 只有同一 online CREATE_INDEX 的 terminal history 可留下待重试删除的文件。
            DdlLogRecord record = marker.orElseThrow();
            if (record.operation() != DdlLogOperation.CREATE_INDEX
                    || record.auxiliaryPath().isEmpty()
                    || !record.auxiliaryPath().orElseThrow().equals(path)
                    || !record.phase().terminal()
                    || descriptorOwners.contains(buildId.value())) {
                throw new DictionaryRecoveryException(
                        "online index row-log still has non-terminal or mismatched owner: build="
                                + buildId.value() + " phase=" + record.phase());
            }
            onlineIndexRuntime.logFiles().delete(buildId, path);
        }
    }

    /**
     * 幂等删除一个已经过受控路径校验的表空间文件，并保留删除原因用于恢复日志。
     *
     * @param path   待删除的规范化受控文件路径。
     * @param reason DROPPED residue 或 uncommitted CREATE orphan 的诊断分类。
     * @throws DictionaryRecoveryException 文件删除失败时抛出并保留 {@link IOException} 根因。
     */
    private static void delete(Path path, String reason) {
        try {
            if (Files.deleteIfExists(path)) {
                log.warn("deleted {}: {}", reason, path);
            }
        } catch (IOException e) {
            throw new DictionaryRecoveryException("delete " + reason + " failed: " + path, e);
        }
    }
}
