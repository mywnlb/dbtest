package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlLogPhase;
import cn.zhangyis.db.dd.ddl.DdlLogRecord;
import cn.zhangyis.db.dd.domain.DdlId;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
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
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.dd.recovery.backup.RecoveryBackupService;
import cn.zhangyis.db.dd.recovery.backup.ValidatedRecoveryBackup;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (control == null || repository == null || cache == null || physical == null || tablesDirectory == null
                || purgeBarrier == null) {
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
            recoverLoggedOperation(record, timeout);
        }

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
            case REBUILD_TABLE -> recoverRebuild(record);
            case DISCARD_RECOVERY_UNAVAILABLE -> recoverUnavailableDiscard(record, timeout);
            case DROP_RECOVERY_UNAVAILABLE -> recoverUnavailableDrop(record, timeout);
            case IMPORT_RECOVERY_REPLACEMENT -> recoverReplacementImport(record, timeout);
        }
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
    private void recoverRebuild(DdlLogRecord record) {
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
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        Path path = checkedMarkerPath(record);
        Path quarantine = record.auxiliaryPath().map(this::checkedTransferPath).orElseThrow(() ->
                new DictionaryRecoveryException("DISCARD marker has no quarantine path"));
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() ->
                new DictionaryRecoveryException("DISCARD marker target table is absent: " + tableId.value()));
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        if (table.state() == TableState.ACTIVE) {
            if (record.phase() != DdlLogPhase.PREPARED) throw new DictionaryRecoveryException("DISCARD ACTIVE phase mismatch");
            repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()), DdlLogPhase.PREPARED, DdlLogPhase.ROLLED_BACK);
            return;
        }
        if (table.state() == TableState.DISCARD_PENDING) {
            if (record.phase() == DdlLogPhase.PREPARED) repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()), DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
            if (Files.exists(path)) physical.discardTablespace(table.storageBinding().orElseThrow(), quarantine, timeout);
            if (repository.ddlLog().find(DdlId.of(record.marker().ddlOperationId())).orElseThrow().phase() == DdlLogPhase.DICTIONARY_COMMITTED)
                repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()), DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0,0,0,0,0,1));
            try (DictionaryTransaction tx = repository.begin(DictionaryVersion.of(ids.dictionaryVersion()))) {
                tx.updateTable(new TableDefinition(table.id(), table.schemaId(), table.name(), DictionaryVersion.of(ids.dictionaryVersion()), TableState.DISCARDED, table.columns(), table.indexes(), table.storageBinding(), table.options())); tx.commit();
            }
            cache.invalidateTable(table.id(), DictionaryVersion.of(ids.dictionaryVersion()));
            repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()), DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            return;
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        if (table.state() == TableState.DISCARDED) finishDroppedMarker(record);
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
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        Path target = checkedMarkerPath(record);
        Path source = record.auxiliaryPath().map(this::checkedTransferPath).orElseThrow(() ->
                new DictionaryRecoveryException("IMPORT marker has no source path"));
        TablespaceFileIdentity identity = record.fileIdentity().orElseThrow(() -> new DictionaryRecoveryException("IMPORT marker has no file identity"));
        TableId tableId = TableId.of(record.marker().affectedObjectId());
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        TableDefinition table = repository.findTableForRecovery(tableId).orElseThrow(() -> new DictionaryRecoveryException("IMPORT marker target table is absent"));
        if (table.state() != TableState.IMPORT_PENDING) { if (table.state() == TableState.ACTIVE) finishDroppedMarker(record); return; }
        if (record.phase() == DdlLogPhase.PREPARED) repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()), DdlLogPhase.PREPARED, DdlLogPhase.DICTIONARY_COMMITTED);
        if (Files.exists(source) && !Files.exists(target)) physical.importTablespace(table.storageBinding().orElseThrow(), source, identity, timeout);
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()), DdlLogPhase.DICTIONARY_COMMITTED, DdlLogPhase.ENGINE_DONE);
        DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0,0,0,0,0,1));
        DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
        TableDefinition active = new TableDefinition(
                table.id(), table.schemaId(), table.name(), version, TableState.ACTIVE,
                table.columns(), table.indexes(), table.storageBinding(), table.options());
        try (DictionaryTransaction tx = repository.begin(version)) {
            tx.updateTable(active);
            tx.commit();
        }
        cache.publishTable(active);
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        repository.ddlLog().transition(DdlId.of(record.marker().ddlOperationId()), DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
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
            if (table.version().value() >= record.marker().dictionaryVersion()
                    || record.phase() == DdlLogPhase.DICTIONARY_COMMITTED) {
                throw new DictionaryRecoveryException(
                        "CREATE INDEX marker claims/loses committed dictionary index: ddl="
                                + ddlId.value());
            }
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
        if (table.version().value() < record.marker().dictionaryVersion()
                || record.phase() == DdlLogPhase.PREPARED) {
            throw new DictionaryRecoveryException(
                    "committed CREATE INDEX has impossible table version/phase: ddl=" + ddlId.value());
        }
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
            if (record.phase() != DdlLogPhase.PREPARED
                    || table.version().value() >= record.marker().dictionaryVersion()) {
                throw new DictionaryRecoveryException(
                        "uncommitted DROP INDEX has impossible table version/phase: ddl="
                                + ddlId.value());
            }
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
        if (table.version().value() != record.marker().dictionaryVersion()) {
            throw new DictionaryRecoveryException(
                    "committed DROP INDEX dictionary version mismatch: ddl=" + ddlId.value()
                            + " tableVersion=" + table.version().value());
        }
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
            completePendingDrop(table, timeout, Optional.of(record));
            return;
        }

        // 4、DROPPED 是比中间 marker 更强的 durable 证据；按状态机逐边补写，不跳过审计阶段。
        if (table.version().value() <= record.marker().dictionaryVersion()) {
            throw new DictionaryRecoveryException("DROPPED version must advance beyond DROP marker: ddl="
                    + record.marker().ddlOperationId());
        }
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
     *     <li>只预留新字典版本并发布 DROPPED，同时失效 cache；恢复不制造没有日志的 DDL id。</li>
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

        // 4、文件已删除后只以单调新版本发布 DROPPED；DDL identity 已由原 marker 拥有，legacy 也不伪造 identity。
        DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 0, 0, 0, 1));
        DictionaryVersion version = DictionaryVersion.of(ids.dictionaryVersion());
        TableDefinition dropped = new TableDefinition(pending.id(), pending.schemaId(), pending.name(), version,
                TableState.DROPPED, pending.columns(), pending.indexes(), pending.storageBinding(),
                pending.options());
        try (DictionaryTransaction transaction = repository.begin(version)) {
            transaction.updateTable(dropped);
            transaction.commit();
        }
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
