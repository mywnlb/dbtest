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
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
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
        if (control == null || repository == null || cache == null || physical == null || tablesDirectory == null
                || purgeBarrier == null) {
            throw new DatabaseValidationException("dictionary DDL recovery collaborators/path must not be null");
        }
        this.control = control;
        this.repository = repository;
        this.cache = cache;
        this.physical = physical;
        this.purgeBarrier = purgeBarrier;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
        this.discovery = new DictionaryTablespaceDiscovery(repository, tablesDirectory);
        this.sdi = new SerializedDictionaryInfoService(physical);
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
            case DROP_TABLE -> recoverDrop(record, timeout);
        }
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
                TableState.DROPPED, pending.columns(), pending.indexes(), pending.storageBinding());
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
                if (table.state() == TableState.ACTIVE || table.state() == TableState.DROP_PENDING) {
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
