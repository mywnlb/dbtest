package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
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
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
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
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexBuildDescriptor;
import cn.zhangyis.db.storage.api.ddl.SecondaryIndexBuildDuplicateKeyException;
import cn.zhangyis.db.storage.api.ddl.IndexStorageBinding;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MDL、ID/version control、持久 DD、cache 与物理 storage DDL 的唯一协调器。锁顺序固定为 schema→table；
 * 等待 MDL 时尚未持有 MTR/page/file 资源，物理 CREATE durable 后才发布 ACTIVE，DROP 则先发布 DROP_PENDING。
 */
@Slf4j
public final class DictionaryDdlService {

    private final DictionaryControlStore control;
    private final PersistentDictionaryRepository repository;
    private final DictionaryObjectCache cache;
    private final MetadataLockManager locks;
    private final TableDdlStorageService physical;
    /** DROP_PENDING 发布前等待 committed history 清零的稳定 storage API。 */
    private final TablePurgeBarrier purgeBarrier;
    private final Path tablesDirectory;
    private final DictionaryDdlFaultInjector faultInjector;
    /** ACTIVE DD 发布前写入完整 table SDI，保证文件冗余先于 catalog 提交 durable。 */
    private final SerializedDictionaryInfoService sdi;

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
                TablePurgeBarrier.NONE, DictionaryDdlFaultInjector.NO_OP);
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
                purgeBarrier, DictionaryDdlFaultInjector.NO_OP);
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
                TablePurgeBarrier.NONE, faultInjector);
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
        if (control == null || repository == null || cache == null || locks == null || physical == null
                || tablesDirectory == null || purgeBarrier == null || faultInjector == null) {
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
    }

    /** 创建 schema；X MDL 覆盖重复名称检查、ID/version 预留与 catalog publish。 */
    public SchemaDefinition createSchema(MdlOwnerId owner, ObjectName name, int charsetId, int collationId,
                                         Duration timeout) {
        validateOwnerTimeout(owner, timeout);
        if (name == null || charsetId <= 0 || collationId <= 0) {
            throw new DatabaseValidationException("create schema name/charset/collation invalid");
        }
        try (MdlTicket ignored = locks.acquire(new MdlRequest(owner, MdlKey.schema(name.canonicalName()),
                MdlMode.EXCLUSIVE, MdlDuration.TRANSACTION), timeout)) {
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
     */
    public TableDefinition createTable(MdlOwnerId owner, CreateTableCommand command, Duration timeout) {
        validateOwnerTimeout(owner, timeout);
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
            // PREPARED 早于物理 CREATE；marker outcome 不确定时立即停止，由恢复按 exact path 裁决。
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), version.value(), tableId.value()),
                    DdlLogOperation.CREATE_TABLE, DdlLogPhase.PREPARED,
                    cn.zhangyis.db.domain.SpaceId.of(ids.firstSpaceId()), path);
            repository.ddlLog().prepare(prepared);
            faultInjector.afterCreatePrepared(prepared);

            // 3、storage 返回代表物理初始化与所需 redo durability 已完成，此后才允许记录 ENGINE_DONE。
            TableStorageBinding binding = physical.createTable(storageRequest);
            DdlLogRecord engineDone = repository.ddlLog().transition(
                    ddlId, DdlLogPhase.PREPARED, DdlLogPhase.ENGINE_DONE);
            faultInjector.afterCreateEngineDone(engineDone);

            // 4、最终 binding 已确定后先写 durable SDI；ACTIVE 仍是字典提交裁决点，SDI 不反向决定 catalog。
            TableDefinition table = new TableDefinition(tableId, schema.id(), command.name().table(), version,
                    TableState.ACTIVE, columns, indexes, java.util.Optional.of(binding));
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
     */
    public TableDefinition createSecondaryIndex(MdlOwnerId owner,
                                                CreateSecondaryIndexCommand command,
                                                Duration timeout) {
        validateOwnerTimeout(owner, timeout);
        if (command == null) {
            throw new DatabaseValidationException("create secondary index command must not be null");
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
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), version.value(), active.id().value()),
                    newIndex.id().value(), DdlLogOperation.CREATE_INDEX, DdlLogPhase.PREPARED,
                    oldBinding.spaceId(), oldBinding.path());
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
            List<IndexDefinition> indexes = new ArrayList<>(active.indexes());
            indexes.add(newIndex);
            List<IndexStorageBinding> bindings = new ArrayList<>(oldBinding.indexes());
            bindings.add(completed);
            TableStorageBinding newBinding = new TableStorageBinding(
                    oldBinding.tableId(), oldBinding.spaceId(), oldBinding.path(),
                    oldBinding.rowFormatVersion(), bindings, oldBinding.lobSegment());
            TableDefinition published = new TableDefinition(
                    active.id(), active.schemaId(), active.name(), version, TableState.ACTIVE,
                    active.columns(), indexes, java.util.Optional.of(newBinding));
            sdi.write(published, timeout);
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
            return published;
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
            TableDefinition active = repository.findTable(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("table does not exist: " + name.canonicalKey()));
            TableStorageBinding binding = active.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException("ACTIVE table has no physical binding: " + active.id().value()));

            // 2、history 仍引用表 metadata 时不能发布 DROP_PENDING；等待只持 Java Condition，不持存储资源。
            purgeBarrier.awaitUnreferenced(active.id().value(), timeout);

            // 3、barrier 清零后才建立不可回退 cache 屏障并持久发布 DROP_PENDING。
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 0, 0, 1, 2));
            DictionaryVersion pendingVersion = DictionaryVersion.of(ids.dictionaryVersion());
            DictionaryVersion droppedVersion = DictionaryVersion.of(ids.dictionaryVersion() + 1);
            TableDefinition pending = lifecycle(active, pendingVersion, TableState.DROP_PENDING);
            DdlId ddlId = DdlId.of(ids.firstDdlId());
            DdlLogRecord prepared = new DdlLogRecord(
                    new DdlUndoMarker(ddlId.value(), pendingVersion.value(), active.id().value()),
                    DdlLogOperation.DROP_TABLE, DdlLogPhase.PREPARED, binding.spaceId(), binding.path());
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
            TableDefinition dropped = lifecycle(pending, droppedVersion, TableState.DROPPED);
            commitUpdate(droppedVersion, dropped);
            faultInjector.afterDropDictionaryCommitted(dropped);
            repository.ddlLog().transition(ddlId, DdlLogPhase.ENGINE_DONE, DdlLogPhase.COMMITTED);
            log.info("dropped table: name={} tableId={} ddlId={} version={}", name.canonicalKey(),
                    active.id().value(), ids.firstDdlId(), droppedVersion.value());
        }
    }

    private void commitUpdate(DictionaryVersion version, TableDefinition table) {
        try (DictionaryTransaction transaction = repository.begin(version)) {
            transaction.updateTable(table);
            transaction.commit();
        }
    }

    private static TableDefinition lifecycle(TableDefinition before, DictionaryVersion version, TableState state) {
        return new TableDefinition(before.id(), before.schemaId(), before.name(), version, state,
                before.columns(), before.indexes(), before.storageBinding());
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
}
