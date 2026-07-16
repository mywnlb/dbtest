package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.cache.DictionaryObjectCache;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
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
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.storage.api.ddl.StorageColumnDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageColumnType;
import cn.zhangyis.db.storage.api.ddl.StorageColumnTypeId;
import cn.zhangyis.db.storage.api.ddl.StorageIndexDefinition;
import cn.zhangyis.db.storage.api.ddl.StorageIndexKeyPart;
import cn.zhangyis.db.storage.api.ddl.StorageIndexOrder;
import cn.zhangyis.db.storage.api.ddl.StorageTableDefinition;
import cn.zhangyis.db.storage.api.ddl.TableDdlStorageService;
import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
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
    private final Path tablesDirectory;
    private final DictionaryDdlFaultInjector faultInjector;

    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory) {
        this(control, repository, cache, locks, physical, tablesDirectory, DictionaryDdlFaultInjector.NO_OP);
    }

    /** 测试构造器：允许在已持久化状态边界注入进程中断，生产调用应使用六参构造。 */
    public DictionaryDdlService(DictionaryControlStore control, PersistentDictionaryRepository repository,
                                DictionaryObjectCache cache, MetadataLockManager locks,
                                TableDdlStorageService physical, Path tablesDirectory,
                                DictionaryDdlFaultInjector faultInjector) {
        if (control == null || repository == null || cache == null || locks == null || physical == null
                || tablesDirectory == null || faultInjector == null) {
            throw new DatabaseValidationException("dictionary DDL collaborators/path must not be null");
        }
        this.control = control;
        this.repository = repository;
        this.cache = cache;
        this.locks = locks;
        this.physical = physical;
        this.tablesDirectory = tablesDirectory.toAbsolutePath().normalize();
        this.faultInjector = faultInjector;
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
     * 创建表：schema IX + table X → 解析 schema/重复名 → durable 预留身份 → 物理 CREATE/flush → 单版本 DD publish
     * → cache publish。DD append 报错时不能证明 catalog header 未 durable，因此保留物理文件到重启：
     * 已提交 catalog 会重建 ACTIVE，未提交则由受控 orphan discovery 删除。
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
            SchemaDefinition schema = repository.findSchema(command.name().schema())
                    .orElseThrow(() -> new DictionaryObjectNotFoundException(
                            "schema does not exist: " + command.name().schema().displayName()));
            if (repository.findTable(schema.id(), command.name().table()).isPresent()) {
                throw new cn.zhangyis.db.dd.exception.DictionaryObjectExistsException(
                        "table already exists: " + command.name().canonicalKey());
            }
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
            TableStorageBinding binding = physical.createTable(storageRequest);
            TableDefinition table = new TableDefinition(tableId, schema.id(), command.name().table(), version,
                    TableState.ACTIVE, columns, indexes, java.util.Optional.of(binding));
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
            cache.publishTable(table);
            log.info("created table: name={} tableId={} space={} ddlId={} version={}",
                    command.name().canonicalKey(), tableId.value(), ids.firstSpaceId(), ids.firstDdlId(),
                    version.value());
            return table;
        }
    }

    /**
     * DROP 两版本状态机：先在 cache 建立不可回退的本地准入屏障，再提交 DROP_PENDING，随后确认历史 pin 归零
     * 并执行物理删除，最后提交 DROPPED。catalog publish 结果不确定时屏障保留到重启，避免旧内存快照复活表；
     * 若物理/最终 publish 失败，DROP_PENDING+binding 是启动恢复的权威续作输入。
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
            SchemaDefinition schema = repository.findSchema(name.schema()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("schema does not exist: " + name.schema().displayName()));
            TableDefinition active = repository.findTable(schema.id(), name.table()).orElseThrow(() ->
                    new DictionaryObjectNotFoundException("table does not exist: " + name.canonicalKey()));
            TableStorageBinding binding = active.storageBinding().orElseThrow(() ->
                    new DictionaryDdlException("ACTIVE table has no physical binding: " + active.id().value()));
            DictionaryIdAllocation ids = control.reserve(new DictionaryIdRequest(0, 0, 0, 0, 1, 2));
            DictionaryVersion pendingVersion = DictionaryVersion.of(ids.dictionaryVersion());
            DictionaryVersion droppedVersion = DictionaryVersion.of(ids.dictionaryVersion() + 1);
            TableDefinition pending = lifecycle(active, pendingVersion, TableState.DROP_PENDING);
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
            faultInjector.afterDropPendingPublished(pending);
            if (!cache.awaitUnpinned(active.id(), timeout)) {
                throw new DictionaryDdlException("timed out waiting dictionary pins before DROP: "
                        + active.id().value());
            }
            physical.dropTable(binding, timeout);
            commitUpdate(droppedVersion, lifecycle(pending, droppedVersion, TableState.DROPPED));
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
