package cn.zhangyis.db.dd.repo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.SchemaDefinition;
import cn.zhangyis.db.dd.domain.SchemaId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;
import cn.zhangyis.db.dd.exception.DictionaryObjectExistsException;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.exception.DictionaryVersionConflictException;
import cn.zhangyis.db.dd.tx.DictionaryTransaction;
import cn.zhangyis.db.storage.api.catalog.CatalogBatch;
import cn.zhangyis.db.storage.api.catalog.CatalogRecord;
import cn.zhangyis.db.storage.api.catalog.InternalCatalogStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * append-only catalog 的 Repository。writerLock 只保护 expected-version 校验、单批 append 和新 snapshot 发布；
 * lookup 读取 volatile immutable snapshot，不拿锁。锁内不获取 MDL/page latch，也不等待其它数据库资源。
 */
public final class PersistentDictionaryRepository {

    /**
     * 本对象持有的 {@code store} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final InternalCatalogStore store;
    /**
     * 本对象持有的 {@code codec} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DictionaryCatalogCodec codec = new DictionaryCatalogCodec();
    /** 与字典 mutation 共享物理 catalog、但独立解释 DDL_LOG batches 的阶段仓储。 */
    private final PersistentDdlLogRepository ddlLog;

    /** 字典版本唯一 writer 临界区，不能跨出本 repository。 */
    private final ReentrantLock writerLock = new ReentrantLock();

    /** 已发布不可变快照；append + rebuild 完成后一次 volatile 替换。 */
    private volatile DictionarySnapshot snapshot;

    /**
     * 创建 {@code PersistentDictionaryRepository}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param store 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public PersistentDictionaryRepository(InternalCatalogStore store) {
        if (store == null) {
            throw new DatabaseValidationException("internal catalog store must not be null");
        }
        this.store = store;
        this.ddlLog = new PersistentDdlLogRepository(store);
        this.snapshot = rebuild(store.readCommittedBatches());
    }

    /**
     * 返回与本 repository 共享同一 durable catalog 的 DDL log 仓储。
     *
     * @return 单实例 phase CAS、跨重启重建且不把普通 DD batch 当 marker 的仓储。
     */
    public PersistentDdlLogRepository ddlLog() {
        return ddlLog;
    }

    /** 创建指定 next version 的内部 Unit of Work；实际冲突在 commit 时重新验证。
     *
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code begin} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DictionaryTransaction begin(DictionaryVersion version) {
        if (version == null) {
            throw new DatabaseValidationException("dictionary transaction version must not be null");
        }
        return new DictionaryTransaction(this, version);
    }

    /**
     * 定位并读取数据字典领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param id 参与 {@code findSchema} 的稳定领域标识 {@code SchemaId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code findSchema} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<SchemaDefinition> findSchema(SchemaId id) {
        return Optional.ofNullable(snapshot.schemas().get(id));
    }

    /**
     * 定位并读取数据字典领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code findSchema} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<SchemaDefinition> findSchema(ObjectName name) {
        if (name == null) {
            throw new DatabaseValidationException("schema name must not be null");
        }
        return snapshot.schemas().values().stream().filter(schema -> schema.name().equals(name)).findFirst();
    }

    /**
     * 定位并读取数据字典领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param id 参与 {@code findTable} 的稳定领域标识 {@code TableId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code findTable} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<TableDefinition> findTable(TableId id) {
        return Optional.ofNullable(snapshot.tables().get(id))
                .filter(table -> table.state() == cn.zhangyis.db.dd.domain.TableState.ACTIVE);
    }

    /**
     * 定位并读取数据字典领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param schemaId 参与 {@code findTable} 的稳定领域标识 {@code SchemaId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code findTable} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<TableDefinition> findTable(SchemaId schemaId, ObjectName name) {
        if (schemaId == null || name == null) {
            throw new DatabaseValidationException("table schema id/name must not be null");
        }
        return snapshot.tables().values().stream()
                .filter(table -> table.state() == cn.zhangyis.db.dd.domain.TableState.ACTIVE)
                .filter(table -> table.schemaId().equals(schemaId) && table.name().equals(name)).findFirst();
    }

    /** recovery/DDL state machine 读取包括 DROP_PENDING/DROPPED 在内的原始最新版本。
     *
     * @param id 参与 {@code findTableForRecovery} 的稳定领域标识 {@code TableId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code findTableForRecovery} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<TableDefinition> findTableForRecovery(TableId id) {
        return Optional.ofNullable(snapshot.tables().get(id));
    }

    /** 按名称读取包括 DISCARDED/IMPORT_PENDING 在内的最新表版本，供 DDL recovery/control plane 使用。
     *
     * @param schemaId 参与 {@code findTableForRecovery} 的稳定领域标识 {@code SchemaId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param name 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return {@code findTableForRecovery} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<TableDefinition> findTableForRecovery(SchemaId schemaId, ObjectName name) {
        if (schemaId == null || name == null) {
            throw new DatabaseValidationException("recovery table lookup schema/name must not be null");
        }
        return snapshot().tables().values().stream()
                .filter(table -> table.schemaId().equals(schemaId) && table.name().equals(name))
                .filter(table -> table.state() != cn.zhangyis.db.dd.domain.TableState.DROPPED)
                .findFirst();
    }

    /**
     * 定位并读取数据字典领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param id 参与 {@code findIndex} 的稳定领域标识 {@code IndexId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code findIndex} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    public Optional<IndexDefinition> findIndex(IndexId id) {
        return Optional.ofNullable(snapshot.indexes().get(id));
    }

    public DictionarySnapshot snapshot() {
        return snapshot;
    }

    /**
     * DictionaryTransaction 的唯一提交入口。先在当前 snapshot 上完成版本、名称和父对象校验，再编码/append；
     * append durable 后从 committed batches 重建并发布，任何前置失败都不会写 catalog。
     *
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schemas 参与 {@code commit} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param tables 参与 {@code commit} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DictionaryVersionConflictException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public void commit(DictionaryVersion version, List<SchemaDefinition> schemas, List<TableDefinition> tables) {
        if (version == null || schemas == null || tables == null || schemas.isEmpty() && tables.isEmpty()) {
            throw new DatabaseValidationException("dictionary commit version/mutations must not be null or empty");
        }
        writerLock.lock();
        try {
            DictionarySnapshot before = snapshot;
            if (version.compareTo(before.publishedVersion()) <= 0) {
                throw new DictionaryVersionConflictException("dictionary version must advance monotonically: before="
                        + before.publishedVersion().value() + ", actual=" + version.value());
            }
            validateMutation(before, version, schemas, tables);
            List<CatalogRecord> records = codec.encode(version, schemas, tables);
            store.append(records);
            DictionarySnapshot rebuilt = rebuild(store.readCommittedBatches());
            if (!rebuilt.publishedVersion().equals(version)) {
                throw new DictionaryCatalogCorruptionException("dictionary append did not publish requested version: "
                        + version.value());
            }
            snapshot = rebuilt;
        } finally {
            writerLock.unlock();
        }
    }

    private static void validateMutation(DictionarySnapshot before, DictionaryVersion version,
                                         List<SchemaDefinition> schemas, List<TableDefinition> tables) {
        Map<SchemaId, SchemaDefinition> schemaView = new LinkedHashMap<>(before.schemas());
        for (SchemaDefinition schema : schemas) {
            if (schema == null || !schema.version().equals(version)) {
                throw new DictionaryVersionConflictException("schema mutation version mismatch");
            }
            boolean duplicateName = schemaView.values().stream().anyMatch(existing ->
                    existing.name().equals(schema.name()) && !existing.id().equals(schema.id()));
            if (duplicateName || before.schemas().containsKey(schema.id())) {
                throw new DictionaryObjectExistsException("schema already exists: " + schema.name());
            }
            schemaView.put(schema.id(), schema);
        }
        Map<TableId, TableDefinition> tableView = new LinkedHashMap<>(before.tables());
        for (TableDefinition table : tables) {
            if (table == null || !table.version().equals(version)) {
                throw new DictionaryVersionConflictException("table mutation version mismatch");
            }
            if (!schemaView.containsKey(table.schemaId())) {
                throw new DictionaryObjectNotFoundException("table schema does not exist: "
                        + table.schemaId().value());
            }
            TableDefinition existingById = tableView.get(table.id());
            boolean duplicateName = tableView.values().stream().anyMatch(existing ->
                    existing.state() != cn.zhangyis.db.dd.domain.TableState.DROPPED
                            &&
                    existing.schemaId().equals(table.schemaId()) && existing.name().equals(table.name())
                            && !existing.id().equals(table.id()));
            if (duplicateName) {
                throw new DictionaryObjectExistsException("table already exists: " + table.name());
            }
            if (existingById == null) {
                if (table.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE) {
                    throw new DictionaryVersionConflictException("new table must enter ACTIVE state");
                }
            } else {
                validateTableReplacement(existingById, table);
            }
            tableView.put(table.id(), table);
        }
    }

    private static void validateTableReplacement(TableDefinition before, TableDefinition after) {
        if (!before.schemaId().equals(after.schemaId()) || !before.name().equals(after.name())
                || !before.columns().equals(after.columns())) {
            throw new DictionaryVersionConflictException(
                    "table replacement cannot change identity or columns");
        }
        boolean lifecycle = before.indexes().equals(after.indexes())
                && before.storageBinding().equals(after.storageBinding())
                && validLifecycleTransition(before.state(), after.state());
        boolean addSecondaryIndex = exactSecondaryIndexAddition(before, after);
        boolean removeSecondaryIndex = exactSecondaryIndexRemoval(before, after);
        if (!lifecycle && !addSecondaryIndex && !removeSecondaryIndex) {
            throw new DictionaryVersionConflictException("invalid table lifecycle transition: "
                    + before.state() + " -> " + after.state());
        }
    }

    private static boolean validLifecycleTransition(cn.zhangyis.db.dd.domain.TableState before,
                                                     cn.zhangyis.db.dd.domain.TableState after) {
        return before == cn.zhangyis.db.dd.domain.TableState.ACTIVE
                && (after == cn.zhangyis.db.dd.domain.TableState.DROP_PENDING
                || after == cn.zhangyis.db.dd.domain.TableState.DISCARD_PENDING)
                || before == cn.zhangyis.db.dd.domain.TableState.DROP_PENDING
                && after == cn.zhangyis.db.dd.domain.TableState.DROPPED
                || before == cn.zhangyis.db.dd.domain.TableState.DISCARD_PENDING
                && after == cn.zhangyis.db.dd.domain.TableState.DISCARDED
                || before == cn.zhangyis.db.dd.domain.TableState.DISCARDED
                && after == cn.zhangyis.db.dd.domain.TableState.IMPORT_PENDING
                || before == cn.zhangyis.db.dd.domain.TableState.IMPORT_PENDING
                && after == cn.zhangyis.db.dd.domain.TableState.ACTIVE;
    }

    /**
     * 精确识别 CREATE INDEX 的 ACTIVE→ACTIVE aggregate 替换。已有逻辑/物理索引必须保持顺序与 identity，
     * 只允许在尾部追加一个非聚簇索引及同 id binding，且 row format、space、path、LOB 均不得改变。
     */
    private static boolean exactSecondaryIndexAddition(TableDefinition before, TableDefinition after) {
        if (before.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || after.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || after.indexes().size() != before.indexes().size() + 1
                || !after.indexes().subList(0, before.indexes().size()).equals(before.indexes())
                || after.indexes().getLast().clustered()
                || before.storageBinding().isEmpty() || after.storageBinding().isEmpty()) {
            return false;
        }
        var oldBinding = before.storageBinding().orElseThrow();
        var newBinding = after.storageBinding().orElseThrow();
        if (oldBinding.tableId() != newBinding.tableId()
                || !oldBinding.spaceId().equals(newBinding.spaceId())
                || !oldBinding.path().equals(newBinding.path())
                || oldBinding.rowFormatVersion() != newBinding.rowFormatVersion()
                || !oldBinding.lobSegment().equals(newBinding.lobSegment())
                || newBinding.indexes().size() != oldBinding.indexes().size() + 1
                || !newBinding.indexes().subList(0, oldBinding.indexes().size()).equals(oldBinding.indexes())) {
            return false;
        }
        return after.indexes().getLast().id().value() == newBinding.indexes().getLast().indexId();
    }

    /**
     * 精确识别 DROP INDEX 的 ACTIVE→ACTIVE aggregate 替换。逻辑定义与物理 binding 必须在同一 ordinal
     * 删除同一个非聚簇 index，其余顺序、identity、row format、space/path 与 LOB 均保持不变。
     */
    private static boolean exactSecondaryIndexRemoval(TableDefinition before, TableDefinition after) {
        if (before.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || after.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || before.indexes().size() != after.indexes().size() + 1
                || before.storageBinding().isEmpty() || after.storageBinding().isEmpty()) {
            return false;
        }
        var oldBinding = before.storageBinding().orElseThrow();
        var newBinding = after.storageBinding().orElseThrow();
        if (oldBinding.tableId() != newBinding.tableId()
                || !oldBinding.spaceId().equals(newBinding.spaceId())
                || !oldBinding.path().equals(newBinding.path())
                || oldBinding.rowFormatVersion() != newBinding.rowFormatVersion()
                || !oldBinding.lobSegment().equals(newBinding.lobSegment())
                || oldBinding.indexes().size() != newBinding.indexes().size() + 1) {
            return false;
        }
        for (int removedOrdinal = 0; removedOrdinal < before.indexes().size(); removedOrdinal++) {
            var removed = before.indexes().get(removedOrdinal);
            if (removed.clustered()
                    || removed.id().value() != oldBinding.indexes().get(removedOrdinal).indexId()) {
                continue;
            }
            List<cn.zhangyis.db.dd.domain.IndexDefinition> logical = new java.util.ArrayList<>(
                    before.indexes());
            logical.remove(removedOrdinal);
            List<cn.zhangyis.db.storage.api.ddl.IndexStorageBinding> physical =
                    new java.util.ArrayList<>(oldBinding.indexes());
            physical.remove(removedOrdinal);
            if (logical.equals(after.indexes()) && physical.equals(newBinding.indexes())) {
                return true;
            }
        }
        return false;
    }

    private DictionarySnapshot rebuild(List<CatalogBatch> batches) {
        DictionarySnapshot current = DictionarySnapshot.emptyBootstrap();
        for (CatalogBatch batch : batches) {
            Optional<DictionaryCatalogCodec.DecodedMutation> decoded = codec.decode(batch);
            if (decoded.isEmpty()) {
                continue;
            }
            DictionaryCatalogCodec.DecodedMutation mutation = decoded.get();
            if (mutation.version().compareTo(current.publishedVersion()) <= 0) {
                throw new DictionaryCatalogCorruptionException("dictionary committed versions are not monotonic: before="
                        + current.publishedVersion().value() + ", actual=" + mutation.version().value());
            }
            validateMutation(current, mutation.version(), mutation.schemas(), mutation.tables());
            Map<SchemaId, SchemaDefinition> schemas = new LinkedHashMap<>(current.schemas());
            Map<TableId, TableDefinition> tables = new LinkedHashMap<>(current.tables());
            Map<IndexId, IndexDefinition> indexes = new LinkedHashMap<>(current.indexes());
            for (SchemaDefinition schema : mutation.schemas()) {
                schemas.put(schema.id(), schema);
            }
            for (TableDefinition table : mutation.tables()) {
                TableDefinition previous = tables.get(table.id());
                if (previous != null) {
                    for (IndexDefinition oldIndex : previous.indexes()) {
                        indexes.remove(oldIndex.id());
                    }
                }
                tables.put(table.id(), table);
                for (IndexDefinition index : table.indexes()) {
                    if (indexes.putIfAbsent(index.id(), index) != null) {
                        throw new DictionaryCatalogCorruptionException("duplicate index id in dictionary: "
                                + index.id().value());
                    }
                }
            }
            current = new DictionarySnapshot(mutation.version(), schemas, tables, indexes);
        }
        return current;
    }
}
