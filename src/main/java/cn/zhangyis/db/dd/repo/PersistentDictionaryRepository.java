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
import cn.zhangyis.db.dd.domain.TableOptions;
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
import java.util.function.Function;

/**
 * append-only catalog 的 Repository。writerLock 只保护 expected-version 校验、单批 append 和新 snapshot 发布；
 * lookup 读取 volatile immutable snapshot，不拿锁。锁内不获取 MDL/page latch 或用户表文件锁；唯一允许的外部
 * durable 协作是按 {@code writerLock -> manifest eventLock} 固定顺序写恢复见证。
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
    /** catalog append 前的 durable witness；生产使用 recovery manifest，兼容构造使用 no-op。 */
    private final DictionaryDurabilityWitness witness;
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
        this(store, DictionaryDurabilityWitness.noOp());
    }

    /**
     * 创建带 catalog mutation 写前 witness 的持久 repository。
     *
     * @param store catalog durable batch store
     * @param witness 每次 mutation append 前必须 durable 返回的 recovery witness
     */
    public PersistentDictionaryRepository(InternalCatalogStore store, DictionaryDurabilityWitness witness) {
        if (store == null) {
            throw new DatabaseValidationException("internal catalog store must not be null");
        }
        if (witness == null) {
            throw new DatabaseValidationException("dictionary durability witness must not be null");
        }
        this.store = store;
        this.witness = witness;
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
     * 在 catalog writer fence 内读取当前不可变快照并执行一个短协调动作。
     *
     * <p>该入口用于 clean recovery manifest：调用方可在同一临界区内按
     * {@code repository writer lock -> manifest event lock} 顺序发布快照，从而保证 mutation intent
     * 不能夹在 snapshot 读取与 clean append 之间。回调只允许执行该 manifest durable append，
     * 禁止等待 MDL、page latch、文件生命周期锁或访问用户数据文件。</p>
     *
     * @param action 只读消费当前 snapshot 的短动作；不得保存锁所有权或反向调用字典 mutation
     * @param <T> 协调动作返回值类型
     * @return action 在稳定 writer fence 内产生的结果；空值语义由 action 自身定义
     * @throws DatabaseValidationException action 为空时抛出，未取得 writer lock
     */
    public <T> T withSnapshotWriterFence(Function<DictionarySnapshot, T> action) {
        if (action == null) {
            throw new DatabaseValidationException("dictionary snapshot writer-fence action must not be null");
        }
        writerLock.lock();
        try {
            return action.apply(snapshot);
        } finally {
            writerLock.unlock();
        }
    }

    /**
     * DictionaryTransaction 的唯一提交入口，按 witness -> catalog -> snapshot 顺序发布 mutation。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在 writer lock 内复核版本严格递增以及 schema/table 名称、父子和状态约束。</li>
     *     <li>把 mutation 编码为带 commit manifest 的确定性 records；编码失败不产生持久副作用。</li>
     *     <li>先 durable 追加 catalog mutation intent，再把同一 records append 到权威 catalog。</li>
     *     <li>从 committed batches 完整重建并核对目标版本后发布 volatile snapshot；失败不伪造内存提交。</li>
     * </ol>
     *
     * @param version 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schemas 参与 {@code commit} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param tables 参与 {@code commit} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DictionaryVersionConflictException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     * @throws DictionaryCatalogCorruptionException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public void commit(DictionaryVersion version, List<SchemaDefinition> schemas, List<TableDefinition> tables) {
        // 1. writer lock 同时保护 expected-version 校验和最终 snapshot 发布。
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
            // 2. codec 输出正是 witness 摘要和 catalog append 共用的 immutable records。
            List<CatalogRecord> records = codec.encode(version, schemas, tables);
            // 3. catalog append 是逻辑真相提交点；intent 必须先 durable，避免 sidecar 静默落后。
            witness.beforeCatalogMutation(version, records);
            store.append(records);
            // 4. 只信任从 committed 物理批次重建的快照，不直接套用调用方对象。
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
        if (!before.id().equals(after.id())) {
            throw new DictionaryVersionConflictException("table replacement cannot change table identity");
        }
        boolean lifecycle = before.indexes().equals(after.indexes())
                && before.schemaId().equals(after.schemaId())
                && before.name().equals(after.name())
                && before.columns().equals(after.columns())
                && before.options().equals(after.options())
                && before.storageBinding().equals(after.storageBinding())
                && validLifecycleTransition(before.state(), after.state());
        boolean addSecondaryIndex = exactSecondaryIndexAddition(before, after);
        boolean removeSecondaryIndex = exactSecondaryIndexRemoval(before, after);
        boolean metadataAlter = exactMetadataAlter(before, after);
        boolean blockingRebuild = exactBlockingRebuild(before, after);
        if (!lifecycle && !addSecondaryIndex && !removeSecondaryIndex
                && !metadataAlter && !blockingRebuild) {
            throw new DictionaryVersionConflictException("invalid table lifecycle transition: "
                    + before.state() + " -> " + after.state());
        }
    }

    /** COMMENT/default charset/rename 只替换逻辑 metadata，物理 row format 与索引绑定必须逐字保持。 */
    private static boolean exactMetadataAlter(TableDefinition before, TableDefinition after) {
        return before.state() == cn.zhangyis.db.dd.domain.TableState.ACTIVE
                && after.state() == cn.zhangyis.db.dd.domain.TableState.ACTIVE
                && before.columns().equals(after.columns())
                && before.indexes().equals(after.indexes())
                && before.storageBinding().equals(after.storageBinding());
    }

    /**
     * 阻塞式结构 ALTER 只能换到全新 space/path，table id 保持，且新 binding 的 row format 必须等于
     * 新 DD version。物理 segments/root 的完整集合继续由 TableDefinition 构造器交叉校验。
     */
    private static boolean exactBlockingRebuild(TableDefinition before, TableDefinition after) {
        if (before.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || after.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || before.storageBinding().isEmpty() || after.storageBinding().isEmpty()) {
            return false;
        }
        var oldBinding = before.storageBinding().orElseThrow();
        var newBinding = after.storageBinding().orElseThrow();
        return oldBinding.tableId() == newBinding.tableId()
                && !oldBinding.spaceId().equals(newBinding.spaceId())
                && !oldBinding.path().equals(newBinding.path())
                && newBinding.rowFormatVersion() == after.version().value();
    }

    private static boolean validLifecycleTransition(cn.zhangyis.db.dd.domain.TableState before,
                                                     cn.zhangyis.db.dd.domain.TableState after) {
        return before == cn.zhangyis.db.dd.domain.TableState.ACTIVE
                && (after == cn.zhangyis.db.dd.domain.TableState.DROP_PENDING
                || after == cn.zhangyis.db.dd.domain.TableState.DISCARD_PENDING
                || after == cn.zhangyis.db.dd.domain.TableState.RECOVERY_UNAVAILABLE)
                || before == cn.zhangyis.db.dd.domain.TableState.DROP_PENDING
                && after == cn.zhangyis.db.dd.domain.TableState.DROPPED
                || before == cn.zhangyis.db.dd.domain.TableState.DISCARD_PENDING
                && after == cn.zhangyis.db.dd.domain.TableState.DISCARDED
                || before == cn.zhangyis.db.dd.domain.TableState.DISCARDED
                && after == cn.zhangyis.db.dd.domain.TableState.IMPORT_PENDING
                || before == cn.zhangyis.db.dd.domain.TableState.IMPORT_PENDING
                && after == cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || before == cn.zhangyis.db.dd.domain.TableState.RECOVERY_UNAVAILABLE
                && (after == cn.zhangyis.db.dd.domain.TableState.RECOVERY_DISCARDED
                || after == cn.zhangyis.db.dd.domain.TableState.DROPPED)
                || before == cn.zhangyis.db.dd.domain.TableState.RECOVERY_DISCARDED
                && (after == cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || after == cn.zhangyis.db.dd.domain.TableState.DROPPED);
    }

    /**
     * 精确识别 CREATE INDEX 的 ACTIVE→ACTIVE aggregate 替换。已有逻辑/物理索引必须保持顺序与 identity，
     * 只允许在尾部追加一个非聚簇索引及同 id binding，且 row format、space、path、LOB 均不得改变。
     */
    private static boolean exactSecondaryIndexAddition(TableDefinition before, TableDefinition after) {
        if (before.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || after.state() != cn.zhangyis.db.dd.domain.TableState.ACTIVE
                || !before.schemaId().equals(after.schemaId())
                || !before.name().equals(after.name())
                || !before.columns().equals(after.columns())
                || !before.options().equals(after.options())
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
                || !before.schemaId().equals(after.schemaId())
                || !before.name().equals(after.name())
                || !before.columns().equals(after.columns())
                || !before.options().equals(after.options())
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
        boolean baselineSeen = false;
        boolean mutationSeen = false;
        for (CatalogBatch batch : batches) {
            Optional<DictionarySnapshot> baseline = codec.decodeBaseline(batch);
            if (baseline.isPresent()) {
                if (baselineSeen || mutationSeen || !current.equals(DictionarySnapshot.emptyBootstrap())) {
                    throw new DictionaryCatalogCorruptionException(
                            "dictionary baseline must be the first and only baseline batch");
                }
                current = baseline.orElseThrow();
                baselineSeen = true;
                continue;
            }
            Optional<DictionaryCatalogCodec.DecodedMutation> decoded = codec.decode(batch);
            if (decoded.isEmpty()) {
                continue;
            }
            mutationSeen = true;
            DictionaryCatalogCodec.DecodedMutation mutation = deriveLegacyTableOptions(
                    current, decoded.get());
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

    /**
     * 旧 table payload 不携带 options，而普通 mutation batch 通常也不重复写父 schema；因此迁移必须在
     * repository 已重建到前一版本后，用当前或同批 schema defaults 填充，不能由 codec 猜测固定字符集。
     */
    private static DictionaryCatalogCodec.DecodedMutation deriveLegacyTableOptions(
            DictionarySnapshot current, DictionaryCatalogCodec.DecodedMutation mutation) {
        if (mutation.legacyOptionTables().isEmpty()) {
            return mutation;
        }
        Map<SchemaId, SchemaDefinition> schemas = new LinkedHashMap<>(current.schemas());
        mutation.schemas().forEach(schema -> schemas.put(schema.id(), schema));
        List<TableDefinition> tables = mutation.tables().stream().map(table -> {
            if (!mutation.legacyOptionTables().contains(table.id())) {
                return table;
            }
            SchemaDefinition schema = Optional.ofNullable(schemas.get(table.schemaId())).orElseThrow(() ->
                    new DictionaryCatalogCorruptionException(
                            "legacy table options reference missing schema: " + table.schemaId().value()));
            return new TableDefinition(table.id(), table.schemaId(), table.name(), table.version(),
                    table.state(), table.columns(), table.indexes(), table.storageBinding(),
                    new TableOptions("", schema.defaultCharsetId(), schema.defaultCollationId()));
        }).toList();
        return new DictionaryCatalogCodec.DecodedMutation(
                mutation.version(), mutation.schemas(), tables, java.util.Set.of());
    }
}
