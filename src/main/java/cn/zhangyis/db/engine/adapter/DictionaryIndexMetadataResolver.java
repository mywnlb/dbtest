package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.dd.ddl.DdlControlState;
import cn.zhangyis.db.dd.ddl.DdlExecutionProtocol;
import cn.zhangyis.db.dd.ddl.DdlLogOperation;
import cn.zhangyis.db.dd.ddl.DdlRetiredResourceKind;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.trx.UndoTargetMetadata;
import cn.zhangyis.db.storage.trx.UndoTargetMetadataResolver;
import cn.zhangyis.db.storage.trx.UndoTargetDisposition;
import cn.zhangyis.db.storage.changebuffer.ChangeBufferMetadataResolver;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;

import java.util.Set;
import java.util.TreeSet;

/** committed DD table/index 定义与物理 binding 的 rollback/purge adapter。无全局默认索引或名称猜测。 */
public final class DictionaryIndexMetadataResolver implements IndexMetadataResolver,
        UndoTargetMetadataResolver, ChangeBufferMetadataResolver {

    /**
     * 本对象持有的 {@code repository} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final PersistentDictionaryRepository repository;
    /** 与 SQL exact-version 路径共享的纯 mapper；repository lookup 只发生在本 resolver 入口。 */
    private final DictionaryStorageMetadataMapper mapper;

    /**
     * 创建 {@code DictionaryIndexMetadataResolver}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param repository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DictionaryIndexMetadataResolver(PersistentDictionaryRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("dictionary index resolver repository must not be null");
        }
        this.repository = repository;
        this.mapper = new DictionaryStorageMetadataMapper();
    }

    /** tableId/indexId 双身份必须命中同一个非 DROPPED 聚簇/二级索引定义和 binding。
     *
     * @param tableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
     * @param indexId 参与 {@code resolve} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code resolve} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    @Override
    public BTreeIndex resolve(long tableId, long indexId) {
        return mapTarget(tableId, indexId).clusteredIndex();
    }

    /** rollback 使用同一个 committed table aggregate 同时映射聚簇索引和权威 LOB segment。
     *
     * @param tableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
     * @param indexId 参与 {@code resolveTarget} 的零基位置 {@code indexId}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code resolveTarget} 构造或恢复的 undo/rollback 对象；成功时不为 {@code null}，事务身份和 roll pointer 链保持一致
     */
    @Override
    public UndoTargetMetadata resolveTarget(long tableId, long indexId) {
        return mapTarget(tableId, indexId);
    }

    /**
     * Change Buffer merge 按持久 table/schema/index 三元组解析 exact-version 二级 descriptor；与 undo resolver 不同，
     * 这里的 indexId 必须指向 secondary，且 schema version 必须逐值相等，禁止用当前同名索引代替旧 incarnation。
     *
     * @param tableId mutation 持久化的正 DD 表 id
     * @param schemaVersion entry 编码时的正 storage schema version
     * @param indexId mutation 持久化的正二级索引 id
     * @return 与三元组精确匹配且仍可物理访问的二级 metadata
     * @throws DictionaryObjectNotFoundException 表/索引已删除、隔离或版本不匹配时抛出，恢复/merge 必须 fail-closed
     */
    @Override
    public SecondaryIndexMetadata resolve(long tableId, long schemaVersion, long indexId) {
        TableDefinition table = repository.findTableForRecovery(TableId.of(tableId)).orElseThrow(() ->
                new DictionaryObjectNotFoundException("change buffer table metadata not found: " + tableId));
        if (table.state() != TableState.ACTIVE) {
            throw new DictionaryObjectNotFoundException(
                    "change buffer references non-active table: " + tableId + " state=" + table.state());
        }
        MappedTableStorage mapped = mapper.map(table);
        if (mapped.tableIndexes().schemaVersion() != schemaVersion) {
            throw new DictionaryObjectNotFoundException("change buffer schema version mismatch: table="
                    + tableId + " expected=" + schemaVersion + " actual="
                    + mapped.tableIndexes().schemaVersion());
        }
        return mapped.tableIndexes().secondaryIndexes().stream()
                .filter(secondary -> secondary.index().indexId() == indexId)
                .findFirst().orElseThrow(() -> new DictionaryObjectNotFoundException(
                        "change buffer secondary metadata not found: table=" + tableId
                                + " index=" + indexId));
    }

    private UndoTargetMetadata mapTarget(long tableId, long indexId) {
        TableDefinition table = repository.findTableForRecovery(TableId.of(tableId)).orElseThrow(() ->
                new DictionaryObjectNotFoundException("undo table metadata not found: " + tableId));
        if (table.state() == TableState.DROPPED) {
            throw new DictionaryObjectNotFoundException("undo references dropped table: " + tableId);
        }
        table.indexes().stream()
                .filter(index -> index.id().value() == indexId).findFirst().orElseThrow(() ->
                        new DictionaryObjectNotFoundException("undo index metadata not found: table="
                                + tableId + " index=" + indexId));
        MappedTableStorage mapped = mapper.map(table);
        if (mapped.clusteredIndex().indexId() != indexId) {
            throw new DictionaryObjectNotFoundException("undo index is not the clustered index: table="
                    + tableId + " index=" + indexId);
        }
        UndoTargetDisposition disposition = table.state() == TableState.RECOVERY_UNAVAILABLE
                || table.state() == TableState.RECOVERY_DISCARDED
                ? UndoTargetDisposition.RECOVERY_UNAVAILABLE
                : UndoTargetDisposition.AVAILABLE;
        return new UndoTargetMetadata(mapped.tableIndexes(), mapped.lobSegment(), disposition,
                retiredSecondaryIndexes(table));
    }

    /**
     * 从未决ONLINE_DROP marker投影当前target DD已移除、但仍由retirement fence持有的secondary identity。
     *
     * <p>只有target version、FORWARD_ONLY、fence owner/table/resource全部闭合才返回；误判会让purge跳过逐entry删除，
     * 因而任何缺失或第三态都返回空集合并由普通metadata校验fail-closed。marker COMMITTED前barrier保证旧history
     * 已清空，COMMITTED后无需继续保留该投影。</p>
     *
     * @param table 当前committed ACTIVE target aggregate
     * @return 按index id排序的不可变退休集合；没有安全未决marker时为空
     */
    private Set<Long> retiredSecondaryIndexes(TableDefinition table) {
        TreeSet<Long> retired = new TreeSet<>();
        repository.ddlLog().unresolved().stream()
                .filter(record -> record.operation() == DdlLogOperation.DROP_INDEX)
                .filter(record -> record.executionProtocol()
                        == DdlExecutionProtocol.ONLINE_DROP_INDEX_V1)
                .filter(record -> record.controlState() == DdlControlState.FORWARD_ONLY)
                .filter(record -> record.marker().affectedObjectId() == table.id().value())
                .filter(record -> record.marker().dictionaryVersion() == table.version().value())
                .filter(record -> table.indexes().stream().noneMatch(index ->
                        index.id().value() == record.secondaryObjectId()))
                .filter(record -> record.retirementFence().isPresent())
                .filter(record -> {
                    var fence = record.retirementFence().orElseThrow();
                    return fence.tableId() == table.id().value()
                            && fence.ownerDdlId() == record.marker().ddlOperationId()
                            && fence.sourceDictionaryVersion() < table.version().value()
                            && fence.sourceMetadataPinVersion()
                            == fence.sourceDictionaryVersion()
                            && fence.resources().size() == 1
                            && fence.resources().getFirst().kind()
                            == DdlRetiredResourceKind.INDEX
                            && fence.resources().getFirst().resourceId()
                            == record.secondaryObjectId();
                })
                .forEach(record -> retired.add(record.secondaryObjectId()));
        return Set.copyOf(retired);
    }
}
