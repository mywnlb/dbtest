package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.dd.domain.TableState;
import cn.zhangyis.db.dd.exception.DictionaryObjectNotFoundException;
import cn.zhangyis.db.dd.repo.PersistentDictionaryRepository;
import cn.zhangyis.db.storage.btree.IndexMetadataResolver;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.trx.UndoTargetMetadata;
import cn.zhangyis.db.storage.trx.UndoTargetMetadataResolver;

/** committed DD table/index 定义与物理 binding 的 rollback/purge adapter。无全局默认索引或名称猜测。 */
public final class DictionaryIndexMetadataResolver implements IndexMetadataResolver, UndoTargetMetadataResolver {

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
        return new UndoTargetMetadata(mapped.tableIndexes(), mapped.lobSegment());
    }
}
