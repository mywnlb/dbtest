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

    private final PersistentDictionaryRepository repository;
    /** 与 SQL exact-version 路径共享的纯 mapper；repository lookup 只发生在本 resolver 入口。 */
    private final DictionaryStorageMetadataMapper mapper;

    public DictionaryIndexMetadataResolver(PersistentDictionaryRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("dictionary index resolver repository must not be null");
        }
        this.repository = repository;
        this.mapper = new DictionaryStorageMetadataMapper();
    }

    /** tableId/indexId 双身份必须命中同一个非 DROPPED 聚簇/二级索引定义和 binding。 */
    @Override
    public BTreeIndex resolve(long tableId, long indexId) {
        return mapTarget(tableId, indexId).clusteredIndex();
    }

    /** rollback 使用同一个 committed table aggregate 同时映射聚簇索引和权威 LOB segment。 */
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
