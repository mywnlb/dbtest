package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 调用方固定的一个 table/schema version 对应的全部索引运行期快照。该聚合让 DML、rollback、MVCC 和 purge 消费
 * 同一版本的聚簇/二级布局，禁止按单个 index id 回退到“最后使用的聚簇索引”。
 *
 * @param tableId          DD 稳定表 id；undo/recovery 以它定位聚合。
 * @param schemaVersion    所有 descriptor/layout 共享的 schema version。
 * @param clusteredIndex   唯一聚簇索引。
 * @param secondaryIndexes 按 index id 严格递增的二级索引元数据。
 */
public record TableIndexMetadata(long tableId, long schemaVersion, BTreeIndex clusteredIndex,
                                 List<SecondaryIndexMetadata> secondaryIndexes) {

    /**
     * 校验并冻结一个表版本的完整索引聚合。
     *
     * @param tableId          DD 分配的稳定表 id，必须为正数。
     * @param schemaVersion    undo、rollback、purge 用于 exact-version 解析的版本，必须为正数。
     * @param clusteredIndex   唯一聚簇索引，必须与表版本一致且 physical unique。
     * @param secondaryIndexes 二级索引元数据；构造时复制，且必须按 index id 严格递增。
     * @throws DatabaseValidationException identity 无效、聚簇属性错误、索引 id 重复/乱序或任一索引版本错配时抛出。
     */
    public TableIndexMetadata {
        if (tableId <= 0 || schemaVersion <= 0 || clusteredIndex == null || secondaryIndexes == null) {
            throw new DatabaseValidationException("table index metadata fields are invalid");
        }
        if (!clusteredIndex.clustered() || !clusteredIndex.physicalUnique()
                || clusteredIndex.schema().schemaVersion() != schemaVersion) {
            throw new DatabaseValidationException("table index metadata requires a matching clustered index");
        }
        secondaryIndexes = List.copyOf(secondaryIndexes);
        Set<Long> ids = new HashSet<>();
        ids.add(clusteredIndex.indexId());
        long previous = Long.MIN_VALUE;
        for (SecondaryIndexMetadata secondary : secondaryIndexes) {
            if (secondary == null || secondary.index().indexId() <= previous
                    || !ids.add(secondary.index().indexId())
                    || secondary.index().schema().schemaVersion() != schemaVersion) {
                throw new DatabaseValidationException(
                        "table secondary indexes must be unique, version-matched and ordered by index id");
            }
            previous = secondary.index().indexId();
        }
    }

    /**
     * 返回供 DDL/诊断遍历使用的完整 descriptor 顺序。
     *
     * @return 聚簇索引在前、二级索引按稳定 id 递增排列的不可变列表。
     */
    public List<BTreeIndex> allIndexes() {
        List<BTreeIndex> result = new ArrayList<>(secondaryIndexes.size() + 1);
        result.add(clusteredIndex);
        secondaryIndexes.stream().map(SecondaryIndexMetadata::index).forEach(result::add);
        return List.copyOf(result);
    }

    /**
     * 在当前 exact-version 聚合中按稳定 id 定位索引 descriptor。
     *
     * @param indexId undo mutation、DD binding 或调用方提供的稳定索引 id。
     * @return 聚簇或二级索引的匹配 descriptor。
     * @throws DatabaseValidationException 当前表版本不存在该索引时抛出；调用方不得回退到其它版本或聚簇索引。
     */
    public BTreeIndex requireIndex(long indexId) {
        if (clusteredIndex.indexId() == indexId) {
            return clusteredIndex;
        }
        return secondaryIndexes.stream().map(SecondaryIndexMetadata::index)
                .filter(index -> index.indexId() == indexId).findFirst().orElseThrow(() ->
                        new DatabaseValidationException("table " + tableId + " has no index " + indexId));
    }

    /**
     * 按稳定 id 定位 exact-version 二级索引 metadata。
     *
     * @param indexId undo secondary mutation 中记录的二级索引稳定 id。
     * @return 同时携带物理 descriptor、紧凑 layout 与 logical unique 语义的二级 metadata。
     * @throws DatabaseValidationException id 指向聚簇索引或当前表版本不存在该二级索引时抛出。
     */
    public SecondaryIndexMetadata requireSecondary(long indexId) {
        return secondaryIndexes.stream()
                .filter(secondary -> secondary.index().indexId() == indexId)
                .findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "table " + tableId + " has no secondary index " + indexId));
    }
}
