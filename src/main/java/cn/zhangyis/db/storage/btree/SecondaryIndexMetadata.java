package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.SecondaryIndexLayout;

/**
 * 一个二级索引的运行期不可变元数据。B+Tree descriptor 只表达完整物理 key，layout 负责完整行与紧凑 entry 的映射，
 * logicalUnique 则保留 DD 约束语义；三者不能压缩成同一个布尔状态。
 *
 * @param index         非聚簇 B+Tree 描述符；完整物理 key 必须唯一。
 * @param layout        与 descriptor schema/keyDef 精确一致的二级 entry 布局。
 * @param logicalUnique DD 声明的二级逻辑 key 是否唯一；不控制 B+Tree 的完整 key duplicate 检查。
 */
public record SecondaryIndexMetadata(BTreeIndex index, SecondaryIndexLayout layout, boolean logicalUnique) {

    /**
     * 校验二级 descriptor、紧凑 entry layout 与 DD 唯一语义能够组成同一索引快照。
     *
     * @param index         非聚簇 B+Tree descriptor；其 physical key 必须包含完整聚簇主键后缀并保持唯一。
     * @param layout        完整表行与紧凑二级 entry 之间的 exact-version 映射。
     * @param logicalUnique DD 声明的逻辑唯一属性；允许与“完整 physical key 永远唯一”同时存在。
     * @throws DatabaseValidationException 字段缺失、索引错误地声明为聚簇/非物理唯一，或 descriptor 与 layout
     *                                     的 index id、schema、key definition 不一致时抛出。
     */
    public SecondaryIndexMetadata {
        if (index == null || layout == null) {
            throw new DatabaseValidationException("secondary index metadata fields must not be null");
        }
        if (index.clustered() || !index.physicalUnique()) {
            throw new DatabaseValidationException("secondary index must be non-clustered and physically unique");
        }
        if (index.indexId() != layout.physicalKeyDef().indexId()
                || !index.keyDef().equals(layout.physicalKeyDef())
                || !index.schema().equals(layout.entrySchema())) {
            throw new DatabaseValidationException("secondary index descriptor/layout mismatch: " + index.indexId());
        }
    }
}
