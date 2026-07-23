package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;

/**
 * merge 时把持久 table/schema/index identity 解析为 exact-version 二级索引 metadata 的内部 SPI。
 * 未知、已 DROP、版本不匹配或物理 binding 变化必须抛领域异常，禁止回退到“当前同名索引”。
 */
@FunctionalInterface
public interface ChangeBufferMetadataResolver {

    /**
     * @param tableId mutation 中持久化的正表 id
     * @param schemaVersion mutation 编码 entry 时使用的正 schema version
     * @param indexId mutation 指向的正二级索引 id
     * @return 与三个 identity 完全一致的二级 descriptor/layout
     */
    SecondaryIndexMetadata resolve(long tableId, long schemaVersion, long indexId);
}
