package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.SchemaId;

/**
 * DROP SCHEMA CASCADE marker 冻结的 schema identity 与前后状态摘要。
 *
 * @param schemaId append-only catalog 中不可复用的 schema identity
 * @param canonicalName 经过 ObjectName 规则归一化的名称
 * @param sourceSchemaDigest ACTIVE schema 的 canonical 摘要
 * @param targetSchemaDigest DROPPED schema tombstone 的 canonical 摘要
 */
public record DdlBatchSchemaEntry(
        SchemaId schemaId,
        String canonicalName,
        DdlSchemaDigest sourceSchemaDigest,
        DdlSchemaDigest targetSchemaDigest) {

    /**
     * 创建级联删除的 schema 证据。
     *
     * @throws DatabaseValidationException identity、名称或摘要缺失时抛出
     */
    public DdlBatchSchemaEntry {
        if (schemaId == null || canonicalName == null
                || canonicalName.isBlank()
                || sourceSchemaDigest == null || targetSchemaDigest == null) {
            throw new DatabaseValidationException(
                    "DDL batch schema manifest fields are invalid");
        }
    }
}
