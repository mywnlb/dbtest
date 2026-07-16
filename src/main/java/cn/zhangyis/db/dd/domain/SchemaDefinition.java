package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 不可变 schema 定义；默认字符语义使用稳定 ID，binder 后续可以据此补全列属性。 */
public record SchemaDefinition(SchemaId id, ObjectName name, int defaultCharsetId,
                               int defaultCollationId, DictionaryVersion version) {
    public SchemaDefinition {
        if (id == null || name == null || version == null || defaultCharsetId < 0 || defaultCollationId < 0) {
            throw new DatabaseValidationException("invalid schema definition");
        }
    }
}
