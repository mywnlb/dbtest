package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 稳定 schema 身份；0 保留给“尚未分配”，持久对象只能使用正数。 */
public record SchemaId(long value) {
    public SchemaId {
        if (value <= 0) {
            throw new DatabaseValidationException("schema id must be positive: " + value);
        }
    }

    public static SchemaId of(long value) {
        return new SchemaId(value);
    }
}
