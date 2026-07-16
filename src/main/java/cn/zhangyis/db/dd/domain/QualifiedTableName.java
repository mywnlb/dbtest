package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** catalog/schema/table 三段规范化名称；v1 默认 catalog 固定为 def。 */
public record QualifiedTableName(ObjectName catalog, ObjectName schema, ObjectName table) {
    public QualifiedTableName {
        if (catalog == null || schema == null || table == null) {
            throw new DatabaseValidationException("qualified table name parts must not be null");
        }
    }

    public static QualifiedTableName of(String schema, String table) {
        return new QualifiedTableName(ObjectName.of("def"), ObjectName.of(schema), ObjectName.of(table));
    }

    public String canonicalKey() {
        return catalog.canonicalName() + "." + schema.canonicalName() + "." + table.canonicalName();
    }
}
