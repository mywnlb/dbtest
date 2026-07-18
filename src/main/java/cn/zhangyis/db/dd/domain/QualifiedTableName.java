package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** catalog/schema/table 三段规范化名称；v1 默认 catalog 固定为 def。
 *
 * @param catalog 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
 */
public record QualifiedTableName(ObjectName catalog, ObjectName schema, ObjectName table) {
    public QualifiedTableName {
        if (catalog == null || schema == null || table == null) {
            throw new DatabaseValidationException("qualified table name parts must not be null");
        }
    }

    /**
     * 根据调用参数构造 {@code of} 对应的数据字典领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param schema 传给 {@code of} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @param table 传给 {@code of} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code of} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public static QualifiedTableName of(String schema, String table) {
        return new QualifiedTableName(ObjectName.of("def"), ObjectName.of(schema), ObjectName.of(table));
    }

    public String canonicalKey() {
        return catalog.canonicalName() + "." + schema.canonicalName() + "." + table.canonicalName();
    }
}
