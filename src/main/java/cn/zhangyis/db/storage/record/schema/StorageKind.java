package cn.zhangyis.db.storage.record.schema;

/** 列存储形态；VARIABLE 与 OVERFLOW_CAPABLE 都进入记录变长目录，后者还允许 external reference。 */
public enum StorageKind {
    /** 编码宽度只由 schema 决定。 */
    FIXED,
    /** 逻辑值始终完整内联，长度由记录变长目录保存。 */
    VARIABLE,
    /** 可内联，也可保存指向 LOB 页链的稳定引用。 */
    OVERFLOW_CAPABLE
}
