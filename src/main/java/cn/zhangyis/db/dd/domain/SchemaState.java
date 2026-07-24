package cn.zhangyis.db.dd.domain;

/**
 * Schema 生命周期。DROPPED 是 append-only catalog tombstone，普通 lookup 必须隐藏。
 */
public enum SchemaState {
    /** 可创建、绑定和访问其下对象。 */
    ACTIVE,
    /** 已完成级联删除的持久 tombstone，可允许同名新 schema 使用新 identity 创建。 */
    DROPPED
}
