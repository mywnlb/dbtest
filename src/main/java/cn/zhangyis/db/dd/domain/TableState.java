package cn.zhangyis.db.dd.domain;

/** 字典层表生命周期；DROP_PENDING 只在 DDL/recovery 内可见，普通 lookup 只返回 ACTIVE。 */
public enum TableState {
    ACTIVE,
    DROP_PENDING,
    DROPPED
}
