package cn.zhangyis.db.storage.api.ddl;

/** 通用INPLACE descriptor中物理索引资源的所有权方向。 */
public enum OnlineAlterIndexDescriptorAction {
    /** 目标DD发布前新建，回滚时释放，前滚后进入target binding。 */
    ADD,
    /** source DD当前引用，前滚发布后按retirement fence释放，回滚时保持不动。 */
    DROP
}
