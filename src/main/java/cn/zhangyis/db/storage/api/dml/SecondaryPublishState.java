package cn.zhangyis.db.storage.api.dml;

/**
 * 前向 DML 发布目标二级物理 identity 前的状态。该状态被写进 UPDATE undo，决定 rollback 是物理删除新 entry，
 * 还是把前向 revive 的 entry 恢复为 delete-marked。
 */
public enum SecondaryPublishState {

    /** 完整物理 key 不存在；前向路径必须 insert，rollback 对应物理删除。 */
    ABSENT,

    /** 同一完整物理 key 存在但已 delete-marked；前向路径 revive，rollback 对应恢复 marked。 */
    DELETE_MARKED
}
