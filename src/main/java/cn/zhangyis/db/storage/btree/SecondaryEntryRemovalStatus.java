package cn.zhangyis.db.storage.btree;

/**
 * 二级索引 entry 物理删除结果。状态冲突与缺失被显式分开：缺失可用于 crash 重试幂等收敛，
 * 冲突则表示调用方使用了错误 inverse，或 purge 遇到仍为 live 的当前物理 entry，必须 fail-closed。
 */
public enum SecondaryEntryRemovalStatus {

    /** entry 状态符合调用方预期，已经从记录链和 Page Directory 中物理摘除。 */
    REMOVED,

    /** 完整物理 key 不存在；没有修改页，是可重试操作的幂等终态。 */
    ABSENT,

    /** entry 存在但 delete 位与调用方要求相反；没有修改页，调用方不得把它当作成功回收。 */
    STATE_CONFLICT
}
