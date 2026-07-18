package cn.zhangyis.db.storage.recovery;

/**
 * crash recovery 对持久 PREPARED resource-manager participant 的外部决议。
 *
 * <p>决议来源必须由上层协调器持久化；存储层不能根据 undo 内容或超时自行推断。</p>
 */
public enum PreparedTransactionDecision {

    /** 全局事务已经决定提交，本地恢复执行 prepared commit。 */
    COMMIT,
    /** 全局事务已经决定回滚，本地恢复执行 prepared rollback。 */
    ROLLBACK,
    /** 协调器没有权威决议；恢复必须 fail-closed。 */
    UNRESOLVED
}
