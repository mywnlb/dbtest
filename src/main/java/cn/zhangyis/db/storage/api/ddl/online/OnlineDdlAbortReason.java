package cn.zhangyis.db.storage.api.ddl.online;

/** Online ADD INDEX 在 DD 提交前进入可恢复回滚路径的稳定原因分类。 */
public enum OnlineDdlAbortReason {
    /** candidate 达到配置容量，但 terminal reserve 仍可写入。 */
    ROW_LOG_CAPACITY,
    /** row-log append/force 出现可明确持久 abort 的 I/O 失败。 */
    ROW_LOG_IO,
    /** 最终 MDL upgrade 在有界等待内未成功。 */
    METADATA_LOCK_TIMEOUT,
    /** final reconciliation 发现确定性的非 NULL logical UNIQUE 冲突。 */
    UNIQUE_CONFLICT,
    /** 调用线程被取消或中断，且尚未越过 ENGINE_DONE。 */
    CANCELLED,
    /** recovery 输入损坏，但 committed DD 尚未发布，允许安全回收 staged owner。 */
    RECOVERY_CORRUPTION,
    /** final 双向验证发现 target 与 clustered source 不一致。 */
    VALIDATION_FAILED,
    /** coordinator 在 FORWARD_ONLY 之前遇到无法进一步细分、但仍可安全回滚的内部协作失败。 */
    INTERNAL_FAILURE
}
