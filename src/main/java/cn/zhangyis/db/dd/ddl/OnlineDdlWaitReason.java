package cn.zhangyis.db.dd.ddl;

/** Online DDL当前唯一主要等待原因；诊断值不参与业务裁决。 */
public enum OnlineDdlWaitReason {
    /** 当前没有外部等待。 */
    NONE,
    /** 等待initial/final metadata lock。 */
    METADATA_LOCK,
    /** 等待gate admission/transaction排空。 */
    GATE_QUIESCENCE,
    /** 等待row-log force返回。 */
    ROW_LOG_FORCE,
    /** 等待tablespace/redo force返回。 */
    TABLESPACE_FORCE,
    /** 等待purge/history安全边界。 */
    PURGE_BARRIER,
    /** 等待旧metadata pin归零。 */
    METADATA_PIN,
    /** 等待prepare handoff形成durable marker或确定失败。 */
    PREPARE_DURABILITY,
    /** 等待旧物理资源满足retirement fence。 */
    RETIREMENT_FENCE
}
