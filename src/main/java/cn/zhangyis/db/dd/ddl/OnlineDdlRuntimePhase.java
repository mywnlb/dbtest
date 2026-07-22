package cn.zhangyis.db.dd.ddl;

/** Online DDL进程内诊断阶段；它不替代durable DDL phase/control状态机。 */
public enum OnlineDdlRuntimePhase {
    /** identity已注册但尚未开始冻结资源。 */
    REGISTERED,
    /** initial freeze/gate正在建立。 */
    ACTIVATING,
    /** row-log capture generation正在发布。 */
    CAPTURING,
    /** coordinator正在分批扫描聚簇真相。 */
    BASE_SCAN,
    /** 正等待final table MDL X。 */
    WAITING_FINAL_MDL,
    /** gate正在排空事务/admission/I/O lease。 */
    FINALIZING,
    /** 已seal并按cutover truth收敛candidate。 */
    RECONCILING,
    /** 正执行双向物理验证。 */
    VERIFYING,
    /** durable control已经进入FORWARD_ONLY。 */
    FORWARD_FENCED,
    /** 正发布SDI/DD/cache或补齐phase。 */
    PUBLISHING,
    /** logical target已发布，正等待旧资源安全回收。 */
    RETIRING,
    /** live operation正常完成。 */
    COMPLETED,
    /** 取消或确定性失败正在回收未发布资源。 */
    ABORTING,
    /** 未发布资源已完整回滚。 */
    ROLLED_BACK,
    /** 启动恢复正在处理source aggregate。 */
    RECOVERING_SOURCE,
    /** 启动恢复正在处理target aggregate。 */
    RECOVERING_TARGET,
    /** 恢复证据不一致，实例必须保持关闭。 */
    FAILED_CLOSED
}
