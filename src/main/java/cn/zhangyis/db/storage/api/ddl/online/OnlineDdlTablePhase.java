package cn.zhangyis.db.storage.api.ddl.online;

/**
 * 单表 Online DDL gate 的运行期状态。状态只存在于进程内，崩溃后的权威阶段由 DDL marker、descriptor 和
 * 可选 row-log manifest 重建，不能把本枚举直接序列化为恢复证据。
 */
public enum OnlineDdlTablePhase {
    /** 表上没有活动 online build，DML 仍登记事务影响集合。 */
    ABSENT,
    /** initial freeze：阻止新 admission，等待旧 DML 与事务终态排空。 */
    ACTIVATING,
    /** base scan 期间允许 DML，并向冻结的 target 追加 candidate。 */
    CAPTURING,
    /** Online DROP prepare期间允许DML继续维护source index，但不创建row-log capture target。 */
    RETIREMENT_OPEN,
    /** final freeze：阻止新 admission，等待 DML、事务和文件 I/O lease 排空。 */
    SEALING,
    /** final freeze 已完成，coordinator 可执行 reconciliation 与发布。 */
    SEALED,
    /** build 已决定回滚；DML 继续但不再访问该 build 的 row log。 */
    ABORTING
}
