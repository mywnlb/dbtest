package cn.zhangyis.db.dd.ddl;

/** Online DDL本进程可观察终点；NONE表示仍在运行。 */
public enum OnlineDdlTerminalResult {
    /** operation尚未进入可观察终点。 */
    NONE,
    /** target已经完整发布并收敛。 */
    COMPLETED,
    /** source保留且staged资源已回滚。 */
    ROLLED_BACK,
    /** 恢复或durability结果不确定，实例必须fail-closed。 */
    FAILED_CLOSED
}
