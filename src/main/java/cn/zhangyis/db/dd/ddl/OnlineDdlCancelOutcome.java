package cn.zhangyis.db.dd.ddl;

/** public Online DDL取消请求的稳定结果码。 */
public enum OnlineDdlCancelOutcome {
    /** cancel在marker/resource前赢得prepare handoff。 */
    ACCEPTED_BEFORE_PREPARE,
    /** CANCEL_REQUESTED已经成功持久化，崩溃后恢复必须回滚。 */
    ACCEPTED_DURABLE,
    /** marker此前已是CANCEL_REQUESTED，本次请求幂等成功。 */
    ALREADY_REQUESTED,
    /** FORWARD_ONLY已持久化，不能再逆向删除target资源。 */
    TOO_LATE_FORWARD_ONLY,
    /** operation已经进入COMMITTED/ROLLED_BACK等终态。 */
    TERMINAL,
    /** registry与durable marker均不存在该identity。 */
    NOT_FOUND
}
