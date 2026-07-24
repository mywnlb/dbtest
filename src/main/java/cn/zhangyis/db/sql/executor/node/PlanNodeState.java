package cn.zhangyis.db.sql.executor.node;

/** PlanNode 的单语句生命周期；任何逆向转换都属于执行协议错误。 */
public enum PlanNodeState {
    /** 尚未打开，没有运行期资源。 */
    NEW,
    /** 已打开，可 advance/current。 */
    OPEN,
    /** 已到 EOF，但资源仍等待 close。 */
    EXHAUSTED,
    /** 已逆序释放全部 child/cursor 资源。 */
    CLOSED
}
