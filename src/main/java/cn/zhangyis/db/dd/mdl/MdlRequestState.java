package cn.zhangyis.db.dd.mdl;

/** MDL request 显式状态；诊断快照不以布尔变量猜测等待/失败原因。 */
public enum MdlRequestState {
    REQUESTED,
    GRANTED,
    PENDING,
    VICTIM,
    TIMEOUT,
    KILLED,
    RELEASED
}
