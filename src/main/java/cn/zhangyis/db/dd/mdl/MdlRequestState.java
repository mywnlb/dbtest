package cn.zhangyis.db.dd.mdl;

/** MDL request 显式状态；诊断快照不以布尔变量猜测等待/失败原因。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code REQUESTED}：表示“REQUESTED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code GRANTED}：表示“GRANTED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code PENDING}：表示“PENDING”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code VICTIM}：表示“VICTIM”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code TIMEOUT}：表示“超时”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code KILLED}：表示“KILLED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code RELEASED}：表示“RELEASED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum MdlRequestState {
    REQUESTED,
    GRANTED,
    PENDING,
    VICTIM,
    TIMEOUT,
    KILLED,
    RELEASED
}
