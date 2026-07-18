package cn.zhangyis.db.session;

/** Session 生命周期；FAILED 只允许 close，CLOSING/CLOSED 拒绝新语句。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code OPEN}：表示“OPEN”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code EXECUTING}：表示“EXECUTING”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code FAILED}：表示“FAILED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code CLOSING}：表示“CLOSING”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code CLOSED}：表示“CLOSED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum SessionState { OPEN, EXECUTING, FAILED, CLOSING, CLOSED }
