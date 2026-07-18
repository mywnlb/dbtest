package cn.zhangyis.db.session;

/** Session 的 SQL transaction owner 模式。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code NONE}：表示“无值哨兵”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code AUTOCOMMIT_STATEMENT}：表示“AUTOCOMMITSTATEMENT”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code IMPLICIT}：表示“IMPLICIT”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code EXPLICIT}：表示“EXPLICIT”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum SessionTransactionMode {
    NONE,
    AUTOCOMMIT_STATEMENT,
    IMPLICIT,
    EXPLICIT
}
