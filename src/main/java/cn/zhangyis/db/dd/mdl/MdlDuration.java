package cn.zhangyis.db.dd.mdl;

/** metadata lock 的释放作用域；session 层后续按 statement/transaction 结束调用 releaseAll。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code STATEMENT}：表示“STATEMENT”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code TRANSACTION}：表示“事务”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code EXPLICIT}：表示“EXPLICIT”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum MdlDuration {
    STATEMENT,
    TRANSACTION,
    EXPLICIT
}
