package cn.zhangyis.db.sql.executor.storage;

/** Session 可选的提交 redo 持久性语义。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code FLUSH_ON_COMMIT}：表示“刷盘在提交”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code WRITE_ON_COMMIT}：表示“写入在提交”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code BACKGROUND_FLUSH}：表示“BACKGROUND刷盘”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum SqlDurabilityMode {
    FLUSH_ON_COMMIT,
    WRITE_ON_COMMIT,
    BACKGROUND_FLUSH
}
