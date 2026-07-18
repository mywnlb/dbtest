package cn.zhangyis.db.sql.executor.storage;

/** v1 已实现一致性读语义的隔离级别；adapter 必须显式映射，不依赖枚举 ordinal。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code READ_COMMITTED}：表示“读取COMMITTED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code REPEATABLE_READ}：表示“REPEATABLE读取”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum SqlIsolationLevel {
    READ_COMMITTED,
    REPEATABLE_READ
}
