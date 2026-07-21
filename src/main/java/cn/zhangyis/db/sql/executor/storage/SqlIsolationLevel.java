package cn.zhangyis.db.sql.executor.storage;

/** SQL/Session 稳定隔离级别；adapter 必须显式映射，不依赖枚举 ordinal。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code READ_UNCOMMITTED}：普通读不建 ReadView，读取当时最新未标删聚簇版本</li>
 *     <li>{@code READ_COMMITTED}：表示“读取COMMITTED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code REPEATABLE_READ}：表示“REPEATABLE读取”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code SERIALIZABLE}：显式/隐式事务普通 SELECT 由 Session 提升为 FOR SHARE</li>
 * </ul>
 */
public enum SqlIsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
}
