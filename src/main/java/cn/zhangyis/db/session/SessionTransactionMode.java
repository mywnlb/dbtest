package cn.zhangyis.db.session;

/** Session 的 SQL transaction owner 模式。 */
public enum SessionTransactionMode {
    NONE,
    AUTOCOMMIT_STATEMENT,
    IMPLICIT,
    EXPLICIT
}
