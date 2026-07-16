package cn.zhangyis.db.sql.executor.storage;

/** Session 可选的提交 redo 持久性语义。 */
public enum SqlDurabilityMode {
    FLUSH_ON_COMMIT,
    WRITE_ON_COMMIT,
    BACKGROUND_FLUSH
}
