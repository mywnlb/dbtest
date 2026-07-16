package cn.zhangyis.db.sql.executor.storage;

/** SQL 层不透明事务能力；实现不得向 parser/binder/executor 暴露真实 storage Transaction。 */
public interface SqlTransactionHandle {
}
