package cn.zhangyis.db.sql.executor.storage;

/**
 * SQL/Session 可持有的保存点不透明能力。实现必须同时封装 undo 与锁边界，
 * 不得向 parser、binder、executor 或 Session 暴露真实 Transaction、索引或物理日志位置。
 */
public interface SqlSavepointHandle {
}
