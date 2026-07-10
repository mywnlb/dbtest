package cn.zhangyis.db.storage.trx;

/**
 * undo segment 进入物理终态的业务原因。该枚举只在事务包内驱动状态校验和测试 crash point，
 * 不进入持久格式；恢复权威仍是 page3 slot 与 undo first-page header。
 */
enum UndoFinalizationKind {
    /** 纯 INSERT 事务提交，insert undo 不再服务 MVCC。 */
    INSERT_COMMIT,
    /** live 事务完整回滚到 EMPTY 后回收。 */
    LIVE_ROLLBACK,
    /** crash recovery 把 ACTIVE undo 回滚到 EMPTY 后回收。 */
    RECOVERY_ROLLBACK,
    /** purge 已清理 committed update/delete undo 对应记录后回收。 */
    PURGE
}
