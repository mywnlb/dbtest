package cn.zhangyis.db.sql.parser.ast;

/**
 * SELECT 尾部锁定读声明。Parser 只保留用户语法意图；Binder 再结合访问路径决定当前切片是否支持该形状。
 */
public enum SelectLockingClause {
    /** 普通一致性读，由隔离级别决定 ReadView 生命周期。 */
    NONE,
    /** 读取当前版本并申请共享事务锁。 */
    FOR_SHARE,
    /** 读取当前版本并申请排他事务锁。 */
    FOR_UPDATE
}
