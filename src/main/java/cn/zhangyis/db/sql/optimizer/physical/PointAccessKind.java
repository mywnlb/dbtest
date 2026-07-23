package cn.zhangyis.db.sql.optimizer.physical;

/**
 * 点查物理访问种类。该枚举只表达 SQL/DD 层选出的路径，不泄露 storage descriptor 或
 * secondary record layout。
 */
public enum PointAccessKind {

    /** 完整、无 prefix 的聚簇主键点查。 */
    CLUSTERED_PRIMARY,

    /** 完整、无 prefix 的逻辑唯一二级键点查，并由 Data Port 完成回表。 */
    UNIQUE_SECONDARY
}
