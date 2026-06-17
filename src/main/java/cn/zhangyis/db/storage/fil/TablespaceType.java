package cn.zhangyis.db.storage.fil;

/**
 * 表空间类型。用于区分 system、独立表空间、general tablespace、undo 和 temporary 的加载来源与扩展策略。
 */
public enum TablespaceType {
    /**
     * 系统表空间。承载 InnoDB 系统级元数据和部分内部结构，通常在实例启动时由固定配置加载。
     */
    SYSTEM,

    /**
     * 独立表空间。对应 file-per-table 的 .ibd 文件，是普通用户表最常见的数据文件形态。
     */
    FILE_PER_TABLE,

    /**
     * 通用表空间。一个 tablespace 可承载多个表或索引，元数据需要结合数据字典判断对象归属。
     */
    GENERAL,

    /**
     * Undo 表空间。承载 undo segment/page，扩展策略和普通用户表空间不同，恢复和 purge 会重点访问。
     */
    UNDO,

    /**
     * 临时表空间。用于临时对象和内部临时结构，通常允许简化 redo 持久化语义，实例重启后可重建。
     */
    TEMPORARY
}
