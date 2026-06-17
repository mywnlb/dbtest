package cn.zhangyis.db.storage.buf;

/**
 * 页内容访问模式（page latch 模式）。SHARED 允许多读并发；EXCLUSIVE 排他，写入必须持有它。
 */
public enum PageLatchMode {
    /** 共享读：多个持有者可并发读同一页内容。 */
    SHARED,
    /** 排他写：独占页内容，写操作必须持有此模式。 */
    EXCLUSIVE
}
