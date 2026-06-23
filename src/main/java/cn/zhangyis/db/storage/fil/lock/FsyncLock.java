package cn.zhangyis.db.storage.fil.lock;

/**
 * #5 fsync 限流锁（预留，本版不启用）。设计用于限制同一 data file 上并发 fsync 数量，避免后台 page cleaner
 * 重复刷同一文件。需待 flush/checkpoint 模块引入后台刷脏后才有意义；故预留。加锁顺序位于末位。
 */
public interface FsyncLock {
}
