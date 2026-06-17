package cn.zhangyis.db.storage.fil;

/**
 * #2 文件句柄锁（预留，本版不启用）。设计用于保护 FileChannel/mmap/预分配句柄的打开、关闭、替换。
 * 首版每个表空间整生命周期持有单个 FileChannel、不替换句柄，因此尚未行使此锁；后续接入句柄替换/恢复重开时实现。
 * 加锁顺序位于 Lifecycle 之后、FileSize 之前。
 */
public interface DataFileHandleLock {
}
