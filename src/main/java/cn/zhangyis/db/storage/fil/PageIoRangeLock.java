package cn.zhangyis.db.storage.fil;

/**
 * #4 页范围锁（预留、可选，本版不启用）。设计用于同页/相邻页写入合并、truncate 边界、故障注入。
 * 同页并发写正常由 Buffer Pool page latch 与 flush snapshot 控制（设计 §8.1），物理层一般不需要；故预留。
 * 加锁顺序位于 FileSize 之后、Fsync 之前。
 */
public interface PageIoRangeLock {
}
