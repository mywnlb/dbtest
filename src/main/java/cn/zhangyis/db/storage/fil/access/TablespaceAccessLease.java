package cn.zhangyis.db.storage.fil.access;
import cn.zhangyis.db.storage.fil.lock.ResourceGuard;


/**
 * 表空间操作级准入令牌。
 *
 * <p>共享令牌允许普通页访问、元数据装载和 flush 并发进行；独占令牌用于
 * truncate、discard、drop 等必须阻断新物理访问的生命周期操作。同一
 * {@code SpaceId} 的共享与独占令牌互斥，不同表空间彼此独立，因此该令牌不会把
 * 全部数据文件串行化。</p>
 *
 * <p>令牌只保护当前进程内的操作生命周期，不证明 page0 持久状态或 registry
 * 快照仍可用于普通访问；调用方在获得令牌后仍须重新执行相应状态校验。令牌必须由
 * 获取线程关闭，通常交给 try-with-resources 或 MTR memo，确保异常路径也释放准入权。</p>
 */
public interface TablespaceAccessLease extends ResourceGuard {
}
