/**
 * fil 层物理资源锁与 RAII guard。
 *
 * <p>本包当前只表达单数据文件的生命周期、文件大小变更和 fsync 串行化：
 * {@code TablespaceLifecycleLatch -> FileSizeLock -> FsyncLock} 是物理资源的概念层级，
 * 具体路径只获取完成自身操作所需的子集。未实现 handle replacement lock 或 page-range lock，
 * 同页内容并发由上层 Buffer Pool/page latch 协调。</p>
 *
 * <p>这些锁不承载 record/事务锁语义，也不进入事务 Wait-For Graph。调用方不得持有它们等待
 * 事务行锁；所有获取和释放均通过显式并发工具及 {@code ResourceGuard} 表达。</p>
 */
package cn.zhangyis.db.storage.fil.lock;
