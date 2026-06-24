package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;

/**
 * 后台 redo flusher 的最小驱动端口。{@link RedoFlushWorker} 只依赖本端口的三个查询/动作，便于单测注入 fake
 * 验证空转跳过与失败语义，而不引入 {@link RedoLogManager} 的全部表面或 DurabilityPolicy/锁拆分等后续关注点。
 *
 * <p>生产实现是 {@link RedoLogManagerFlushTarget}，仅委托 {@link RedoLogManager} 的同名方法。
 */
public interface RedoFlushTarget {

    /** 下一个空闲 LSN（已分配边界）。worker 用它与 {@link #flushedToDiskLsn()} 比较判断是否有待刷 redo。 */
    Lsn currentLsn();

    /** 已 fsync 的最高 LSN。 */
    Lsn flushedToDiskLsn();

    /** 同步写出并 fsync 待刷 redo，返回推进后的 durable LSN；IO 失败抛 {@link RedoLogIoException}。 */
    Lsn flush();
}
