package cn.zhangyis.db.storage.flush.cleaner;

import cn.zhangyis.db.storage.flush.FlushCycleResult;

import java.time.Duration;
import java.util.Optional;

/**
 * Supervisor 视角下的 page cleaner worker 控制端口。
 *
 * <p>真实实现是 {@link PageCleanerWorker}；测试可以注入脚本化实现验证失败重启，而不需要构造人为损坏的 IO 栈。
 */
public interface PageCleanerWorkerHandle extends AutoCloseable {

    /** 启动后台 worker。 */
    void start();

    /** 提交一次后台 flush 请求。 */
    void requestFlush(int maxPages);

    /** 等待 worker 空闲或进入终态。 */
    boolean awaitIdle(Duration timeout);

    /** 请求 worker 停止。 */
    boolean stop(Duration timeout);

    /** 当前 worker 状态。 */
    PageCleanerState state();

    /** 最近成功 cycle。 */
    Optional<FlushCycleResult> lastCycle();

    /** 当前 worker 诊断快照。 */
    PageCleanerWorkerSnapshot snapshot();

    @Override
    void close();
}
