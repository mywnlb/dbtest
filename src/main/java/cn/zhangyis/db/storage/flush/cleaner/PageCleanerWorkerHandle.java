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

    /** 提交一次后台 flush 请求。
     *
     * @param maxPages 参与 {@code requestFlush} 的上界或规格值 {@code maxPages}；必须非负且不能使容量、页数或编码长度计算溢出
     */
    void requestFlush(int maxPages);

    /** 等待 worker 空闲或进入终态。
     *
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return 在超时或取消前观察到 {@code awaitIdle} 的目标状态时为 {@code true}；等待期限届满且状态仍未满足时为 {@code false}
     */
    boolean awaitIdle(Duration timeout);

    /** 请求 worker 停止。
     *
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code stop} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     */
    boolean stop(Duration timeout);

    /** 当前 worker 状态。
     *
     * @return {@code state} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    PageCleanerState state();

    /** 最近成功 cycle。
     *
     * @return 当前可见的最近快照或持久边界；尚未产生对应状态时为空 {@code Optional}，从不返回 Java {@code null}
     */
    Optional<FlushCycleResult> lastCycle();

    /** 当前 worker 诊断快照。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    PageCleanerWorkerSnapshot snapshot();

    /**
     * 释放本方法拥有的脏页刷盘与 checkpoint资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    void close();
}
