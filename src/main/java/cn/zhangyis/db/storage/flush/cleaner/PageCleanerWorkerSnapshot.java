package cn.zhangyis.db.storage.flush.cleaner;

/**
 * Page cleaner worker 的只读诊断快照。
 *
 * <p>快照由 worker 在自身锁内复制生成，不包含锁、Condition 或线程对象引用。supervisor 只通过该值对象观察
 * worker 生命周期，避免把重启策略塞回 worker 内部。
 *
 * @param state 当前 worker 状态。
 * @param inFlight 是否有一轮 flush cycle 正在锁外执行。
 * @param queuedRequests 当前显式 flush 请求队列长度。
 * @param completedCycles 当前 worker 生命周期内成功完成的 flush cycle 数。
 * @param lastCyclePresent 是否已经有最近一次成功 cycle 结果。
 * @param failureMessage 失败诊断消息；无失败时为空字符串。
 */
public record PageCleanerWorkerSnapshot(PageCleanerState state,
                                        boolean inFlight,
                                        int queuedRequests,
                                        long completedCycles,
                                        boolean lastCyclePresent,
                                        String failureMessage) {
}
