package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.domain.TransactionId;

import java.time.Duration;
import java.util.List;

/**
 * row-lock 事件汇聚端口。{@link LockManager} 只通过该接口向观测层单向发布不可变 row-lock 事实：
 * 打开事件、进入等待、授予、释放、超时、死锁 victim；观测层不能通过这个端口授锁、释放锁或执行 rollback。
 *
 * <p><b>分层约束</b>：端口与其事件载荷（{@link RowLockObservation}/{@link RowLockBlocker}/{@link ThreadEventId}）
 * 都定义在 storage 锁层，使 LockManager 不必反向依赖上层 server.lockobs。真正的 Performance Schema 诊断适配
 * （{@code data_locks}/{@code data_lock_waits}、wait slot、deadlock report）由 server.lockobs 向下实现该端口。
 */
public interface RowLockEventSink {

    /** 创建 row-lock 请求事件，返回 thread/event id。 */
    ThreadEventId openRowLockEvent(TransactionId owner, TransactionLockKey key,
                                   TransactionLockMode mode, long requestId);

    /** 请求进入等待队列后登记当前 wait slot 和 blocker 摘要。 */
    void markRowLockWaiting(RowLockObservation observation, List<RowLockBlocker> blockers, Duration timeout);

    /** 请求被授予后清理当前 wait slot；立即授予时没有 wait slot，调用应为 no-op。 */
    void markRowLockGranted(RowLockObservation observation);

    /** 已授予锁释放或等待请求被清理后清理观测元数据。 */
    void markRowLockReleased(RowLockObservation observation, String releaseReason);

    /** 等待超时后完成 wait slot 并保留 timeout 计数扩展点。 */
    void markRowLockTimeout(RowLockObservation observation);

    /** 当前等待请求被选为 deadlock victim 后保存最近死锁报告并完成 wait slot。 */
    void markRowLockVictim(RowLockObservation observation, List<WaitForEdgeSnapshot> edges);

    /** 未接观测层时使用的 no-op 端口实例。 */
    static RowLockEventSink noop() {
        return NoopRowLockEventSink.INSTANCE;
    }
}
