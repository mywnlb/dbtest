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

    /** 创建 row-lock 请求事件，返回 thread/event id。
     *
     * @param owner 参与 {@code openRowLockEvent} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param key 参与 {@code openRowLockEvent} 的稳定领域标识 {@code TransactionLockKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param requestId 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code openRowLockEvent} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    ThreadEventId openRowLockEvent(TransactionId owner, TransactionLockKey key,
                                   TransactionLockMode mode, long requestId);

    /** 请求进入等待队列后登记当前 wait slot 和 blocker 摘要。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param blockers 参与 {@code markRowLockWaiting} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     */
    void markRowLockWaiting(RowLockObservation observation, List<RowLockBlocker> blockers, Duration timeout);

    /** 请求被授予后清理当前 wait slot；立即授予时没有 wait slot，调用应为 no-op。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     */
    void markRowLockGranted(RowLockObservation observation);

    /** 已授予锁释放或等待请求被清理后清理观测元数据。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param releaseReason 生命周期回调；只在契约定义的成功或释放边界调用，且不得为 {@code null}
     */
    void markRowLockReleased(RowLockObservation observation, String releaseReason);

    /** 等待超时后完成 wait slot 并保留 timeout 计数扩展点。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     */
    void markRowLockTimeout(RowLockObservation observation);

    /** 当前等待请求被选为 deadlock victim 后保存最近死锁报告并完成 wait slot。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param edges 参与 {@code markRowLockVictim} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    void markRowLockVictim(RowLockObservation observation, List<WaitForEdgeSnapshot> edges);

    /** 未接观测层时使用的 no-op 端口实例。 */
    static RowLockEventSink noop() {
        return NoopRowLockEventSink.INSTANCE;
    }
}
