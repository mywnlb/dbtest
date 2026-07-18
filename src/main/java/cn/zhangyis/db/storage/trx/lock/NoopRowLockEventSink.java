package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.domain.TransactionId;

import java.time.Duration;
import java.util.List;

/**
 * {@link RowLockEventSink} 的 no-op 实现。LockManager 未显式接线观测层时使用它，保证业务锁行为不变、
 * 不分配真实 thread/event id（返回 {@link ThreadEventId#NONE}）。诊断快照仍可由 LockManager 只读快照单独推导。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code INSTANCE}：无可变状态的单例值，避免为相同空值语义重复分配对象</li>
 * </ul>
 */
enum NoopRowLockEventSink implements RowLockEventSink {
    INSTANCE;

    /**
     * 按事务、MVCC 与锁并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param owner 参与 {@code openRowLockEvent} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param key 参与 {@code openRowLockEvent} 的稳定领域标识 {@code TransactionLockKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param requestId 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code openRowLockEvent} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    @Override
    public ThreadEventId openRowLockEvent(TransactionId owner, TransactionLockKey key,
                                          TransactionLockMode mode, long requestId) {
        return ThreadEventId.NONE;
    }

    /**
     * 按事务、MVCC 与锁并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param blockers 参与 {@code markRowLockWaiting} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     */
    @Override
    public void markRowLockWaiting(RowLockObservation observation, List<RowLockBlocker> blockers, Duration timeout) {
    }

    /**
     * 按事务、MVCC 与锁并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     */
    @Override
    public void markRowLockGranted(RowLockObservation observation) {
    }

    /**
     * 按事务、MVCC 与锁并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param releaseReason 生命周期回调；只在契约定义的成功或释放边界调用，且不得为 {@code null}
     */
    @Override
    public void markRowLockReleased(RowLockObservation observation, String releaseReason) {
    }

    /**
     * 按事务、MVCC 与锁并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     */
    @Override
    public void markRowLockTimeout(RowLockObservation observation) {
    }

    /**
     * 按事务、MVCC 与锁并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param edges 参与 {@code markRowLockVictim} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    @Override
    public void markRowLockVictim(RowLockObservation observation, List<WaitForEdgeSnapshot> edges) {
    }
}
