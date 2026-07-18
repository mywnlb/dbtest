package cn.zhangyis.db.server.lockobs.api;

import cn.zhangyis.db.server.lockobs.report.DeadlockReport;
import cn.zhangyis.db.server.lockobs.snapshot.LockDiagnosticSnapshot;
import cn.zhangyis.db.storage.trx.lock.LockSnapshot;
import cn.zhangyis.db.storage.trx.lock.RowLockEventSink;

import java.util.List;

/**
 * server.lockobs 观测门面。它在 storage 锁层 {@link RowLockEventSink} 事件端口之上，追加 Performance Schema
 * 风格的只读诊断能力（{@code data_locks}/{@code data_lock_waits} 快照与最近 deadlock report）。
 *
 * <p><b>依赖方向</b>：本接口位于 server 层，只向下依赖 {@code storage.trx.lock} 的端口与只读快照类型；
 * LockManager 只认识 {@link RowLockEventSink}，不反向依赖本包，从而打破 storage ⇄ server 循环。观测层不能通过
 * 该门面授锁、释放锁或执行 rollback。
 */
public interface LockObservationService extends RowLockEventSink {

    /** 从 LockManager 只读快照构造诊断快照行。
     *
     * @param lockSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code captureSnapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    LockDiagnosticSnapshot captureSnapshot(LockSnapshot lockSnapshot, SnapshotRequest request);

    /** 最近 deadlock report，最新在列表首位。
     *
     * @return 调用时刻的不可变状态集合或映射；没有已发布条目时返回空集合，调用方修改不会影响权威状态
     */
    List<DeadlockReport> latestDeadlocks();
}
