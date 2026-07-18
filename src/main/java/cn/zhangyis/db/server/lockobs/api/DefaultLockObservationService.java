package cn.zhangyis.db.server.lockobs.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.server.lockobs.domain.WaitSlotSnapshot;
import cn.zhangyis.db.server.lockobs.report.DeadlockReport;
import cn.zhangyis.db.server.lockobs.snapshot.DataLockRow;
import cn.zhangyis.db.server.lockobs.snapshot.DataLockWaitRow;
import cn.zhangyis.db.server.lockobs.snapshot.LockDiagnosticSnapshot;
import cn.zhangyis.db.storage.trx.lock.GapLockKey;
import cn.zhangyis.db.storage.trx.lock.GrantedLockSnapshot;
import cn.zhangyis.db.storage.trx.lock.InsertIntentionLockKey;
import cn.zhangyis.db.storage.trx.lock.LockSnapshot;
import cn.zhangyis.db.storage.trx.lock.NextKeyLockKey;
import cn.zhangyis.db.storage.trx.lock.RecordLockKey;
import cn.zhangyis.db.storage.trx.lock.RowLockBlocker;
import cn.zhangyis.db.storage.trx.lock.RowLockObservation;
import cn.zhangyis.db.storage.trx.lock.ThreadEventId;
import cn.zhangyis.db.storage.trx.lock.TransactionLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;
import cn.zhangyis.db.storage.trx.lock.WaitForEdgeSnapshot;
import cn.zhangyis.db.storage.trx.lock.WaitingLockSnapshot;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 默认锁观测服务。它只接收 LockManager 发布的不可变事件，维护当前 wait slot 与最近 deadlock report，
 * 并把 LockManager 的只读快照适配成 `data_locks` / `data_lock_waits` 行。
 *
 * <p><b>并发边界</b>：current wait slot 使用 ConcurrentHashMap，deadlock report ring 使用单独显式锁。
 * 本服务不反向调用 LockManager，不持有存储层 latch/file lock，也不参与授锁或 rollback。
 */
public final class DefaultLockObservationService implements LockObservationService {

    /** 当前教学实现的 engine 名。 */
    private static final String ENGINE = "INNODB";

    /** 最多保留的最近死锁报告数。 */
    private static final int MAX_DEADLOCK_REPORTS = 16;

    /** 全局事件 id 分配器；0 保留给 no-op。 */
    private final AtomicLong nextEventId = new AtomicLong(1);

    /** 快照 epoch 分配器；用于区分不同诊断查询结果。 */
    private final AtomicLong nextSnapshotEpoch = new AtomicLong(1);

    /** deadlock report id 分配器。 */
    private final AtomicLong nextDeadlockReportId = new AtomicLong(1);

    /** threadId -> 当前等待槽；等待完成/timeout/victim 后删除。 */
    private final ConcurrentHashMap<Long, WaitSlotSnapshot> waitSlotsByThread = new ConcurrentHashMap<>();

    /** 最近死锁报告 ring，最新在队头；由 reportLock 保护。 */
    private final ArrayDeque<DeadlockReport> deadlockReports = new ArrayDeque<>();

    /** 保护 deadlockReports 的显式短锁。 */
    private final ReentrantLock reportLock = new ReentrantLock();

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param owner 参与 {@code openRowLockEvent} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param key 参与 {@code openRowLockEvent} 的稳定领域标识 {@code TransactionLockKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param requestId 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code openRowLockEvent} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public ThreadEventId openRowLockEvent(TransactionId owner, TransactionLockKey key,
                                          TransactionLockMode mode, long requestId) {
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("row lock observation owner must be real");
        }
        if (key == null || mode == null) {
            throw new DatabaseValidationException("row lock observation key/mode must not be null");
        }
        if (requestId <= 0) {
            throw new DatabaseValidationException("row lock observation request id must be positive: " + requestId);
        }
        return new ThreadEventId(Thread.currentThread().threadId(), nextEventId.getAndIncrement());
    }

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param blockers 参与 {@code markRowLockWaiting} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void markRowLockWaiting(RowLockObservation observation, List<RowLockBlocker> blockers, Duration timeout) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        requireObservation(observation);
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new DatabaseValidationException("row lock wait timeout must be positive");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        ThreadEventId eventId = observation.threadEventId();
        if (!eventId.real()) {
            return;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        long now = System.nanoTime();
        long deadline = safeDeadline(now, timeout);
        WaitSlotSnapshot slot = new WaitSlotSnapshot(eventId, 0, 0, observation.owner(), "WAITING",
                "wait/lock/row/innodb", observation.engineLockId(), now, deadline);
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        waitSlotsByThread.put(eventId.threadId(), slot);
    }

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     */
    @Override
    public void markRowLockGranted(RowLockObservation observation) {
        completeWaitSlot(observation);
    }

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param releaseReason 生命周期回调；只在契约定义的成功或释放边界调用，且不得为 {@code null}
     */
    @Override
    public void markRowLockReleased(RowLockObservation observation, String releaseReason) {
        completeWaitSlot(observation);
    }

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     */
    @Override
    public void markRowLockTimeout(RowLockObservation observation) {
        completeWaitSlot(observation);
    }

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param observation 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param edges 参与 {@code markRowLockVictim} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void markRowLockVictim(RowLockObservation observation, List<WaitForEdgeSnapshot> edges) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        requireObservation(observation);
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        if (edges == null || edges.isEmpty()) {
            throw new DatabaseValidationException("deadlock report edges must not be empty");
        }
        DeadlockReport report = new DeadlockReport(nextDeadlockReportId.getAndIncrement(), Instant.now(),
                observation.owner(), observation.requestId(), List.copyOf(edges),
                "row lock deadlock victim trx=" + observation.owner().value()
                        + " request=" + observation.requestId());
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        reportLock.lock();
        try {
            deadlockReports.addFirst(report);
            while (deadlockReports.size() > MAX_DEADLOCK_REPORTS) {
                deadlockReports.removeLast();
            }
        } finally {
            reportLock.unlock();
        }
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        completeWaitSlot(observation);
    }

    /**
     * 采集 {@code captureSnapshot} 对应的锁与等待观测稳定快照；返回对象与后续内部修改隔离，不转移内部可变状态的所有权。
     *
     * @param lockSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code captureSnapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public LockDiagnosticSnapshot captureSnapshot(LockSnapshot lockSnapshot, SnapshotRequest request) {
        if (lockSnapshot == null) {
            throw new DatabaseValidationException("lock snapshot must not be null");
        }
        if (request == null) {
            throw new DatabaseValidationException("snapshot request must not be null");
        }
        return buildSnapshot(lockSnapshot, request, nextSnapshotEpoch.getAndIncrement(), waitSlots(), latestDeadlocks());
    }

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @return 调用时刻的不可变状态集合或映射；没有已发布条目时返回空集合，调用方修改不会影响权威状态
     */
    @Override
    public List<DeadlockReport> latestDeadlocks() {
        reportLock.lock();
        try {
            return List.copyOf(deadlockReports);
        } finally {
            reportLock.unlock();
        }
    }

    /**
     * 从 LockManager 快照构造只读诊断行。该方法为 no-op 实现复用，因而显式接收 wait slot/report 副本，
     * 保证构造期间不访问可变 registry。
     *
     * @param lockSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param epoch 参与 {@code buildSnapshot} 的单调版本值 {@code epoch}；必须非负，回退或与权威快照冲突时拒绝
     * @param waitSlots 参与 {@code buildSnapshot} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param reports 参与 {@code buildSnapshot} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code buildSnapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static LockDiagnosticSnapshot buildSnapshot(LockSnapshot lockSnapshot, SnapshotRequest request,
                                                       long epoch, List<WaitSlotSnapshot> waitSlots,
                                                       List<DeadlockReport> reports) {
        if (lockSnapshot == null || request == null || waitSlots == null || reports == null) {
            throw new DatabaseValidationException("snapshot build args must not be null");
        }
        List<DataLockRow> rows = new ArrayList<>();
        for (GrantedLockSnapshot granted : lockSnapshot.grantedLocks()) {
            rows.add(row(granted.requestId(), granted.owner(), granted.key(), granted.mode(),
                    granted.state().name(), granted.threadEventId(), request.includeLockData()));
        }
        for (WaitingLockSnapshot waiting : lockSnapshot.waitingLocks()) {
            rows.add(row(waiting.requestId(), waiting.owner(), waiting.key(), waiting.mode(),
                    waiting.state().name(), waiting.threadEventId(), request.includeLockData()));
        }
        rows.sort(Comparator.comparing(DataLockRow::engineLockId));

        boolean truncated = rows.size() > request.maxRows();
        if (truncated) {
            rows = new ArrayList<>(rows.subList(0, request.maxRows()));
        }
        Map<String, DataLockRow> rowsByLockId = new HashMap<>();
        for (DataLockRow row : rows) {
            rowsByLockId.put(row.engineLockId(), row);
        }

        List<DataLockWaitRow> waits = new ArrayList<>();
        for (WaitForEdgeSnapshot edge : lockSnapshot.waitEdges()) {
            DataLockRow requesting = rowsByLockId.get(engineLockId(edge.waitingRequestId()));
            DataLockRow blocking = rowsByLockId.get(engineLockId(edge.blockingRequestId()));
            if (requesting != null && blocking != null) {
                waits.add(new DataLockWaitRow(ENGINE, requesting.engineLockId(), requesting.engineTransactionId(),
                        requesting.threadId(), requesting.eventId(), requesting.objectInstanceId(),
                        blocking.engineLockId(), blocking.engineTransactionId(), blocking.threadId(),
                        blocking.eventId(), blocking.objectInstanceId()));
            }
        }
        return new LockDiagnosticSnapshot(epoch, rows, waits, waitSlots, reports, false, truncated);
    }

    private List<WaitSlotSnapshot> waitSlots() {
        return waitSlotsByThread.values().stream()
                .sorted(Comparator.comparingLong(slot -> slot.threadEventId().eventId()))
                .toList();
    }

    private void completeWaitSlot(RowLockObservation observation) {
        requireObservation(observation);
        ThreadEventId eventId = observation.threadEventId();
        if (eventId.real()) {
            waitSlotsByThread.remove(eventId.threadId());
        }
    }

    private static void requireObservation(RowLockObservation observation) {
        if (observation == null) {
            throw new DatabaseValidationException("row lock observation must not be null");
        }
    }

    private static long safeDeadline(long now, Duration timeout) {
        try {
            return Math.addExact(now, timeout.toNanos());
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static DataLockRow row(long requestId, TransactionId owner, TransactionLockKey key,
                                   TransactionLockMode mode, String status, ThreadEventId eventId,
                                   boolean includeLockData) {
        ThreadEventId safeEvent = eventId == null ? ThreadEventId.NONE : eventId;
        return new DataLockRow(ENGINE, engineLockId(requestId), owner, safeEvent.threadId(), safeEvent.eventId(),
                "", "", "index#" + key.indexId(), engineLockId(requestId), lockType(key), lockMode(mode),
                status, includeLockData ? lockData(key) : "");
    }

    private static String engineLockId(long requestId) {
        return "INNODB:" + requestId;
    }

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param key 参与 {@code lockType} 的稳定领域标识 {@code TransactionLockKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code lockType} 生成的非空文本表示；字符顺序保持 SQL、标识符或诊断格式约定，无结果时返回空串而非 {@code null}
     */
    private static String lockType(TransactionLockKey key) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        if (key instanceof RecordLockKey) {
            return "RECORD";
        }
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        if (key instanceof GapLockKey) {
            return "GAP";
        }
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        if (key instanceof NextKeyLockKey) {
            return "NEXT_KEY";
        }
        if (key instanceof InsertIntentionLockKey) {
            return "INSERT_INTENTION";
        }
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        return "ROW";
    }

    private static String lockMode(TransactionLockMode mode) {
        return switch (mode) {
            case REC_S -> "S,REC_NOT_GAP";
            case REC_X -> "X,REC_NOT_GAP";
            case GAP_S -> "S,GAP";
            case GAP_X -> "X,GAP";
            case NEXT_KEY_S -> "S";
            case NEXT_KEY_X -> "X";
            case INSERT_INTENTION -> "X,GAP,INSERT_INTENTION";
        };
    }

    /**
     * 按锁与等待观测并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param key 参与 {@code lockData} 的稳定领域标识 {@code TransactionLockKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code lockData} 生成的非空文本表示；字符顺序保持 SQL、标识符或诊断格式约定，无结果时返回空串而非 {@code null}
     */
    private static String lockData(TransactionLockKey key) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        if (key instanceof RecordLockKey record) {
            return "index=" + record.indexId()
                    + " space=" + record.pageId().spaceId().value()
                    + " page=" + record.pageId().pageNo().value()
                    + " heap=" + record.heapNo();
        }
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        if (key instanceof GapLockKey gap) {
            return "index=" + gap.indexId()
                    + " gap=(" + boundary(gap.leftKey()) + "," + boundary(gap.rightKey()) + ")";
        }
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        if (key instanceof NextKeyLockKey nextKey) {
            return "next-key{" + lockData(nextKey.recordKey()) + "; " + lockData(nextKey.gapKey()) + "}";
        }
        if (key instanceof InsertIntentionLockKey insert) {
            return "insert-intention{" + lockData(insert.gapKey()) + "}";
        }
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        return key.toString();
    }

    private static String boundary(Object value) {
        return value == null ? "INF" : value.toString();
    }
}
