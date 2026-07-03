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

    @Override
    public void markRowLockWaiting(RowLockObservation observation, List<RowLockBlocker> blockers, Duration timeout) {
        requireObservation(observation);
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new DatabaseValidationException("row lock wait timeout must be positive");
        }
        ThreadEventId eventId = observation.threadEventId();
        if (!eventId.real()) {
            return;
        }
        long now = System.nanoTime();
        long deadline = safeDeadline(now, timeout);
        WaitSlotSnapshot slot = new WaitSlotSnapshot(eventId, 0, 0, observation.owner(), "WAITING",
                "wait/lock/row/innodb", observation.engineLockId(), now, deadline);
        waitSlotsByThread.put(eventId.threadId(), slot);
    }

    @Override
    public void markRowLockGranted(RowLockObservation observation) {
        completeWaitSlot(observation);
    }

    @Override
    public void markRowLockReleased(RowLockObservation observation, String releaseReason) {
        completeWaitSlot(observation);
    }

    @Override
    public void markRowLockTimeout(RowLockObservation observation) {
        completeWaitSlot(observation);
    }

    @Override
    public void markRowLockVictim(RowLockObservation observation, List<WaitForEdgeSnapshot> edges) {
        requireObservation(observation);
        if (edges == null || edges.isEmpty()) {
            throw new DatabaseValidationException("deadlock report edges must not be empty");
        }
        DeadlockReport report = new DeadlockReport(nextDeadlockReportId.getAndIncrement(), Instant.now(),
                observation.owner(), observation.requestId(), List.copyOf(edges),
                "row lock deadlock victim trx=" + observation.owner().value()
                        + " request=" + observation.requestId());
        reportLock.lock();
        try {
            deadlockReports.addFirst(report);
            while (deadlockReports.size() > MAX_DEADLOCK_REPORTS) {
                deadlockReports.removeLast();
            }
        } finally {
            reportLock.unlock();
        }
        completeWaitSlot(observation);
    }

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

    private static String lockType(TransactionLockKey key) {
        if (key instanceof RecordLockKey) {
            return "RECORD";
        }
        if (key instanceof GapLockKey) {
            return "GAP";
        }
        if (key instanceof NextKeyLockKey) {
            return "NEXT_KEY";
        }
        if (key instanceof InsertIntentionLockKey) {
            return "INSERT_INTENTION";
        }
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

    private static String lockData(TransactionLockKey key) {
        if (key instanceof RecordLockKey record) {
            return "index=" + record.indexId()
                    + " space=" + record.pageId().spaceId().value()
                    + " page=" + record.pageId().pageNo().value()
                    + " heap=" + record.heapNo();
        }
        if (key instanceof GapLockKey gap) {
            return "index=" + gap.indexId()
                    + " gap=(" + boundary(gap.leftKey()) + "," + boundary(gap.rightKey()) + ")";
        }
        if (key instanceof NextKeyLockKey nextKey) {
            return "next-key{" + lockData(nextKey.recordKey()) + "; " + lockData(nextKey.gapKey()) + "}";
        }
        if (key instanceof InsertIntentionLockKey insert) {
            return "insert-intention{" + lockData(insert.gapKey()) + "}";
        }
        return key.toString();
    }

    private static String boundary(Object value) {
        return value == null ? "INF" : value.toString();
    }
}
