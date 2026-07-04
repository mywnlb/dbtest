package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SequencedSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务锁内核。它维护内存级 record/gap/next-key/insert-intention 锁表、等待队列、事务持锁集合与
 * row-lock wait-for graph；不持有 page latch，不读取 BufferFrame，也不执行事务 rollback。
 *
 * <p>并发边界：锁表按 indexId 分片，分片内 {@link LockShard#mutex} 保护 granted/waiting/heldByOwner；
 * wait-for graph 由独立 {@link WaitForGraph#mutex} 保护。所有需要同时修改锁表与图的路径都遵循
 * “先分片 mutex，后 graph mutex”的顺序，禁止反向获取，避免死锁检测自身引入 Java 层死锁。
 */
public final class LockManager {

    /** 默认分片数；当前单聚簇 DML 已接入，16 个索引级分片足以表达教学版锁表并发边界。 */
    private static final int DEFAULT_SHARD_COUNT = 16;

    /** 默认死锁搜索步数上限，避免异常长链在持 graph mutex 时无界遍历。 */
    private static final int DEFAULT_DEADLOCK_SEARCH_LIMIT = 64;

    /** 内部请求 id 分配器，仅用于精确释放某个 LockHandle。 */
    private final AtomicLong nextRequestId = new AtomicLong(1);

    /** 按 indexId 路由的锁表分片。 */
    private final LockShard[] shards;

    /** row-lock wait-for graph；不包含 page latch、redo/file 等物理等待。 */
    private final WaitForGraph waitForGraph = new WaitForGraph();

    /** row-lock 事件汇聚端口；默认 no-op，生产 StorageEngine 注入 server.lockobs 的实现。 */
    private final RowLockEventSink observation;

    /** 每次死锁检测最多遍历的 graph 边/点预算。 */
    private final int deadlockSearchLimit;

    /**
     * 创建默认配置的 LockManager。生产组合根通常使用带观测端口的构造器；本构造器保留给无观测需求的测试和嵌入式使用。
     */
    public LockManager() {
        this(DEFAULT_SHARD_COUNT, DEFAULT_DEADLOCK_SEARCH_LIMIT, RowLockEventSink.noop());
    }

    /**
     * 创建接入观测端口的默认配置 LockManager。
     *
     * @param observation row-lock 事件汇聚端口，不能为 null。
     */
    public LockManager(RowLockEventSink observation) {
        this(DEFAULT_SHARD_COUNT, DEFAULT_DEADLOCK_SEARCH_LIMIT, observation);
    }

    /**
     * 创建可测试配置的 LockManager。
     *
     * @param shardCount              锁表分片数，必须为正。
     * @param deadlockSearchLimit     死锁检测搜索预算，必须为正。
     */
    public LockManager(int shardCount, int deadlockSearchLimit) {
        this(shardCount, deadlockSearchLimit, RowLockEventSink.noop());
    }

    /**
     * 创建可测试配置的 LockManager，并显式接入观测端口。
     *
     * @param shardCount              锁表分片数，必须为正。
     * @param deadlockSearchLimit     死锁检测搜索预算，必须为正。
     * @param observation             row-lock 事件汇聚端口，不能为 null。
     */
    public LockManager(int shardCount, int deadlockSearchLimit, RowLockEventSink observation) {
        if (shardCount <= 0) {
            throw new DatabaseValidationException("lock manager shardCount must be positive: " + shardCount);
        }
        if (deadlockSearchLimit <= 0) {
            throw new DatabaseValidationException(
                    "lock manager deadlockSearchLimit must be positive: " + deadlockSearchLimit);
        }
        this.shards = new LockShard[shardCount];
        for (int i = 0; i < shardCount; i++) {
            this.shards[i] = new LockShard();
        }
        this.deadlockSearchLimit = deadlockSearchLimit;
        if (observation == null) {
            throw new DatabaseValidationException("lock observation service must not be null");
        }
        this.observation = observation;
    }

    /**
     * 申请事务锁。数据流为：调用方给出事务 id、锁 key、模式与 timeout；LockManager 校验 key/mode 后进入
     * index 分片，若与已授予锁兼容则立即登记到 granted 与 heldByOwner；否则入 wait queue、写入 wait-for
     * graph 并执行有界死锁检测。等待期间线程只挂在事务锁分片 Condition 上，不持有任何 page latch。
     *
     * @param owner   申请锁的真实事务 id，不能为 NONE。
     * @param key     锁资源。
     * @param mode    锁模式，必须与 key 类型匹配。
     * @param timeout 最大等待时间，必须为正。
     * @return 已授予锁的释放句柄。
     * @throws LockWaitTimeoutException 等待超时或等待线程被中断时抛出，抛出前已清理 wait queue 与 graph。
     * @throws DeadlockDetectedException 当前等待请求形成 row-lock 环时抛出，victim 为当前请求。
     */
    public LockHandle acquire(TransactionId owner, TransactionLockKey key,
                              TransactionLockMode mode, Duration timeout) {
        validateAcquire(owner, key, mode, timeout);
        long remainingNanos = timeoutNanos(timeout);
        long requestId = nextRequestId.getAndIncrement();
        ThreadEventId threadEventId = observation.openRowLockEvent(owner, key, mode, requestId);
        LockShard shard = shardFor(key);
        LockRequest request;
        shard.mutex.lock();
        try {
            List<LockRequest> blockers = blockingRequests(shard, owner, key, mode);
            request = new LockRequest(requestId, owner, key, mode, threadEventId,
                    shard.mutex.newCondition());
            if (blockers.isEmpty()) {
                grantRequest(shard, request);
                observation.markRowLockGranted(request.observation());
                return request.handle;
            }
            request.state = TransactionLockState.WAITING;
            shard.waiting.addLast(request);
            waitForGraph.replaceWaitEdges(request, blockers);
            observation.markRowLockWaiting(request.observation(), blockerSummaries(blockers), timeout);
            if (waitForGraph.introducesCycle(owner, blockerOwners(blockers), deadlockSearchLimit)) {
                observation.markRowLockVictim(request.observation(), waitForGraph.snapshotEdges());
                removeWaitingRequest(shard, request, TransactionLockState.VICTIM);
                throw new DeadlockDetectedException("deadlock detected for transaction " + owner.value());
            }
            while (request.state == TransactionLockState.WAITING) {
                if (remainingNanos <= 0) {
                    removeWaitingRequest(shard, request, TransactionLockState.TIMEOUT);
                    throw new LockWaitTimeoutException("lock wait timeout for transaction " + owner.value());
                }
                try {
                    remainingNanos = request.condition.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    removeWaitingRequest(shard, request, TransactionLockState.TIMEOUT);
                    throw new LockWaitTimeoutException(
                            "lock wait interrupted for transaction " + owner.value(), e);
                }
            }
            if (request.state == TransactionLockState.GRANTED) {
                return request.handle;
            }
            if (request.state == TransactionLockState.VICTIM) {
                throw new DeadlockDetectedException("deadlock victim transaction " + owner.value());
            }
            throw new LockWaitTimeoutException("lock wait cancelled for transaction " + owner.value());
        } finally {
            shard.mutex.unlock();
        }
    }

    /**
     * 释放一个已授予锁句柄。释放后会扫描同一分片的等待队列，授予当前已兼容的请求并唤醒等待线程。
     * 该方法幂等：同一句柄重复释放、或 releaseAll 已经释放后再次 close，都不会重复修改锁表。
     *
     * @param handle acquire 返回的句柄。
     */
    public void release(LockHandle handle) {
        if (handle == null) {
            throw new DatabaseValidationException("lock handle must not be null");
        }
        if (!handle.markClosedByCaller()) {
            return;
        }
        LockShard shard = shardFor(handle.key());
        shard.mutex.lock();
        try {
            LockRequest request = findGrantedRequest(shard, handle.requestId());
            if (request != null) {
                releaseGrantedRequest(shard, request);
                grantCompatibleWaiters(shard);
            }
        } finally {
            shard.mutex.unlock();
        }
    }

    /**
     * 释放某事务在所有分片上已经授予的锁，并取消该事务尚在等待队列中的请求。2.1 起
     * {@code ClusteredDmlService.commit/rollback} 会在事务结束收尾调用本入口；{@code TransactionManager}
     * 自身仍保持纯内存状态机，不自动持有本锁管理器依赖。
     *
     * @param owner 要清理的事务 id。
     * @return 实际释放的已授予锁数量；被取消的等待请求不计入该数量。
     */
    public int releaseAll(TransactionId owner) {
        validateOwner(owner);
        int released = 0;
        for (LockShard shard : shards) {
            shard.mutex.lock();
            try {
                released += releaseGrantedByOwner(shard, owner);
                cancelWaitingByOwner(shard, owner);
                grantCompatibleWaiters(shard);
            } finally {
                shard.mutex.unlock();
            }
        }
        waitForGraph.removeTransaction(owner);
        return released;
    }

    /**
     * 读取当前锁表和 wait-for graph 的不可变快照。快照用于测试与后续 lock observability adapter；
     * 它不允许调用方反向修改 LockManager 内部队列。
     *
     * @return 锁和等待边的只读快照。
     */
    public LockSnapshot snapshot() {
        List<GrantedLockSnapshot> granted = new ArrayList<>();
        List<WaitingLockSnapshot> waiting = new ArrayList<>();
        for (LockShard shard : shards) {
            shard.mutex.lock();
            try {
                for (LockRequest request : shard.granted) {
                    granted.add(new GrantedLockSnapshot(request.requestId, request.threadEventId,
                            request.owner, request.key, request.mode, request.state));
                }
                for (LockRequest request : shard.waiting) {
                    waiting.add(new WaitingLockSnapshot(request.requestId, request.threadEventId,
                            request.owner, request.key, request.mode, request.state));
                }
            } finally {
                shard.mutex.unlock();
            }
        }
        return new LockSnapshot(granted, waiting, waitForGraph.snapshotEdges());
    }

    private void validateAcquire(TransactionId owner, TransactionLockKey key,
                                 TransactionLockMode mode, Duration timeout) {
        validateOwner(owner);
        if (key == null) {
            throw new DatabaseValidationException("transaction lock key must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("transaction lock mode must not be null");
        }
        if (timeout == null) {
            throw new DatabaseValidationException("transaction lock timeout must not be null");
        }
        if (timeout.isNegative() || timeout.isZero()) {
            throw new DatabaseValidationException("transaction lock timeout must be positive: " + timeout);
        }
        if (!modeMatchesKey(key, mode)) {
            throw new DatabaseValidationException(
                    "transaction lock mode " + mode + " does not match key " + key.getClass().getSimpleName());
        }
    }

    private static void validateOwner(TransactionId owner) {
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("transaction lock owner must be a real transaction id");
        }
    }

    private static boolean modeMatchesKey(TransactionLockKey key, TransactionLockMode mode) {
        if (key instanceof RecordLockKey) {
            return mode == TransactionLockMode.REC_S || mode == TransactionLockMode.REC_X;
        }
        if (key instanceof GapLockKey) {
            return mode == TransactionLockMode.GAP_S || mode == TransactionLockMode.GAP_X;
        }
        if (key instanceof NextKeyLockKey) {
            return mode == TransactionLockMode.NEXT_KEY_S || mode == TransactionLockMode.NEXT_KEY_X;
        }
        if (key instanceof InsertIntentionLockKey) {
            return mode == TransactionLockMode.INSERT_INTENTION;
        }
        return false;
    }

    private static long timeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private LockShard shardFor(TransactionLockKey key) {
        int index = Math.floorMod(Long.hashCode(key.indexId()), shards.length);
        return shards[index];
    }

    private List<LockRequest> blockingRequests(LockShard shard, TransactionId owner,
                                               TransactionLockKey key, TransactionLockMode mode) {
        List<LockRequest> blockers = new ArrayList<>();
        for (LockRequest granted : shard.granted) {
            if (!granted.owner.equals(owner) && conflicts(granted.key, granted.mode, key, mode)) {
                blockers.add(granted);
            }
        }
        return List.copyOf(blockers);
    }

    private static List<TransactionId> blockerOwners(List<LockRequest> blockers) {
        LinkedHashSet<TransactionId> owners = new LinkedHashSet<>();
        for (LockRequest blocker : blockers) {
            owners.add(blocker.owner);
        }
        return List.copyOf(owners);
    }

    private static List<RowLockBlocker> blockerSummaries(List<LockRequest> blockers) {
        List<RowLockBlocker> summaries = new ArrayList<>(blockers.size());
        for (LockRequest blocker : blockers) {
            summaries.add(new RowLockBlocker(blocker.requestId, blocker.owner, blocker.key,
                    blocker.mode, blocker.threadEventId));
        }
        return List.copyOf(summaries);
    }

    private static boolean conflicts(TransactionLockKey heldKey, TransactionLockMode heldMode,
                                     TransactionLockKey requestedKey, TransactionLockMode requestedMode) {
        RecordLockKey heldRecord = recordPart(heldKey);
        RecordLockKey requestedRecord = recordPart(requestedKey);
        if (heldRecord != null && requestedRecord != null && heldRecord.equals(requestedRecord)
                && recordModesConflict(recordMode(heldMode), recordMode(requestedMode))) {
            return true;
        }

        GapLockKey heldGap = gapPart(heldKey);
        GapLockKey requestedGap = gapPart(requestedKey);
        if (heldGap != null && requestedGap != null && heldGap.equals(requestedGap)) {
            boolean heldInsert = heldMode == TransactionLockMode.INSERT_INTENTION;
            boolean requestedInsert = requestedMode == TransactionLockMode.INSERT_INTENTION;
            if (heldInsert && requestedInsert) {
                return false;
            }
            return heldInsert || requestedInsert;
        }
        return false;
    }

    private static RecordLockKey recordPart(TransactionLockKey key) {
        if (key instanceof RecordLockKey recordKey) {
            return recordKey;
        }
        if (key instanceof NextKeyLockKey nextKey) {
            return nextKey.recordKey();
        }
        return null;
    }

    private static GapLockKey gapPart(TransactionLockKey key) {
        if (key instanceof GapLockKey gapKey) {
            return gapKey;
        }
        if (key instanceof NextKeyLockKey nextKey) {
            return nextKey.gapKey();
        }
        if (key instanceof InsertIntentionLockKey insertIntention) {
            return insertIntention.gapKey();
        }
        return null;
    }

    private static TransactionLockMode recordMode(TransactionLockMode mode) {
        if (mode == TransactionLockMode.REC_S || mode == TransactionLockMode.NEXT_KEY_S) {
            return TransactionLockMode.REC_S;
        }
        if (mode == TransactionLockMode.REC_X || mode == TransactionLockMode.NEXT_KEY_X) {
            return TransactionLockMode.REC_X;
        }
        return null;
    }

    private static boolean recordModesConflict(TransactionLockMode heldMode, TransactionLockMode requestedMode) {
        if (heldMode == null || requestedMode == null) {
            return false;
        }
        return heldMode == TransactionLockMode.REC_X || requestedMode == TransactionLockMode.REC_X;
    }

    private void grantRequest(LockShard shard, LockRequest request) {
        request.state = TransactionLockState.GRANTED;
        request.handle = new LockHandle(this, request.requestId, request.owner, request.key, request.mode);
        shard.granted.add(request);
        shard.heldByOwner.computeIfAbsent(request.owner, ignored -> new LinkedHashSet<>()).add(request);
    }

    private void grantCompatibleWaiters(LockShard shard) {
        Iterator<LockRequest> iterator = shard.waiting.iterator();
        while (iterator.hasNext()) {
            LockRequest request = iterator.next();
            List<LockRequest> blockers = blockingRequests(shard, request.owner, request.key, request.mode);
            if (blockers.isEmpty()) {
                iterator.remove();
                waitForGraph.removeRequest(request.requestId);
                grantRequest(shard, request);
                observation.markRowLockGranted(request.observation());
                request.condition.signal();
            } else {
                waitForGraph.replaceWaitEdges(request, blockers);
            }
        }
    }

    private void removeWaitingRequest(LockShard shard, LockRequest request, TransactionLockState terminalState) {
        shard.waiting.remove(request);
        request.state = terminalState;
        waitForGraph.removeRequest(request.requestId);
        if (terminalState == TransactionLockState.TIMEOUT) {
            observation.markRowLockTimeout(request.observation());
        } else if (terminalState == TransactionLockState.RELEASED) {
            observation.markRowLockReleased(request.observation(), "WAIT_CANCELLED");
        }
        request.condition.signal();
    }

    private LockRequest findGrantedRequest(LockShard shard, long requestId) {
        for (LockRequest request : shard.granted) {
            if (request.requestId == requestId) {
                return request;
            }
        }
        return null;
    }

    private void releaseGrantedRequest(LockShard shard, LockRequest request) {
        if (shard.granted.remove(request)) {
            SequencedSet<LockRequest> ownerLocks = shard.heldByOwner.get(request.owner);
            if (ownerLocks != null) {
                ownerLocks.remove(request);
                if (ownerLocks.isEmpty()) {
                    shard.heldByOwner.remove(request.owner);
                }
            }
            request.state = TransactionLockState.RELEASED;
            if (request.handle != null) {
                request.handle.markReleasedByManager();
            }
            observation.markRowLockReleased(request.observation(), "RELEASE");
        }
    }

    private int releaseGrantedByOwner(LockShard shard, TransactionId owner) {
        SequencedSet<LockRequest> ownerLocks = shard.heldByOwner.remove(owner);
        if (ownerLocks == null || ownerLocks.isEmpty()) {
            return 0;
        }
        int released = 0;
        for (LockRequest request : List.copyOf(ownerLocks)) {
            if (shard.granted.remove(request)) {
                request.state = TransactionLockState.RELEASED;
                if (request.handle != null) {
                    request.handle.markReleasedByManager();
                }
                observation.markRowLockReleased(request.observation(), "RELEASE_ALL");
                released++;
            }
        }
        return released;
    }

    private void cancelWaitingByOwner(LockShard shard, TransactionId owner) {
        Iterator<LockRequest> iterator = shard.waiting.iterator();
        while (iterator.hasNext()) {
            LockRequest request = iterator.next();
            if (request.owner.equals(owner)) {
                iterator.remove();
                request.state = TransactionLockState.RELEASED;
                waitForGraph.removeRequest(request.requestId);
                observation.markRowLockReleased(request.observation(), "WAIT_CANCELLED_BY_OWNER");
                request.condition.signal();
            }
        }
    }

    /** 单个索引分片的锁表状态。所有字段只允许在持有 mutex 时读写。 */
    private static final class LockShard {

        /** 保护本分片 granted/waiting/heldByOwner 的显式 mutex。 */
        private final ReentrantLock mutex = new ReentrantLock();

        /** 本分片已经授予的锁请求。 */
        private final List<LockRequest> granted = new ArrayList<>();

        /** 本分片正在等待的锁请求，Condition 均绑定到 mutex。 */
        private final ArrayDeque<LockRequest> waiting = new ArrayDeque<>();

        /** 事务持锁集合索引，供 releaseAll 按事务批量释放。 */
        private final Map<TransactionId, SequencedSet<LockRequest>> heldByOwner = new HashMap<>();
    }

    /** 内部锁请求。可变状态由所属 LockShard.mutex 保护。 */
    private static final class LockRequest {

        /** 全局唯一请求 id，用于 handle 精确释放与 graph request 边定位。 */
        private final long requestId;

        /** 请求所属事务。 */
        private final TransactionId owner;

        /** 请求锁资源。 */
        private final TransactionLockKey key;

        /** 请求锁模式。 */
        private final TransactionLockMode mode;

        /** 观测层 thread/event id；未接 lockobs 时为 NONE。 */
        private final ThreadEventId threadEventId;

        /** 等待线程挂起/唤醒条件；必须在所属分片 mutex 内 await/signal。 */
        private final Condition condition;

        /** 请求状态，观测快照读取该字段。 */
        private TransactionLockState state;

        /** 授予后创建的释放句柄；等待期间为 null。 */
        private LockHandle handle;

        private LockRequest(long requestId, TransactionId owner, TransactionLockKey key,
                            TransactionLockMode mode, ThreadEventId threadEventId, Condition condition) {
            this.requestId = requestId;
            this.owner = owner;
            this.key = key;
            this.mode = mode;
            this.threadEventId = threadEventId;
            this.condition = condition;
            this.state = TransactionLockState.GRANTED;
        }

        private RowLockObservation observation() {
            return new RowLockObservation(requestId, owner, key, mode, threadEventId);
        }
    }

    /**
     * row-lock wait-for graph。它只记录事务锁等待边，且按 requestId 存储，避免同一事务未来出现多个等待请求时
     * 互相覆盖观测数据；死锁检测时再按事务聚合出 adjacency。
     */
    private static final class WaitForGraph {

        /** 保护 edgesByRequest 的显式 mutex；调用方可在持 LockShard.mutex 时获取它，但不能反向。 */
        private final ReentrantLock mutex = new ReentrantLock();

        /** requestId -> 该等待请求产生的 waiting -> blocking 边。 */
        private final Map<Long, List<WaitForEdgeSnapshot>> edgesByRequest = new LinkedHashMap<>();

        private void replaceWaitEdges(LockRequest request, List<LockRequest> blockers) {
            mutex.lock();
            try {
                List<WaitForEdgeSnapshot> edges = new ArrayList<>(blockers.size());
                for (LockRequest blocker : blockers) {
                    edges.add(new WaitForEdgeSnapshot(request.requestId, blocker.requestId,
                            request.threadEventId, blocker.threadEventId,
                            request.owner, blocker.owner, request.key, request.mode, blocker.key, blocker.mode));
                }
                edgesByRequest.put(request.requestId, List.copyOf(edges));
            } finally {
                mutex.unlock();
            }
        }

        private void removeRequest(long requestId) {
            mutex.lock();
            try {
                edgesByRequest.remove(requestId);
            } finally {
                mutex.unlock();
            }
        }

        private void removeTransaction(TransactionId owner) {
            mutex.lock();
            try {
                Iterator<Map.Entry<Long, List<WaitForEdgeSnapshot>>> iterator = edgesByRequest.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, List<WaitForEdgeSnapshot>> entry = iterator.next();
                    List<WaitForEdgeSnapshot> kept = new ArrayList<>();
                    for (WaitForEdgeSnapshot edge : entry.getValue()) {
                        if (!edge.waitingTransactionId().equals(owner)
                                && !edge.blockingTransactionId().equals(owner)) {
                            kept.add(edge);
                        }
                    }
                    if (kept.isEmpty()) {
                        iterator.remove();
                    } else {
                        entry.setValue(List.copyOf(kept));
                    }
                }
            } finally {
                mutex.unlock();
            }
        }

        private boolean introducesCycle(TransactionId waiter, List<TransactionId> blockers, int searchLimit) {
            mutex.lock();
            try {
                Map<TransactionId, Set<TransactionId>> adjacency = adjacency();
                for (TransactionId blocker : blockers) {
                    if (reaches(adjacency, blocker, waiter, searchLimit)) {
                        return true;
                    }
                }
                return false;
            } finally {
                mutex.unlock();
            }
        }

        private List<WaitForEdgeSnapshot> snapshotEdges() {
            mutex.lock();
            try {
                List<WaitForEdgeSnapshot> result = new ArrayList<>();
                for (List<WaitForEdgeSnapshot> edges : edgesByRequest.values()) {
                    result.addAll(edges);
                }
                return List.copyOf(result);
            } finally {
                mutex.unlock();
            }
        }

        private Map<TransactionId, Set<TransactionId>> adjacency() {
            Map<TransactionId, Set<TransactionId>> adjacency = new HashMap<>();
            for (List<WaitForEdgeSnapshot> edges : edgesByRequest.values()) {
                for (WaitForEdgeSnapshot edge : edges) {
                    adjacency.computeIfAbsent(edge.waitingTransactionId(), ignored -> new LinkedHashSet<>())
                            .add(edge.blockingTransactionId());
                }
            }
            return adjacency;
        }

        private boolean reaches(Map<TransactionId, Set<TransactionId>> adjacency,
                                TransactionId start, TransactionId target, int searchLimit) {
            ArrayDeque<TransactionId> stack = new ArrayDeque<>();
            Set<TransactionId> visited = new HashSet<>();
            stack.push(start);
            int searched = 0;
            while (!stack.isEmpty() && searched < searchLimit) {
                TransactionId current = stack.pop();
                if (!visited.add(current)) {
                    continue;
                }
                if (current.equals(target)) {
                    return true;
                }
                searched++;
                Set<TransactionId> next = adjacency.get(current);
                if (next != null) {
                    for (TransactionId transactionId : next) {
                        stack.push(transactionId);
                    }
                }
            }
            return false;
        }
    }
}
