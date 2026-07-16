package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.exception.MetadataDeadlockException;
import cn.zhangyis.db.dd.exception.MetadataLockStateException;
import cn.zhangyis.db.dd.exception.MetadataLockTimeoutException;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分片 Metadata Lock 表。每个 key 只属于一个 shard；共享状态由 shard ReentrantLock 保护，等待线程挂在同一锁的
 * request Condition 上。等待期间 ReentrantLock 自动释放，调用方不得持 page/file/MTR/row-lock 资源。
 *
 * <p>队列严格 FIFO：只有队首与当前 granted 集合兼容时才授予；连续兼容队首可批量通过，因而 pending X 后来的
 * reader 不能插队。metadata wait graph 与 row-lock graph 完全分离。
 */
public final class MetadataLockManager {

    private final LockShard[] shards;
    private final int deadlockSearchLimit;
    private final AtomicLong nextRequestId = new AtomicLong(1);
    private final MetadataWaitGraph waitGraph = new MetadataWaitGraph();

    public MetadataLockManager() {
        this(32, 1024);
    }

    public MetadataLockManager(int shardCount, int deadlockSearchLimit) {
        if (shardCount <= 0 || deadlockSearchLimit <= 0) {
            throw new DatabaseValidationException("metadata lock shard/search limits must be positive");
        }
        this.shards = new LockShard[shardCount];
        for (int i = 0; i < shardCount; i++) {
            shards[i] = new LockShard();
        }
        this.deadlockSearchLimit = deadlockSearchLimit;
    }

    /**
     * 获取 MDL。冲突时 request 进入 FIFO queue、发布 wait edges 并做有界环检测；victim/timeout/interruption
     * 都在抛异常前完成 queue/graph 清理。
     */
    public MdlTicket acquire(MdlRequest request, Duration timeout) {
        validateAcquire(request, timeout);
        long remaining = timeoutNanos(timeout);
        long requestId = nextRequestId.getAndIncrement();
        LockShard shard = shardFor(request.key());
        shard.lock.lock();
        try {
            LockQueue queue = shard.queues.computeIfAbsent(request.key(), ignored -> new LockQueue());
            MdlTicket ticket = new MdlTicket(requestId, request.owner(), request.key(), request.mode(),
                    request.duration(), this);
            InternalRequest internal = new InternalRequest(requestId, request.owner(), request.key(), request.mode(),
                    request.duration(), ticket, shard.lock.newCondition(), null);
            Set<MdlOwnerId> blockers = blockers(queue, internal);
            if (blockers.isEmpty() && queue.waiting.isEmpty()) {
                grant(queue, internal);
                return ticket;
            }
            internal.state = MdlRequestState.PENDING;
            queue.waiting.addLast(internal);
            waitGraph.replace(internal.owner, blockersIncludingEarlier(queue, internal));
            if (waitGraph.introducesCycle(internal.owner, deadlockSearchLimit)) {
                removeWaiting(shard, queue, internal, MdlRequestState.VICTIM);
                throw new MetadataDeadlockException("metadata deadlock victim owner=" + request.owner().value());
            }
            while (internal.state == MdlRequestState.PENDING) {
                if (remaining <= 0) {
                    removeWaiting(shard, queue, internal, MdlRequestState.TIMEOUT);
                    throw new MetadataLockTimeoutException("metadata lock timeout owner=" + request.owner().value());
                }
                try {
                    remaining = internal.condition.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    removeWaiting(shard, queue, internal, MdlRequestState.TIMEOUT);
                    throw new MetadataLockTimeoutException(
                            "metadata lock wait interrupted owner=" + request.owner().value(), e);
                }
            }
            if (internal.state == MdlRequestState.GRANTED) {
                return ticket;
            }
            if (internal.state == MdlRequestState.VICTIM) {
                throw new MetadataDeadlockException("metadata deadlock victim owner=" + request.owner().value());
            }
            throw new MetadataLockTimeoutException("metadata lock request cancelled owner="
                    + request.owner().value());
        } finally {
            shard.lock.unlock();
        }
    }

    /**
     * 将 SHARED_UPGRADABLE ticket 升级为 EXCLUSIVE。原 SU grant 在等待期保留，upgrade waiter 排入同一 FIFO；
     * 授予时原子更新原 grant/ticket，不创建第二把已授予锁。
     */
    public MdlTicket upgrade(MdlTicket ticket, MdlMode targetMode, Duration timeout) {
        if (ticket == null || targetMode == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("metadata upgrade ticket/mode/positive timeout required");
        }
        if (ticket.isClosed()) {
            throw new MetadataLockStateException("cannot upgrade closed metadata lock ticket");
        }
        if (ticket.mode() != MdlMode.SHARED_UPGRADABLE || targetMode != MdlMode.EXCLUSIVE) {
            throw new MetadataLockStateException("only SHARED_UPGRADABLE -> EXCLUSIVE is supported");
        }
        long remaining = timeoutNanos(timeout);
        LockShard shard = shardFor(ticket.key());
        shard.lock.lock();
        try {
            LockQueue queue = requireQueue(shard, ticket.key());
            InternalRequest original = findGranted(queue, ticket.requestId());
            if (original == null) {
                throw new MetadataLockStateException("metadata upgrade ticket is no longer granted");
            }
            InternalRequest waiter = new InternalRequest(nextRequestId.getAndIncrement(), ticket.owner(), ticket.key(),
                    targetMode, ticket.duration(), ticket, shard.lock.newCondition(), original);
            Set<MdlOwnerId> blockers = blockers(queue, waiter);
            if (blockers.isEmpty() && queue.waiting.isEmpty()) {
                publishUpgrade(original, targetMode);
                return ticket;
            }
            waiter.state = MdlRequestState.PENDING;
            queue.waiting.addLast(waiter);
            waitGraph.replace(waiter.owner, blockersIncludingEarlier(queue, waiter));
            if (waitGraph.introducesCycle(waiter.owner, deadlockSearchLimit)) {
                removeWaiting(shard, queue, waiter, MdlRequestState.VICTIM);
                throw new MetadataDeadlockException("metadata upgrade deadlock owner=" + ticket.owner().value());
            }
            while (waiter.state == MdlRequestState.PENDING) {
                if (remaining <= 0) {
                    removeWaiting(shard, queue, waiter, MdlRequestState.TIMEOUT);
                    throw new MetadataLockTimeoutException("metadata upgrade timeout owner=" + ticket.owner().value());
                }
                try {
                    remaining = waiter.condition.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    removeWaiting(shard, queue, waiter, MdlRequestState.TIMEOUT);
                    throw new MetadataLockTimeoutException(
                            "metadata upgrade interrupted owner=" + ticket.owner().value(), e);
                }
            }
            if (waiter.state == MdlRequestState.GRANTED) {
                return ticket;
            }
            throw new MetadataLockTimeoutException("metadata upgrade cancelled owner=" + ticket.owner().value());
        } finally {
            shard.lock.unlock();
        }
    }

    /** 释放单 ticket；同一 ticket 重复 close 或 releaseAll 后 close 均为 no-op。 */
    public void release(MdlTicket ticket) {
        if (ticket == null) {
            throw new DatabaseValidationException("metadata lock ticket must not be null");
        }
        if (!ticket.closeByCaller()) {
            return;
        }
        LockShard shard = shardFor(ticket.key());
        shard.lock.lock();
        try {
            LockQueue queue = shard.queues.get(ticket.key());
            if (queue == null) {
                return;
            }
            InternalRequest granted = findGranted(queue, ticket.requestId());
            if (granted != null) {
                queue.granted.remove(granted);
                granted.state = MdlRequestState.RELEASED;
            }
            cancelWaitingForTicket(queue, ticket, MdlRequestState.KILLED);
            grantCompatibleWaiters(shard, queue);
            removeQueueIfEmpty(shard, ticket.key(), queue);
        } finally {
            shard.lock.unlock();
        }
    }

    /** 按 owner/duration 释放并取消请求；session statement/transaction end 使用本入口。 */
    public int releaseAll(MdlOwnerId owner, MdlDuration duration) {
        if (owner == null || duration == null) {
            throw new DatabaseValidationException("metadata release-all owner/duration must not be null");
        }
        int released = 0;
        for (LockShard shard : shards) {
            shard.lock.lock();
            try {
                Iterator<Map.Entry<MdlKey, LockQueue>> iterator = shard.queues.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<MdlKey, LockQueue> entry = iterator.next();
                    LockQueue queue = entry.getValue();
                    Iterator<InternalRequest> granted = queue.granted.iterator();
                    while (granted.hasNext()) {
                        InternalRequest request = granted.next();
                        if (request.owner.equals(owner) && request.duration == duration) {
                            granted.remove();
                            request.state = MdlRequestState.RELEASED;
                            request.ticket.closeByManager();
                            released++;
                        }
                    }
                    cancelWaiting(queue, owner, duration, MdlRequestState.KILLED);
                    grantCompatibleWaiters(shard, queue);
                    if (queue.granted.isEmpty() && queue.waiting.isEmpty()) {
                        iterator.remove();
                    }
                }
            } finally {
                shard.lock.unlock();
            }
        }
        waitGraph.removeOwner(owner);
        return released;
    }

    /** 释放 owner 的所有 duration，供 session kill/连接关闭。 */
    public int releaseAll(MdlOwnerId owner) {
        int released = 0;
        for (MdlDuration duration : MdlDuration.values()) {
            released += releaseAll(owner, duration);
        }
        return released;
    }

    /** kill owner：取消所有 pending 并释放 granted，保证 wait graph 无残留。 */
    public int cancelOwner(MdlOwnerId owner) {
        return releaseAll(owner);
    }

    /** 复制所有 shard 后再复制 graph；快照不携带 Condition/ticket/内部锁。 */
    public MetadataLockSnapshot snapshot() {
        List<GrantedMetadataLock> granted = new ArrayList<>();
        List<WaitingMetadataLock> waiting = new ArrayList<>();
        long now = System.nanoTime();
        for (LockShard shard : shards) {
            shard.lock.lock();
            try {
                for (LockQueue queue : shard.queues.values()) {
                    for (InternalRequest request : queue.granted) {
                        granted.add(new GrantedMetadataLock(request.requestId, request.owner, request.key,
                                request.mode, request.duration));
                    }
                    for (InternalRequest request : queue.waiting) {
                        waiting.add(new WaitingMetadataLock(request.requestId, request.owner, request.key,
                                request.mode, request.duration, request.state, Math.max(0, now - request.createdNanos)));
                    }
                }
            } finally {
                shard.lock.unlock();
            }
        }
        return new MetadataLockSnapshot(granted, waiting, waitGraph.snapshot());
    }

    private static void validateAcquire(MdlRequest request, Duration timeout) {
        if (request == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("metadata acquire request/positive timeout required");
        }
    }

    private Set<MdlOwnerId> blockers(LockQueue queue, InternalRequest requested) {
        Set<MdlOwnerId> blockers = new HashSet<>();
        for (InternalRequest granted : queue.granted) {
            if (!granted.owner.equals(requested.owner) && !requested.mode.compatibleWith(granted.mode)) {
                blockers.add(granted.owner);
            }
        }
        return blockers;
    }

    private Set<MdlOwnerId> blockersIncludingEarlier(LockQueue queue, InternalRequest requested) {
        Set<MdlOwnerId> blockers = blockers(queue, requested);
        for (InternalRequest earlier : queue.waiting) {
            if (earlier == requested) {
                break;
            }
            if (!earlier.owner.equals(requested.owner) && !requested.mode.compatibleWith(earlier.mode)) {
                blockers.add(earlier.owner);
            }
        }
        return blockers;
    }

    private void grantCompatibleWaiters(LockShard shard, LockQueue queue) {
        while (!queue.waiting.isEmpty()) {
            InternalRequest head = queue.waiting.peekFirst();
            Set<MdlOwnerId> blockers = blockers(queue, head);
            if (!blockers.isEmpty()) {
                waitGraph.replace(head.owner, blockers);
                return;
            }
            queue.waiting.removeFirst();
            waitGraph.removeWaiter(head.owner);
            grant(queue, head);
            head.condition.signal();
        }
    }

    private static void grant(LockQueue queue, InternalRequest request) {
        if (request.upgradeTarget != null) {
            publishUpgrade(request.upgradeTarget, request.mode);
        } else {
            queue.granted.add(request);
        }
        request.state = MdlRequestState.GRANTED;
    }

    private static void publishUpgrade(InternalRequest original, MdlMode target) {
        original.mode = target;
        original.ticket.publishMode(target);
    }

    private void removeWaiting(LockShard shard, LockQueue queue, InternalRequest request, MdlRequestState terminal) {
        queue.waiting.remove(request);
        request.state = terminal;
        waitGraph.removeWaiter(request.owner);
        grantCompatibleWaiters(shard, queue);
        removeQueueIfEmpty(shard, request.key, queue);
    }

    private void cancelWaitingForTicket(LockQueue queue, MdlTicket ticket, MdlRequestState terminal) {
        Iterator<InternalRequest> iterator = queue.waiting.iterator();
        while (iterator.hasNext()) {
            InternalRequest request = iterator.next();
            if (request.ticket == ticket) {
                iterator.remove();
                request.state = terminal;
                waitGraph.removeWaiter(request.owner);
                request.condition.signal();
            }
        }
    }

    private void cancelWaiting(LockQueue queue, MdlOwnerId owner, MdlDuration duration,
                               MdlRequestState terminal) {
        Iterator<InternalRequest> iterator = queue.waiting.iterator();
        while (iterator.hasNext()) {
            InternalRequest request = iterator.next();
            if (request.owner.equals(owner) && request.duration == duration) {
                iterator.remove();
                request.state = terminal;
                request.ticket.closeByManager();
                waitGraph.removeWaiter(owner);
                request.condition.signal();
            }
        }
    }

    private static InternalRequest findGranted(LockQueue queue, long requestId) {
        return queue.granted.stream().filter(request -> request.requestId == requestId).findFirst().orElse(null);
    }

    private static LockQueue requireQueue(LockShard shard, MdlKey key) {
        LockQueue queue = shard.queues.get(key);
        if (queue == null) {
            throw new MetadataLockStateException("metadata lock queue no longer exists: " + key);
        }
        return queue;
    }

    private static void removeQueueIfEmpty(LockShard shard, MdlKey key, LockQueue queue) {
        if (queue.granted.isEmpty() && queue.waiting.isEmpty()) {
            shard.queues.remove(key, queue);
        }
    }

    private LockShard shardFor(MdlKey key) {
        return shards[Math.floorMod(key.hashCode(), shards.length)];
    }

    private static long timeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new DatabaseValidationException("metadata lock timeout is too large", overflow);
        }
    }

    private static final class LockShard {
        /** 保护本 shard queue/granted/waiting 的公平显式锁。 */
        private final ReentrantLock lock = new ReentrantLock(true);
        private final Map<MdlKey, LockQueue> queues = new HashMap<>();
    }

    private static final class LockQueue {
        private final List<InternalRequest> granted = new ArrayList<>();
        private final ArrayDeque<InternalRequest> waiting = new ArrayDeque<>();
    }

    private static final class InternalRequest {
        private final long requestId;
        private final MdlOwnerId owner;
        private final MdlKey key;
        private MdlMode mode;
        private final MdlDuration duration;
        private final MdlTicket ticket;
        private final Condition condition;
        /** 非 null 表示 upgrade waiter，授予时修改该原始 grant 而非新增 grant。 */
        private final InternalRequest upgradeTarget;
        private final long createdNanos = System.nanoTime();
        private MdlRequestState state = MdlRequestState.REQUESTED;

        private InternalRequest(long requestId, MdlOwnerId owner, MdlKey key, MdlMode mode,
                                MdlDuration duration, MdlTicket ticket, Condition condition,
                                InternalRequest upgradeTarget) {
            this.requestId = requestId;
            this.owner = owner;
            this.key = key;
            this.mode = mode;
            this.duration = duration;
            this.ticket = ticket;
            this.condition = condition;
            this.upgradeTarget = upgradeTarget;
        }
    }
}
