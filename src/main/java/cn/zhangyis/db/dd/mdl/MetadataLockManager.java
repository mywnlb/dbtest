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

    /**
     * 本对象关联的 {@code shards} 事务、会话或锁状态；owner 在生命周期内稳定，等待、可见性和释放路径均依赖该关联。
     */
    private final LockShard[] shards;
    /**
     * 记录 {@code deadlockSearchLimit} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
     */
    private final int deadlockSearchLimit;
    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private final AtomicLong nextRequestId = new AtomicLong(1);
    /**
     * 本对象关联的 {@code waitGraph} 事务、会话或锁状态；owner 在生命周期内稳定，等待、可见性和释放路径均依赖该关联。
     */
    private final MetadataWaitGraph waitGraph = new MetadataWaitGraph();

    /**
     * 创建 {@code MetadataLockManager}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     */
    public MetadataLockManager() {
        this(32, 1024);
    }

    /**
     * 创建 {@code MetadataLockManager}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param shardCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param deadlockSearchLimit 参与 {@code 构造} 的上界或规格值 {@code deadlockSearchLimit}；必须非负且不能使容量、页数或编码长度计算溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public MetadataLockManager(int shardCount, int deadlockSearchLimit) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (shardCount <= 0 || deadlockSearchLimit <= 0) {
            throw new DatabaseValidationException("metadata lock shard/search limits must be positive");
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.shards = new LockShard[shardCount];
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        for (int i = 0; i < shardCount; i++) {
            shards[i] = new LockShard();
        }
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.deadlockSearchLimit = deadlockSearchLimit;
    }

    /**
     * 获取 MDL。冲突时 request 进入 FIFO queue、发布 wait edges 并做有界环检测；victim/timeout/interruption
     * 都在抛异常前完成 queue/graph 清理。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code acquire} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws MetadataDeadlockException 锁请求违反兼容性、等待或死锁约束时抛出；调用方应释放当前操作资源并按事务边界回滚或重试
     * @throws MetadataLockTimeoutException 操作在约定时限内无法完成时抛出；调用方可回滚或稍后重试
     */
    public MdlTicket acquire(MdlRequest request, Duration timeout) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        validateAcquire(request, timeout);
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        long remaining = timeoutNanos(timeout);
        long requestId = nextRequestId.getAndIncrement();
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        LockShard shard = shardFor(request.key());
        shard.lock.lock();
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
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
     *
     * @param ticket 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param targetMode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code upgrade} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws MetadataLockStateException 锁请求违反兼容性、等待或死锁约束时抛出；调用方应释放当前操作资源并按事务边界回滚或重试
     * @throws MetadataDeadlockException 锁请求违反兼容性、等待或死锁约束时抛出；调用方应释放当前操作资源并按事务边界回滚或重试
     * @throws MetadataLockTimeoutException 操作在约定时限内无法完成时抛出；调用方可回滚或稍后重试
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

    /**
     * 把已授予的 EXCLUSIVE ticket 原地降级为 SHARED_UPGRADABLE。降级不等待、不创建新 request，也不改变
     * owner/key/duration；发布弱模式后按既有 FIFO 规则唤醒队首连续兼容请求。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在进入 shard 前校验 ticket、目标模式和未关闭状态，拒绝其它转换。</li>
     *     <li>取得目标 key 的 shard 锁并重新定位原 granted request，防止与 close/release 竞争。</li>
     *     <li>在同一临界区同时更新 internal grant 与 ticket volatile mode，保持快照和调用方视图一致。</li>
     *     <li>按 FIFO 兼容性授予等待者并释放 shard 锁；本路径不创建或遗留 metadata wait edge。</li>
     * </ol>
     *
     * @param ticket 当前 manager 已授予且尚未关闭的 EXCLUSIVE ticket
     * @param targetMode 唯一支持的目标 {@link MdlMode#SHARED_UPGRADABLE}
     * @return 与输入相同的 ticket 实例，其 request id、owner、key 和 duration 均保持不变
     * @throws DatabaseValidationException ticket 或目标模式为 {@code null} 时抛出；调用方应修正输入
     * @throws MetadataLockStateException ticket 已关闭、已释放、不是 EXCLUSIVE 或目标不是 SU 时抛出
     */
    public MdlTicket downgrade(MdlTicket ticket, MdlMode targetMode) {
        // 1. 降级是严格的 X→SU 原地转换，不能被当作任意“改弱”接口绕过兼容矩阵。
        if (ticket == null || targetMode == null) {
            throw new DatabaseValidationException("metadata downgrade ticket/mode required");
        }
        if (ticket.isClosed()) {
            throw new MetadataLockStateException("cannot downgrade closed metadata lock ticket");
        }
        if (ticket.mode() != MdlMode.EXCLUSIVE || targetMode != MdlMode.SHARED_UPGRADABLE) {
            throw new MetadataLockStateException("only EXCLUSIVE -> SHARED_UPGRADABLE is supported");
        }

        // 2. shard 锁内重新核对 grant identity；close 若先获锁会让 findGranted 明确失败。
        LockShard shard = shardFor(ticket.key());
        shard.lock.lock();
        try {
            LockQueue queue = requireQueue(shard, ticket.key());
            InternalRequest original = findGranted(queue, ticket.requestId());
            if (ticket.isClosed() || original == null || original.ticket != ticket
                    || original.mode != MdlMode.EXCLUSIVE) {
                throw new MetadataLockStateException("metadata downgrade ticket is no longer exclusive/granted");
            }

            // 3. internal request 与对外 ticket 在同一锁内发布，snapshot 不会观察到分裂模式。
            publishUpgrade(original, targetMode);

            // 4. SU 与 SR/SW 兼容，FIFO 队首可连续通过；第二个 SU/SNW/X 仍由矩阵阻塞。
            grantCompatibleWaiters(shard, queue);
            return ticket;
        } finally {
            shard.lock.unlock();
        }
    }

    /** 释放单 ticket；同一 ticket 重复 close 或 releaseAll 后 close 均为 no-op。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param ticket 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void release(MdlTicket ticket) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        if (ticket == null) {
            throw new DatabaseValidationException("metadata lock ticket must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        if (!ticket.closeByCaller()) {
            return;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
        LockShard shard = shardFor(ticket.key());
        shard.lock.lock();
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
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

    /** 按 owner/duration 释放并取消请求；session statement/transaction end 使用本入口。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 owner、目标资源、锁模式与等待时限；非法请求在进入队列或建立等待边前拒绝。</li>
     *     <li>按分片与队列锁顺序定位请求，在显式锁内重新检查兼容性，并用有界条件等待处理竞争。</li>
     *     <li>授予、转换或释放锁所有权，同时维护等待队列、Wait-For Graph 与可观测状态的一致视图。</li>
     *     <li>唤醒后再次验证结果并释放内部短锁；超时、中断或 victim 路径不得遗留锁请求或等待边。</li>
     * </ol>
     *
     * @param owner 参与 {@code releaseAll} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param duration 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @return {@code releaseAll} 实际完成的资源、绑定、页或槽位数量；未处理任何对象时为零，结果不得超过输入候选数
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public int releaseAll(MdlOwnerId owner, MdlDuration duration) {
        // 1、校验 owner、目标资源、锁模式与等待时限，在共享或持久副作用前拒绝非法状态。
        if (owner == null || duration == null) {
            throw new DatabaseValidationException("metadata release-all owner/duration must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按分片与队列锁顺序定位请求，保持处理顺序与资源边界。
        int released = 0;
        // 3、在中间分支复核阶段性结果；满足条件后，授予、转换或释放锁所有权，并维持领域不变量。
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
        // 4、唤醒后再次验证结果并释放内部短锁，以稳定返回或领域异常完成收口。
        return released;
    }

    /** 释放 owner 的所有 duration，供 session kill/连接关闭。
     *
     * @param owner 参与 {@code releaseAll} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code releaseAll} 实际完成的资源、绑定、页或槽位数量；未处理任何对象时为零，结果不得超过输入候选数
     */
    public int releaseAll(MdlOwnerId owner) {
        int released = 0;
        for (MdlDuration duration : MdlDuration.values()) {
            released += releaseAll(owner, duration);
        }
        return released;
    }

    /**
     * 取消 owner 当前所有尚未授予的 acquire/upgrade 请求，但不释放已授予 ticket。
     *
     * <p>该入口专供 Online DDL 持久取消后唤醒 final-MDL 等待。对 upgrade 请求，等待者与已授予
     * SU 共用同一 ticket，因此本方法只终止 upgrade waiter，不关闭该 ticket。coordinator 被唤醒后会在自身
     * 安全点退出，再由原有 RAII 路径释放已授予锁。</p>
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验 owner，避免空身份导致跨会话队列扫描。</li>
     *     <li>逐 shard 取得显式短锁，精确移除该 owner 的 pending request 并删除 wait-graph 边。</li>
     *     <li>对独立 acquire ticket 发布 manager-close；对 upgrade 保留原 granted ticket，随后唤醒等待线程。</li>
     *     <li>重新执行 FIFO 授予并清理空队列；全路径不触及其他 owner 或任何已授予请求。</li>
     * </ol>
     *
     * @param owner 已经完成 durable cancel 裁决的 DDL MDL owner；不得为 {@code null}
     * @return 本次从 FIFO 队列中精确移除并唤醒的 pending request 数量
     * @throws DatabaseValidationException owner 为 {@code null} 时抛出，且不修改任何队列
     */
    public int cancelPending(MdlOwnerId owner) {
        // 1. 控制面必须携带精确 owner，不允许使用空值表示广播取消。
        if (owner == null) {
            throw new DatabaseValidationException("metadata pending-cancel owner must not be null");
        }

        int cancelled = 0;
        // 2. shard 之间不同时持锁；该 owner 的全部 pending 都会在返回前被移除。
        for (LockShard shard : shards) {
            shard.lock.lock();
            try {
                Iterator<Map.Entry<MdlKey, LockQueue>> queues = shard.queues.entrySet().iterator();
                while (queues.hasNext()) {
                    Map.Entry<MdlKey, LockQueue> entry = queues.next();
                    LockQueue queue = entry.getValue();
                    Iterator<InternalRequest> waiting = queue.waiting.iterator();
                    while (waiting.hasNext()) {
                        InternalRequest request = waiting.next();
                        if (!request.owner.equals(owner)) {
                            continue;
                        }
                        waiting.remove();
                        request.state = MdlRequestState.KILLED;
                        waitGraph.removeWaiter(owner);
                        // 3. upgrade waiter 共用原 SU ticket；只有独立 acquire waiter 才能在此关闭。
                        if (request.upgradeTarget == null) {
                            request.ticket.closeByManager();
                        }
                        request.condition.signal();
                        cancelled++;
                    }

                    // 4. 取消可能移除 FIFO 队首，必须立即让后续兼容请求获得授予机会。
                    grantCompatibleWaiters(shard, queue);
                    if (queue.granted.isEmpty() && queue.waiting.isEmpty()) {
                        queues.remove();
                    }
                }
            } finally {
                shard.lock.unlock();
            }
        }
        return cancelled;
    }

    /** kill owner：取消所有 pending 并释放 granted，保证 wait graph 无残留。
     *
     * @param owner 参与 {@code cancelOwner} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code cancelOwner} 实际完成的资源、绑定、页或槽位数量；未处理任何对象时为零，结果不得超过输入候选数
     */
    public int cancelOwner(MdlOwnerId owner) {
        return releaseAll(owner);
    }

    /** 复制所有 shard 后再复制 graph；快照不携带 Condition/ticket/内部锁。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
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

    /**
     * 按数据字典并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param queue 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param requested 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    private Set<MdlOwnerId> blockers(LockQueue queue, InternalRequest requested) {
        Set<MdlOwnerId> blockers = new HashSet<>();
        for (InternalRequest granted : queue.granted) {
            if (!granted.owner.equals(requested.owner) && !requested.mode.compatibleWith(granted.mode)) {
                blockers.add(granted.owner);
            }
        }
        return blockers;
    }

    /**
     * 按数据字典并发协议获取或等待资源；等待必须有界，失败路径保持锁顺序并释放已取得资源。
     *
     * @param queue 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param requested 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
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

    /**
     * 数据字典的 {@code LockShard} 并发分片；分片内部状态由其显式锁保护，用于降低共享结构上的竞争范围。
     */
    private static final class LockShard {
        /** 保护本 shard queue/granted/waiting 的公平显式锁。 */
        private final ReentrantLock lock = new ReentrantLock(true);
        /**
         * 本对象拥有的 {@code queues} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final Map<MdlKey, LockQueue> queues = new HashMap<>();
    }

    /**
     * 数据字典的 {@code LockQueue} 等待队列；入队、授予与移除必须由所属分片锁保护，并维持可观测顺序。
     */
    private static final class LockQueue {
        /**
         * 本对象拥有的 {@code granted} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final List<InternalRequest> granted = new ArrayList<>();
        /**
         * 本对象拥有的 {@code waiting} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final ArrayDeque<InternalRequest> waiting = new ArrayDeque<>();
    }

    /**
     * 数据字典的 {@code InternalRequest} 内部请求状态；身份在创建后稳定，模式与等待状态只在所属显式锁内转换。
     */
    private static final class InternalRequest {
        /**
         * 记录 {@code requestId} 的稳定身份或单调版本；只由分配/发布路径更新，重复、回退或跨 owner 复用会破坏可见性。
         */
        private final long requestId;
        /**
         * 构造时冻结的 {@code owner} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
         */
        private final MdlOwnerId owner;
        /**
         * 构造时冻结的 {@code key} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
         */
        private final MdlKey key;
        /**
         * 本对象的权威状态机字段 {@code mode}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
         */
        private MdlMode mode;
        /**
         * 本对象关联的 {@code duration} 事务、会话或锁状态；owner 在生命周期内稳定，等待、可见性和释放路径均依赖该关联。
         */
        private final MdlDuration duration;
        /**
         * 本对象关联的 {@code ticket} 事务、会话或锁状态；owner 在生命周期内稳定，等待、可见性和释放路径均依赖该关联。
         */
        private final MdlTicket ticket;
        /**
         * 与本对象显式锁绑定的等待条件；等待方必须在锁内循环复核领域谓词并使用有界超时。
         */
        private final Condition condition;
        /** 非 null 表示 upgrade waiter，授予时修改该原始 grant 而非新增 grant。 */
        private final InternalRequest upgradeTarget;
        /**
         * 记录 {@code createdNanos} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private final long createdNanos = System.nanoTime();
        /**
         * 本对象的权威状态机字段 {@code state}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
         */
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
