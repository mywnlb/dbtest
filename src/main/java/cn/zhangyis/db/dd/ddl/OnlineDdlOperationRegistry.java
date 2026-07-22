package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DdlId;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DatabaseEngine唯一拥有的Online DDL tracker registry。active map只保存轻量tracker；terminal history由短锁保护的
 * 有界ring保存不可变snapshot，永不持有或关闭coordinator的MDL、gate、row-log和物理资源。
 */
public final class OnlineDdlOperationRegistry {

    /** 防止错误配置形成无界诊断内存。 */
    private static final int MAX_HISTORY_CAPACITY = 65_536;

    /** active operation按DDL identity索引；tracker内部拥有自己的短锁。 */
    private final ConcurrentHashMap<DdlId, OnlineDdlOperationTracker> active =
            new ConcurrentHashMap<>();
    /** 保护terminal ring/map的短锁；不与tracker锁或任何下游锁嵌套等待。 */
    private final ReentrantLock historyLock = new ReentrantLock();
    /** 按完成先后保存identity，超容量时从头淘汰。 */
    private final ArrayDeque<DdlId> historyOrder = new ArrayDeque<>();
    /** terminal identity到最终不可变snapshot。 */
    private final Map<DdlId, OnlineDdlOperationSnapshot> history = new HashMap<>();
    /** 本进程最多保留的terminal数量。 */
    private final int historyCapacity;

    /**
     * @param historyCapacity 正且有界的本进程terminal history容量
     */
    public OnlineDdlOperationRegistry(int historyCapacity) {
        if (historyCapacity <= 0 || historyCapacity > MAX_HISTORY_CAPACITY) {
            throw new DatabaseValidationException("Online DDL history capacity is invalid");
        }
        this.historyCapacity = historyCapacity;
    }

    /**
     * 注册一个live/recovery tracker；重复identity表示两个coordinator争用同一durable owner。
     *
     * @param identity 已冻结且不可变的operation身份
     * @param protocol 决定cancel capability的执行协议
     * @return 由调用方更新、但不拥有业务资源的新tracker
     */
    public OnlineDdlOperationTracker register(
            OnlineDdlOperationIdentity identity, DdlExecutionProtocol protocol) {
        if (identity == null || protocol == null) {
            throw new DatabaseValidationException("Online DDL registry identity/protocol must not be null");
        }
        OnlineDdlOperationTracker tracker = new OnlineDdlOperationTracker(identity, protocol);
        if (active.putIfAbsent(identity.ddlId(), tracker) != null) {
            throw new DatabaseValidationException(
                    "Online DDL identity is already active: " + identity.ddlId().value());
        }
        historyLock.lock();
        try {
            if (history.containsKey(identity.ddlId())) {
                active.remove(identity.ddlId(), tracker);
                throw new DatabaseValidationException(
                        "Online DDL identity already completed in this process: "
                                + identity.ddlId().value());
            }
        } finally {
            historyLock.unlock();
        }
        return tracker;
    }

    /**
     * 把active tracker发布为terminal并移入有界history；业务资源必须由coordinator在调用前自行收敛。
     *
     * @param ddlId active operation identity
     * @param result 非NONE终点
     * @param errorCode 可选有界领域错误码
     * @param forwardRecoveryRequired 是否需重启继续前滚
     * @return history保存的最终不可变snapshot
     */
    public OnlineDdlOperationSnapshot complete(
            DdlId ddlId, OnlineDdlTerminalResult result,
            Optional<String> errorCode, boolean forwardRecoveryRequired) {
        requireId(ddlId);
        OnlineDdlOperationTracker tracker = active.get(ddlId);
        if (tracker == null) {
            return findHistory(ddlId).orElseThrow(() -> new DatabaseValidationException(
                    "Online DDL completion target is not active: " + ddlId.value()));
        }
        OnlineDdlOperationSnapshot snapshot = tracker.complete(
                result, errorCode, forwardRecoveryRequired);
        historyLock.lock();
        try {
            if (history.putIfAbsent(ddlId, snapshot) == null) {
                historyOrder.addLast(ddlId);
                while (historyOrder.size() > historyCapacity) {
                    DdlId expired = historyOrder.removeFirst();
                    history.remove(expired);
                }
            }
            active.remove(ddlId, tracker);
            return history.getOrDefault(ddlId, snapshot);
        } finally {
            historyLock.unlock();
        }
    }

    /** @return active或本进程terminal history中的不可变snapshot。 */
    public Optional<OnlineDdlOperationSnapshot> find(DdlId ddlId) {
        requireId(ddlId);
        OnlineDdlOperationTracker tracker = active.get(ddlId);
        if (tracker != null) {
            return Optional.of(tracker.snapshot());
        }
        return findHistory(ddlId);
    }

    /** @return active与有界history合并后按DDL id升序排列的弱一致快照。 */
    public List<OnlineDdlOperationSnapshot> list() {
        List<OnlineDdlOperationSnapshot> result = new ArrayList<>();
        for (OnlineDdlOperationTracker tracker : active.values()) {
            result.add(tracker.snapshot());
        }
        historyLock.lock();
        try {
            result.addAll(history.values());
        } finally {
            historyLock.unlock();
        }
        result.sort(Comparator.comparingLong(snapshot -> snapshot.identity().ddlId().value()));
        return List.copyOf(result);
    }

    /**
     * 等待本进程active operation进入terminal；已在history时立即返回，identity未知或超时返回empty。
     *
     * @param ddlId 待等待的operation identity
     * @param timeout 正等待上界
     * @return terminal snapshot；未知/超时为空
     */
    public Optional<OnlineDdlOperationSnapshot> awaitTerminal(
            DdlId ddlId, Duration timeout) {
        requireId(ddlId);
        OnlineDdlOperationTracker tracker = active.get(ddlId);
        if (tracker == null) {
            return findHistory(ddlId);
        }
        return tracker.awaitTerminal(timeout);
    }

    /** control facade取得tracker参与prepare handoff或刷新durable投影；不向普通诊断调用方暴露map。 */
    Optional<OnlineDdlOperationTracker> tracker(DdlId ddlId) {
        return Optional.ofNullable(active.get(ddlId));
    }

    private Optional<OnlineDdlOperationSnapshot> findHistory(DdlId ddlId) {
        historyLock.lock();
        try {
            return Optional.ofNullable(history.get(ddlId));
        } finally {
            historyLock.unlock();
        }
    }

    private static void requireId(DdlId ddlId) {
        if (ddlId == null) {
            throw new DatabaseValidationException("Online DDL identity must not be null");
        }
    }
}
