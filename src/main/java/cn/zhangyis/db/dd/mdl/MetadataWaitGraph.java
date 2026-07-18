package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 只含 MDL owner 的 wait-for graph。graph lock 从不回调 lock shard，形成 shard -> graph 的单向锁序。
 */
final class MetadataWaitGraph {

    /**
     * 保护本对象共享状态的显式并发闩；获取后必须在 {@code finally} 或 Guard 关闭路径释放。
     */
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * 本对象拥有的 {@code edges} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final Map<MdlOwnerId, Set<MdlOwnerId>> edges = new HashMap<>();

    /**
     * 在 graph lock 内原子替换 waiter 的全部出边；空 blocker 集合删除该 waiter，非空集合以不可变副本发布。
     *
     * @param waiter 参与 {@code replace} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param blockers 参与 {@code replace} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    void replace(MdlOwnerId waiter, Set<MdlOwnerId> blockers) {
        lock.lock();
        try {
            if (blockers.isEmpty()) {
                edges.remove(waiter);
            } else {
                edges.put(waiter, Set.copyOf(blockers));
            }
        } finally {
            lock.unlock();
        }
    }

    /** 请求离开等待态时只删 outgoing edges；owner 可能仍持 grant，不能误删其它 waiter 的 incoming edge。
     *
     * @param owner 参与 {@code removeWaiter} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    void removeWaiter(MdlOwnerId owner) {
        lock.lock();
        try {
            edges.remove(owner);
        } finally {
            lock.unlock();
        }
    }

    /** owner 的全部 granted/waiting 均已结束时，清理 outgoing 与 incoming edges。
     *
     * @param owner 参与 {@code removeOwner} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    void removeOwner(MdlOwnerId owner) {
        lock.lock();
        try {
            edges.remove(owner);
            edges.replaceAll((waiter, blockers) -> {
                Set<MdlOwnerId> copy = new HashSet<>(blockers);
                copy.remove(owner);
                return Set.copyOf(copy);
            });
            edges.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在 graph lock 内从指定 owner 做有界深度优先遍历；搜索重新到达起点时报告环，达到上限或遍历结束时报告无环。
     *
     * @param start 参与 {@code introducesCycle} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param searchLimit 参与 {@code introducesCycle} 的上界或规格值 {@code searchLimit}；必须非负且不能使容量、页数或编码长度计算溢出
     * @return {@code introducesCycle} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    boolean introducesCycle(MdlOwnerId start, int searchLimit) {
        lock.lock();
        try {
            ArrayDeque<MdlOwnerId> stack = new ArrayDeque<>();
            Set<MdlOwnerId> visited = new HashSet<>();
            stack.push(start);
            int visitedCount = 0;
            while (!stack.isEmpty() && visitedCount++ < searchLimit) {
                MdlOwnerId owner = stack.pop();
                for (MdlOwnerId blocker : edges.getOrDefault(owner, Set.of())) {
                    if (blocker.equals(start)) {
                        return true;
                    }
                    if (visited.add(blocker)) {
                        stack.push(blocker);
                    }
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 采集 {@code snapshot} 对应的数据字典稳定快照；返回对象与后续内部修改隔离，不转移内部可变状态的所有权。
     *
     * @return 调用时刻的不可变状态集合或映射；没有已发布条目时返回空集合，调用方修改不会影响权威状态
     */
    List<MetadataWaitEdge> snapshot() {
        lock.lock();
        try {
            List<MetadataWaitEdge> result = new ArrayList<>();
            for (Map.Entry<MdlOwnerId, Set<MdlOwnerId>> entry : edges.entrySet()) {
                for (MdlOwnerId blocker : entry.getValue()) {
                    result.add(new MetadataWaitEdge(entry.getKey(), blocker));
                }
            }
            return List.copyOf(result);
        } finally {
            lock.unlock();
        }
    }
}
