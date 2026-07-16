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

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<MdlOwnerId, Set<MdlOwnerId>> edges = new HashMap<>();

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

    /** 请求离开等待态时只删 outgoing edges；owner 可能仍持 grant，不能误删其它 waiter 的 incoming edge。 */
    void removeWaiter(MdlOwnerId owner) {
        lock.lock();
        try {
            edges.remove(owner);
        } finally {
            lock.unlock();
        }
    }

    /** owner 的全部 granted/waiting 均已结束时，清理 outgoing 与 incoming edges。 */
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
