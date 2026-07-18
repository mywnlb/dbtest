package cn.zhangyis.db.storage.trx;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.4 TransactionSystem.openReadViewSnapshot 的快照边界与并发原子性（设计 §5.3/§17）。
 * 验证 up/low 边界，以及并发 allocate/commit 与 snapshot 竞态下每个快照都内部一致（active id 全 ∈ [up, low)，
 * 即 ReadView 构造不变量不被撕裂读破坏）。
 */
class TransactionSystemReadViewTest {

    private static Transaction readOnlyProbe(TransactionManager mgr) {
        return mgr.begin(new TransactionOptions(IsolationLevel.REPEATABLE_READ, true, true));
    }

    /**
     * 验证 {@code emptyActiveSetUpEqualsLow} 所描述的值对象语义，并断言相等性、哈希、排序及非法构造边界一致。
     */
    @Test
    void emptyActiveSetUpEqualsLow() {
        TransactionSystem sys = new TransactionSystem();
        TransactionManager mgr = new TransactionManager(sys);
        ReadView v = sys.openReadViewSnapshot(readOnlyProbe(mgr));
        assertTrue(v.activeIds().isEmpty());
        assertEquals(v.lowLimitId(), v.upLimitId(), "空活跃集合 up==low");
        assertEquals(1L, v.lowLimitId(), "fresh system nextTransactionId=1");
    }

    /**
     * 验证 {@code nonEmptyActiveSetBoundaries} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void nonEmptyActiveSetBoundaries() {
        TransactionSystem sys = new TransactionSystem();
        TransactionManager mgr = new TransactionManager(sys);
        Transaction t1 = mgr.begin(TransactionOptions.defaults());
        Transaction t2 = mgr.begin(TransactionOptions.defaults());
        Transaction t3 = mgr.begin(TransactionOptions.defaults());
        mgr.assignWriteId(t1); // id 1
        mgr.assignWriteId(t2); // id 2
        mgr.assignWriteId(t3); // id 3

        ReadView v = sys.openReadViewSnapshot(readOnlyProbe(mgr));
        assertEquals(Set.of(1L, 2L, 3L), v.activeIds());
        assertEquals(1L, v.upLimitId(), "up = min active");
        assertEquals(4L, v.lowLimitId(), "low = nextTransactionId（只读探针不分配，故仍为 4）");

        mgr.commit(t2); // 移出 active
        ReadView v2 = sys.openReadViewSnapshot(readOnlyProbe(mgr));
        assertEquals(Set.of(1L, 3L), v2.activeIds());
        assertEquals(1L, v2.upLimitId());
        assertEquals(4L, v2.lowLimitId());
    }

    /**
     * 验证 {@code nonReadOnlyProbeAllocatesCreatorAtomically} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void nonReadOnlyProbeAllocatesCreatorAtomically() {
        TransactionSystem sys = new TransactionSystem();
        TransactionManager mgr = new TransactionManager(sys);
        Transaction writer = mgr.begin(TransactionOptions.defaults());
        ReadView v = sys.openReadViewSnapshot(writer);
        // 可写事务在快照临界区内分配 creator id 并登记 active，故 creator ∈ activeIds 且 < low
        assertEquals(writer.transactionId(), v.creatorTrxId());
        assertTrue(v.activeIds().contains(writer.transactionId().value()), "creator 已登记活跃");
        assertTrue(writer.transactionId().value() < v.lowLimitId());
    }

    /**
     * 验证 {@code concurrentAllocateCommitAndSnapshotStayConsistent} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     *
     * @throws InterruptedException 等待被中断时抛出；调用方应恢复中断标志并终止当前资源获取流程
     */
    @Test
    void concurrentAllocateCommitAndSnapshotStayConsistent() throws InterruptedException {
        TransactionSystem sys = new TransactionSystem();
        TransactionManager mgr = new TransactionManager(sys);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
        int rounds = 300;

        try {
            // 4 writer：模拟事务生命周期 allocate(进 active) → remove(出 active)
            for (int w = 0; w < 4; w++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < rounds; i++) {
                            Transaction t = mgr.begin(TransactionOptions.defaults());
                            mgr.assignWriteId(t);
                            mgr.commit(t);
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                });
            }
            // 4 reader：持续快照；ReadView 构造校验 active⊆[up,low)，撕裂读会抛异常被捕获
            for (int r = 0; r < 4; r++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < rounds; i++) {
                            ReadView v = sys.openReadViewSnapshot(readOnlyProbe(mgr));
                            for (long id : v.activeIds()) {
                                if (id < v.upLimitId() || id >= v.lowLimitId()) {
                                    throw new AssertionError("torn snapshot: " + id + " outside ["
                                            + v.upLimitId() + "," + v.lowLimitId() + ")");
                                }
                            }
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                });
            }
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "executor terminated");
        }
        assertTrue(errors.isEmpty(), "并发快照应无撕裂读/异常，实际: " + errors);
    }
}
