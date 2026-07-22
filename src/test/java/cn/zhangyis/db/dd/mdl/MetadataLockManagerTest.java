package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.exception.MetadataDeadlockException;
import cn.zhangyis.db.dd.exception.MetadataLockTimeoutException;
import cn.zhangyis.db.dd.exception.MetadataLockStateException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;

/** MDL 六模式、队列公平性、upgrade、timeout 和独立 metadata wait graph 的 TDD。 */
class MetadataLockManagerTest {

    /** 固定兼容矩阵是授锁的单一权威，测试覆盖全部 36 个组合，防止实现分支漂移。 */
    @Test
    void exposesTheCompleteSymmetricCompatibilityMatrix() {
        boolean[][] expected = {
                {true, true, true, true, true, false},
                {true, true, true, true, true, false},
                {true, true, true, true, false, false},
                {true, true, true, false, false, false},
                {true, true, false, false, true, false},
                {false, false, false, false, false, false}
        };
        MdlMode[] modes = MdlMode.values();
        for (int requested = 0; requested < modes.length; requested++) {
            for (int granted = 0; granted < modes.length; granted++) {
                assertEquals(expected[requested][granted], modes[requested].compatibleWith(modes[granted]),
                        modes[requested] + " vs " + modes[granted]);
            }
        }
    }

    /** pending X 后到达的 SR 不得插队；释放当前 reader 后必须先授 X，再授后来的 reader。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void preventsReaderBargingPastPendingExclusiveRequest() throws Exception {
        MetadataLockManager manager = new MetadataLockManager(8, 64);
        MdlKey key = MdlKey.table("def.app.orders");
        MdlTicket firstReader = manager.acquire(request(1, key, MdlMode.SHARED_READ), Duration.ofSeconds(2));
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<MdlTicket> writer = executor.submit(() -> manager.acquire(
                    request(2, key, MdlMode.EXCLUSIVE), Duration.ofSeconds(2)));
            await(() -> manager.snapshot().waiting().size() == 1);
            Future<MdlTicket> laterReader = executor.submit(() -> manager.acquire(
                    request(3, key, MdlMode.SHARED_READ), Duration.ofSeconds(2)));
            await(() -> manager.snapshot().waiting().size() == 2);

            firstReader.close();
            try (MdlTicket writerTicket = writer.get()) {
                assertFalse(laterReader.isDone());
                assertEquals(MdlMode.EXCLUSIVE, writerTicket.mode());
            }
            try (MdlTicket ignored = laterReader.get()) {
                assertTrue(manager.snapshot().waiting().isEmpty());
            }
        }
    }

    /** SU owner 升级 X 时保留原 ticket，等待其它 SW 退出；成功后 ticket mode 原子切换为 X。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void upgradesSharedUpgradableAfterBlockersLeave() throws Exception {
        MetadataLockManager manager = new MetadataLockManager(8, 64);
        MdlKey key = MdlKey.table("def.app.orders");
        MdlTicket upgrader = manager.acquire(request(1, key, MdlMode.SHARED_UPGRADABLE),
                Duration.ofSeconds(2));
        MdlTicket writer = manager.acquire(request(2, key, MdlMode.SHARED_WRITE), Duration.ofSeconds(2));
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<MdlTicket> upgraded = executor.submit(() -> manager.upgrade(upgrader, MdlMode.EXCLUSIVE,
                    Duration.ofSeconds(2)));
            await(() -> manager.snapshot().waiting().size() == 1);
            writer.close();

            assertEquals(upgrader, upgraded.get());
            assertEquals(MdlMode.EXCLUSIVE, upgrader.mode());
        } finally {
            upgrader.close();
        }
    }

    /** X 原地降级 SU 必须保留 ticket identity，并唤醒兼容 SR/SW，且不创建 wait-graph 边。 */
    @Test
    void downgradesExclusiveInPlaceAndWakesCompatibleReadersAndWriters() throws Exception {
        MetadataLockManager manager = new MetadataLockManager(8, 64);
        MdlKey key = MdlKey.table("def.app.orders");
        MdlTicket exclusive = manager.acquire(request(1, key, MdlMode.EXCLUSIVE), Duration.ofSeconds(1));
        long requestId = exclusive.requestId();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<MdlTicket> reader = executor.submit(() -> manager.acquire(
                    request(2, key, MdlMode.SHARED_READ), Duration.ofSeconds(2)));
            Future<MdlTicket> writer = executor.submit(() -> manager.acquire(
                    request(3, key, MdlMode.SHARED_WRITE), Duration.ofSeconds(2)));
            await(() -> manager.snapshot().waiting().size() == 2);

            MdlTicket downgraded = manager.downgrade(exclusive, MdlMode.SHARED_UPGRADABLE);

            assertSame(exclusive, downgraded);
            assertEquals(requestId, downgraded.requestId());
            assertEquals(MdlOwnerId.of(1), downgraded.owner());
            assertEquals(MdlDuration.EXPLICIT, downgraded.duration());
            assertEquals(MdlMode.SHARED_UPGRADABLE, downgraded.mode());
            try (MdlTicket ignoredReader = reader.get(1, java.util.concurrent.TimeUnit.SECONDS);
                 MdlTicket ignoredWriter = writer.get(1, java.util.concurrent.TimeUnit.SECONDS)) {
                assertTrue(manager.snapshot().waiting().isEmpty());
                assertTrue(manager.snapshot().waitEdges().isEmpty());
            }
        } finally {
            exclusive.close();
        }
    }

    /** SU 降级结果仍排斥第二个 SU 与 X；错误 source/target 和 closed ticket 必须无副作用拒绝。 */
    @Test
    void rejectsInvalidDowngradeAndKeepsStrongCompatibilityBoundary() {
        MetadataLockManager manager = new MetadataLockManager(8, 64);
        MdlKey key = MdlKey.table("def.app.orders");
        MdlTicket exclusive = manager.acquire(request(1, key, MdlMode.EXCLUSIVE), Duration.ofSeconds(1));

        manager.downgrade(exclusive, MdlMode.SHARED_UPGRADABLE);

        assertThrows(MetadataLockTimeoutException.class, () -> manager.acquire(
                request(2, key, MdlMode.SHARED_UPGRADABLE), Duration.ofMillis(30)));
        assertThrows(MetadataLockTimeoutException.class, () -> manager.acquire(
                request(3, key, MdlMode.EXCLUSIVE), Duration.ofMillis(30)));
        assertThrows(MetadataLockStateException.class,
                () -> manager.downgrade(exclusive, MdlMode.SHARED_READ));
        exclusive.close();
        assertThrows(MetadataLockStateException.class,
                () -> manager.downgrade(exclusive, MdlMode.SHARED_UPGRADABLE));
        assertTrue(manager.snapshot().waitEdges().isEmpty());
    }

    /**
     * Online DDL 取消只能唤醒尚在等待的 final-X upgrade；已授予的 SU 与其它 owner 的 SW 都必须保留，
     * 否则控制线程会在 coordinator 尚未到达安全点时强拆其资源所有权。
     *
     * @throws Exception virtual-thread future 在断言前传播意外执行失败时抛出
     */
    @Test
    void cancelsOnlyPendingRequestsAndKeepsGrantedTickets() throws Exception {
        MetadataLockManager manager = new MetadataLockManager(8, 64);
        MdlKey key = MdlKey.table("def.app.orders");
        MdlOwnerId ddlOwner = MdlOwnerId.of(2);
        MdlTicket ddlSharedUpgradable = manager.acquire(
                request(ddlOwner.value(), key, MdlMode.SHARED_UPGRADABLE), Duration.ofSeconds(2));
        MdlTicket blockingWriter = manager.acquire(
                request(1, key, MdlMode.SHARED_WRITE), Duration.ofSeconds(2));
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<MdlTicket> finalUpgrade = executor.submit(() -> manager.upgrade(
                    ddlSharedUpgradable, MdlMode.EXCLUSIVE, Duration.ofSeconds(5)));
            await(() -> manager.snapshot().waiting().size() == 1);

            assertEquals(1, manager.cancelPending(ddlOwner));
            assertThrows(java.util.concurrent.ExecutionException.class, finalUpgrade::get);

            MetadataLockSnapshot snapshot = manager.snapshot();
            assertTrue(snapshot.waiting().isEmpty());
            assertTrue(snapshot.waitEdges().isEmpty());
            assertEquals(2, snapshot.granted().size());
            assertTrue(snapshot.granted().stream().anyMatch(lock ->
                    lock.owner().equals(ddlOwner) && lock.mode() == MdlMode.SHARED_UPGRADABLE));
            assertTrue(snapshot.granted().stream().anyMatch(lock ->
                    lock.owner().equals(MdlOwnerId.of(1)) && lock.mode() == MdlMode.SHARED_WRITE));
            assertFalse(ddlSharedUpgradable.isClosed());
            assertFalse(blockingWriter.isClosed());
        } finally {
            blockingWriter.close();
            ddlSharedUpgradable.close();
        }
    }

    /** timeout 抛领域异常前必须清除 queue 和 wait graph，不遗留幽灵 blocker。 */
    @Test
    void removesTimedOutRequestFromQueueAndWaitGraph() {
        MetadataLockManager manager = new MetadataLockManager(8, 64);
        MdlKey key = MdlKey.table("def.app.orders");
        try (MdlTicket ignored = manager.acquire(request(1, key, MdlMode.EXCLUSIVE), Duration.ofSeconds(1))) {
            assertThrows(MetadataLockTimeoutException.class, () -> manager.acquire(
                    request(2, key, MdlMode.SHARED_READ), Duration.ofMillis(30)));
        }

        assertTrue(manager.snapshot().waiting().isEmpty());
        assertTrue(manager.snapshot().waitEdges().isEmpty());
    }

    /** 两个 table key 形成 metadata 环时，最后形成环的当前请求是确定性 victim，行锁图不参与。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void detectsCrossKeyMetadataDeadlockAndKeepsOtherWaiterLive() throws Exception {
        MetadataLockManager manager = new MetadataLockManager(8, 64);
        MdlKey a = MdlKey.table("def.app.a");
        MdlKey b = MdlKey.table("def.app.b");
        MdlTicket aByOne = manager.acquire(request(1, a, MdlMode.EXCLUSIVE), Duration.ofSeconds(2));
        MdlTicket bByTwo = manager.acquire(request(2, b, MdlMode.EXCLUSIVE), Duration.ofSeconds(2));
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<MdlTicket> oneWaitsForB = executor.submit(() -> manager.acquire(
                    request(1, b, MdlMode.EXCLUSIVE), Duration.ofSeconds(2)));
            await(() -> manager.snapshot().waiting().size() == 1);

            assertThrows(MetadataDeadlockException.class, () -> manager.acquire(
                    request(2, a, MdlMode.EXCLUSIVE), Duration.ofSeconds(2)));
            assertEquals(1, manager.snapshot().waiting().size());
            assertTrue(manager.snapshot().waitEdges().contains(
                    new MetadataWaitEdge(MdlOwnerId.of(1), MdlOwnerId.of(2))));

            bByTwo.close();
            try (MdlTicket acquired = oneWaitsForB.get()) {
                assertEquals(MdlOwnerId.of(1), acquired.owner());
            }
        } finally {
            aByOne.close();
            bByTwo.close();
        }
    }

    private static MdlRequest request(long owner, MdlKey key, MdlMode mode) {
        return new MdlRequest(MdlOwnerId.of(owner), key, mode, MdlDuration.EXPLICIT);
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("condition did not become true before deadline");
            }
            Thread.yield();
        }
    }
}
