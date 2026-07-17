package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 DML 与 purge 的短物理行协调 guard。它不替代事务 record lock：只保证同一聚簇主键的多索引 MTR 与
 * secondary purge 不交错；DML 有界等待，purge 绝不等待或进入事务 wait-for graph。
 */
class PurgeDmlRowGuardManagerTest {

    /**
     * DML 已持 guard 时，同 identity 的第二个 DML 必须在明确 timeout 后失败；释放后可重新获取，
     * 证明显式 lock 的所有路径都有可验证释放点，而不是依赖 Java monitor。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>owner 线程取得同表同主键 guard，并用 latch 暂停在物理临界区。</li>
     *     <li>waiter 线程以短 timeout 请求同 identity，断言收到领域超时而非无界等待。</li>
     *     <li>唤醒 owner 释放 guard，主线程再次取得同 identity，验证正常与超时路径都没有泄漏锁。</li>
     * </ol>
     *
     * @throws Exception 测试线程 join/latch 等待被 JUnit 线程中断时向框架报告失败。
     */
    @Test
    void dmlUsesBoundedWaitAndCanAcquireAfterRelease() throws Exception {
        // 1. owner 先进入短物理区；CountDownLatch 只协调测试时序，不参与生产锁实现。
        PurgeDmlRowGuardManager manager = new PurgeDmlRowGuardManager();
        SearchKey key = key(7);
        CountDownLatch ownerReady = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        Thread owner = Thread.ofPlatform().start(() -> {
            try (PurgeDmlRowGuard ignored = manager.acquireForDml(3, key, Duration.ofSeconds(1))) {
                ownerReady.countDown();
                releaseOwner.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(ownerReady.await(1, TimeUnit.SECONDS));

        // 2. waiter 必须在 50ms 边界后得到 PurgeDmlRowGuardTimeoutException，并终止请求线程。
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread waiter = Thread.ofPlatform().start(() -> {
            try (PurgeDmlRowGuard ignored = manager.acquireForDml(3, key, Duration.ofMillis(50))) {
                failure.set(new AssertionError("same-row DML unexpectedly crossed held guard"));
            } catch (Throwable expected) {
                failure.set(expected);
            }
        });
        waiter.join(1_000);
        assertInstanceOf(PurgeDmlRowGuardTimeoutException.class, failure.get());

        // 3. owner 关闭 AutoCloseable guard 后，同 identity 应能立即重新取得，证明 stripe 没有泄漏。
        releaseOwner.countDown();
        owner.join(1_000);
        try (PurgeDmlRowGuard ignored = manager.acquireForDml(3, key, Duration.ofSeconds(1))) {
            assertTrue(true, "released row guard can be reacquired");
        }
    }

    /**
     * purge 使用零等待 try-acquire：busy 时立即返回 empty，DML 释放后再调才能取得 guard。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>前台 DML 持有同 identity guard，记录 purge try-acquire 的耗时与 empty 结果。</li>
     *     <li>DML 释放后再次尝试，断言 purge 取得并能通过 try-with-resources 释放 guard。</li>
     * </ol>
     */
    @Test
    void purgeNeverWaitsForBusyDmlGuard() {
        // 1. busy purge 不进入事务 wait-for graph，且应在远小于测试阈值的时间内返回 empty。
        PurgeDmlRowGuardManager manager = new PurgeDmlRowGuardManager();
        SearchKey key = key(8);

        try (PurgeDmlRowGuard ignored = manager.acquireForDml(4, key, Duration.ofSeconds(1))) {
            long start = System.nanoTime();
            Optional<PurgeDmlRowGuard> busy = manager.tryAcquireForPurge(4, key);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(busy.isEmpty());
            assertTrue(elapsedMillis < 100, "purge busy path must not enter a blocking wait");
        }

        // 2. DML guard 释放后，purge 可取得同一 stripe 并在 task 结束时显式关闭。
        try (PurgeDmlRowGuard acquired = manager.tryAcquireForPurge(4, key).orElseThrow()) {
            assertTrue(true, "purge acquires after DML physical section releases the row guard");
        }
    }

    /**
     * 表 id 是 guard identity 的一部分；相同主键值位于不同表时不得互相阻塞。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>构造相同物化主键并为第一张表取得 guard。</li>
     *     <li>在同一线程为第二张表取得 guard，验证 identity 至少包含 table id。</li>
     * </ol>
     */
    @Test
    void sameKeyInDifferentTablesHasIndependentIdentity() {
        // 1. 第一张表取得 key=9 的 guard。
        PurgeDmlRowGuardManager manager = new PurgeDmlRowGuardManager();
        SearchKey key = key(9);

        // 2. 第二张表的相同 key 不应被视作同一行；两份 guard 均由 try-with-resources 逆序释放。
        try (PurgeDmlRowGuard firstTable = manager.acquireForDml(10, key, Duration.ofSeconds(1));
             PurgeDmlRowGuard secondTable = manager.acquireForDml(11, key, Duration.ofSeconds(1))) {
            assertTrue(true, "table id prevents cross-table row identity aliasing");
        }
    }

    /**
     * 构造单列聚簇主键；SearchKey 自身做防御性复制，可安全参与 guard hash identity。
     *
     * @param id BIGINT 聚簇主键值。
     * @return 只含一个 IntValue part 的完整物化搜索键。
     */
    private static SearchKey key(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }
}
