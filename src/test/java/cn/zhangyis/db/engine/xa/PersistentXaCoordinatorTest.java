package cn.zhangyis.db.engine.xa;

import cn.zhangyis.db.domain.XaId;
import cn.zhangyis.db.sql.executor.storage.SqlStorageGateway;
import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.sql.executor.storage.SqlXaCompletionOutcome;
import cn.zhangyis.db.sql.executor.storage.SqlXaPrepareOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** XA coordinator 的 per-XID owner、绝对 deadline 与 phase-two 幂等边界测试。 */
class PersistentXaCoordinatorTest {

    @TempDir
    Path directory;

    /**
     * 同一 XID 的首个 phase-two 持有 branch owner 执行 storage IO 时，第二个命令必须按自己的总预算超时，
     * 不能无界等待；首命令释放后仍应完成唯一一次 storage commit 和 registry COMPLETED。
     *
     * @throws Exception 并发测试等待被中断或 future 未按期限完成时由 JUnit 报告
     */
    @Test
    void concurrentPhaseTwoWaitUsesBoundedAbsoluteDeadline() throws Exception {
        XaId xid = new XaId(17, new byte[]{7, 1}, new byte[]{2});
        SqlTransactionHandle handle = new SqlTransactionHandle() { };
        CountDownLatch storageEntered = new CountDownLatch(1);
        CountDownLatch releaseStorage = new CountDownLatch(1);
        AtomicInteger commitCalls = new AtomicInteger();
        SqlStorageGateway gateway = gateway(handle, storageEntered, releaseStorage, commitCalls);

        try (FileXaRegistry registry = FileXaRegistry.openOrCreate(directory)) {
            PersistentXaCoordinator coordinator = new PersistentXaCoordinator(registry);
            coordinator.prepare(xid, 71, gateway, handle, Duration.ofSeconds(1));

            // 1、首命令取得 branch owner，并停在不持 registry/map 锁的 storage phase-two 内。
            CompletableFuture<Void> first = CompletableFuture.runAsync(
                    () -> coordinator.commitPrepared(xid, Duration.ofSeconds(2)));
            assertTrue(storageEntered.await(1, TimeUnit.SECONDS));

            // 2、第二命令只能等待自己的 60ms 总预算；超时不得进入 storage 或改变 durable 决议方向。
            long started = System.nanoTime();
            assertThrows(XaException.class,
                    () -> coordinator.commitPrepared(xid, Duration.ofMillis(60)));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue(elapsedMillis < 500, "branch owner wait must remain bounded");
            assertEquals(1, commitCalls.get());

            // 3、释放首命令后，同一 durable commit 决议完成且 registry 不再暴露未完成 branch。
            releaseStorage.countDown();
            first.get(1, TimeUnit.SECONDS);
            assertTrue(registry.pendingEntries().isEmpty());
            assertEquals(1, commitCalls.get());
        } finally {
            releaseStorage.countDown();
        }
    }

    /**
     * 创建只实现本测试所需 XA 方法的动态 gateway；其它调用代表 coordinator 越界。
     *
     * @param expectedHandle prepare/commit 必须收到的同一 opaque handle
     * @param storageEntered commit 进入 fake storage 后发布的并发栅栏
     * @param releaseStorage 测试主线程允许首个 commit 返回的释放栅栏
     * @param commitCalls 实际进入 storage commit 的调用计数
     * @return 只支持 prepareXa/commitPreparedXa 的测试 gateway
     */
    private static SqlStorageGateway gateway(
            SqlTransactionHandle expectedHandle,
            CountDownLatch storageEntered,
            CountDownLatch releaseStorage,
            AtomicInteger commitCalls) {
        return (SqlStorageGateway) Proxy.newProxyInstance(
                SqlStorageGateway.class.getClassLoader(),
                new Class<?>[]{SqlStorageGateway.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "prepareXa" -> {
                        assertEquals(expectedHandle, args[0]);
                        yield new SqlXaPrepareOutcome(71, 1);
                    }
                    case "commitPreparedXa" -> {
                        assertEquals(expectedHandle, args[0]);
                        commitCalls.incrementAndGet();
                        storageEntered.countDown();
                        if (!releaseStorage.await(2, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to release fake XA storage commit");
                        }
                        yield new SqlXaCompletionOutcome(71, true, 1, 2, 0, 0);
                    }
                    case "toString" -> "PersistentXaCoordinatorTestGateway";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new AssertionError(
                            "unexpected SqlStorageGateway method: " + method.getName());
                });
    }
}
