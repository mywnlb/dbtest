package cn.zhangyis.db.storage.fil.access;

import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 表空间 operation lease 测试：共享访问可并行，截断独占 lease 必须 drain 已进入的普通访问。 */
class TablespaceAccessControllerTest {

    /**
     * 验证 {@code exclusiveLeaseWaitsUntilSharedLeaseCloses} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void exclusiveLeaseWaitsUntilSharedLeaseCloses() throws Exception {
        TablespaceAccessController controller = new TablespaceAccessController(Duration.ofSeconds(2));
        SpaceId spaceId = SpaceId.of(77);
        TablespaceAccessLease shared = controller.acquireShared(spaceId);

        CompletableFuture<Void> exclusive = CompletableFuture.runAsync(() -> {
            try (TablespaceAccessLease ignored = controller.acquireExclusive(spaceId)) {
                // 成功取得即可；future 的完成时刻用于验证 drain 边界。
            }
        });

        TimeUnit.MILLISECONDS.sleep(100);
        assertFalse(exclusive.isDone());
        shared.close();
        exclusive.get(1, TimeUnit.SECONDS);
        assertTrue(exclusive.isDone());
    }

    /**
     * 验证 {@code waitingLeaseHasBoundedTimeout} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void waitingLeaseHasBoundedTimeout() {
        TablespaceAccessController controller = new TablespaceAccessController(Duration.ofMillis(50));
        SpaceId spaceId = SpaceId.of(78);
        try (TablespaceAccessLease ignored = controller.acquireShared(spaceId)) {
            CompletableFuture<Void> exclusive = CompletableFuture.runAsync(() ->
                    assertThrows(TablespaceAccessTimeoutException.class,
                            () -> controller.acquireExclusive(spaceId)));
            exclusive.join();
        }
    }

    /** maintenance 的零等待 X lease 在普通 S owner 存在时必须立即返回 empty，且不能影响 owner 后续释放。 */
    @Test
    void tryExclusiveReturnsEmptyWithoutWaitingForSharedOwner() {
        TablespaceAccessController controller = new TablespaceAccessController(Duration.ofSeconds(2));
        SpaceId spaceId = SpaceId.of(80);
        try (TablespaceAccessLease ignored = controller.acquireShared(spaceId)) {
            long started = System.nanoTime();

            assertTrue(controller.tryAcquireExclusive(spaceId).isEmpty());
            assertTrue(System.nanoTime() - started < Duration.ofMillis(250).toNanos(),
                    "zero-wait maintenance lease must not consume the configured two-second timeout");
        }

        try (TablespaceAccessLease ignored = controller.tryAcquireExclusive(spaceId).orElseThrow()) {
            // 共享 owner 释放后应立即取得独占 lease；try-with-resources 同时验证 owner 线程释放路径。
        }
    }

    /** 跨线程 close 必须被拒绝且不能把 lease 标成已释放，owner 随后仍能正常关闭。 */
    @Test
    void leaseCannotBeClosedByAnotherThread() {
        TablespaceAccessController controller = new TablespaceAccessController(Duration.ofSeconds(1));
        TablespaceAccessLease lease = controller.acquireShared(SpaceId.of(79));
        CompletableFuture<Void> wrongThread = CompletableFuture.runAsync(() ->
                assertThrows(RuntimeException.class, lease::close));
        wrongThread.join();
        lease.close();
        try (TablespaceAccessLease ignored = controller.acquireExclusive(SpaceId.of(79))) {
            // owner 正常释放共享 lease 后，独占 lease 可立即取得。
        }
    }
}
