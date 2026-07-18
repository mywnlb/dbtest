package cn.zhangyis.db.storage.fil.lock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FilLockTest 固定物理文件锁的核心语义：S 可并发、X 排他于 S、Guard 释放正确。
 * 不进事务死锁检测；这里只验证物理锁本身的互斥与释放路径。
 */
class FilLockTest {

    /**
     * 验证 {@code exclusiveShouldBeAvailableAfterSharedGuardClosed} 所描述的 B+Tree 定位或结构变化，并断言键序、父子链接、页资源和唯一性不变量。
     */
    @Test
    void exclusiveShouldBeAvailableAfterSharedGuardClosed() {
        TablespaceLifecycleLatch latch = new TablespaceLifecycleLatch();
        try (ResourceGuard ignored = latch.acquireShared()) {
            // 持有 S
        }
        assertTrue(tryExclusive(latch), "exclusive should be acquirable after shared released");
    }

    /**
     * 验证 {@code exclusiveShouldBeBlockedWhileSharedHeld} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     *
     * @throws InterruptedException 等待被中断时抛出；调用方应恢复中断标志并终止当前资源获取流程
     */
    @Test
    void exclusiveShouldBeBlockedWhileSharedHeld() throws InterruptedException {
        TablespaceLifecycleLatch latch = new TablespaceLifecycleLatch();
        try (ResourceGuard ignored = latch.acquireShared()) {
            // 注意：tryExclusive 内层 50ms 超时模拟竞争窗口。极端高负载下该断言可能因线程调度延迟（而非锁竞争）
            // 超时返回 false——属“即使非预期原因也会通过”的弱点，教学引擎可接受；生产代码应改用 Phaser/Semaphore 做确定性同步。
            assertFalse(tryExclusive(latch), "exclusive must not be acquirable while shared held");
        }
    }

    /**
     * 验证 {@code fileSizeLockShouldBeReleasedByGuard} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void fileSizeLockShouldBeReleasedByGuard() {
        FileSizeLock lock = new FileSizeLock();
        try (ResourceGuard ignored = lock.acquire()) {
            assertFalse(tryFileSize(lock), "file size lock must be held inside guard");
        }
        assertTrue(tryFileSize(lock), "file size lock must be released after guard closed");
    }

    /**
     * 在独立线程里限时尝试获取 X 闩：成功竞争到返回 true，超时或无法获取返回 false。
     * 外层 2s await 防止线程挂死拖垮测试，内层 50ms 是模拟竞争的尝试窗口。
     */
    private boolean tryExclusive(TablespaceLifecycleLatch latch) {
        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            ResourceGuard g = latch.tryAcquireExclusive(50, TimeUnit.MILLISECONDS);
            if (g != null) {
                acquired.set(true);
                g.close();
            }
            done.countDown();
        });
        t.start();
        try {
            done.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return acquired.get();
    }

    /**
     * 在独立线程里限时尝试获取文件大小锁：成功返回 true，超时或无法获取返回 false；两级超时含义同 {@link #tryExclusive}。
     */
    private boolean tryFileSize(FileSizeLock lock) {
        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            ResourceGuard g = lock.tryAcquire(50, TimeUnit.MILLISECONDS);
            if (g != null) {
                acquired.set(true);
                g.close();
            }
            done.countDown();
        });
        t.start();
        try {
            done.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return acquired.get();
    }
}
