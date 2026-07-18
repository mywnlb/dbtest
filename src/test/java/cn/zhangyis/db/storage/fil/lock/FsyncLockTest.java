package cn.zhangyis.db.storage.fil.lock;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * FsyncLock 测试固定 data-file fsync 限流锁的最小语义：同一 data file 上只允许一个 force 进入，
 * 等待路径必须支持 timeout，释放用 RAII guard 保证异常路径不泄漏物理文件锁。
 */
class FsyncLockTest {

    /**
     * 验证 {@code tryAcquireTimesOutWhileAnotherFsyncOwnsThePermitAndSucceedsAfterRelease} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void tryAcquireTimesOutWhileAnotherFsyncOwnsThePermitAndSucceedsAfterRelease() {
        FsyncLock lock = new FsyncLock();

        try (ResourceGuard first = lock.acquire()) {
            assertNull(lock.tryAcquire(Duration.ofMillis(20)));
        }

        try (ResourceGuard second = lock.tryAcquire(Duration.ofMillis(20))) {
            assertNotNull(second);
        }
    }
}
