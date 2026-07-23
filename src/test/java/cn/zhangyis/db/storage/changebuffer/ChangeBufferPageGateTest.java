package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Change Buffer per-target 显式 gate 的等待边界测试。 */
class ChangeBufferPageGateTest {

    /** 极大合法 Duration 的纳秒换算应饱和，空闲 gate 仍可立即取得，不能因算术溢出拒绝配置。 */
    @Test
    void hugeTimeoutSaturatesAndAcquiresAvailableGate() {
        ChangeBufferPageGate gate = new ChangeBufferPageGate(4);
        PageId target = PageId.of(SpaceId.of(91), PageNo.of(8));

        assertDoesNotThrow(() -> {
            try (ChangeBufferPageGate.Lease ignored =
                         gate.acquire(target, Duration.ofSeconds(Long.MAX_VALUE))) {
                // lease close 是本测试的释放断言；重复获取计数不会泄漏到后续测试。
            }
        });
    }

    /** lease 只能由取得条带 hold 的线程关闭；跨线程误释放应转成领域异常并保留 owner 的锁所有权。 */
    @Test
    void rejectsCloseFromNonOwnerThreadWithoutReleasingGate() throws Exception {
        ChangeBufferPageGate gate = new ChangeBufferPageGate(4);
        PageId target = PageId.of(SpaceId.of(91), PageNo.of(9));
        ChangeBufferPageGate.Lease lease = gate.acquire(target, Duration.ofSeconds(1));
        try {
            CompletableFuture<Throwable> wrongOwner = CompletableFuture.supplyAsync(() -> {
                try {
                    lease.close();
                    return null;
                } catch (Throwable failure) {
                    return failure;
                }
            });
            assertInstanceOf(DatabaseValidationException.class,
                    wrongOwner.get(1, TimeUnit.SECONDS));
        } finally {
            lease.close();
        }
    }
}
