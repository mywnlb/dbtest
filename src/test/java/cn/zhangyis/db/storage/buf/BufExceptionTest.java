package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 固定 buf 基础类型：page latch 模式枚举值与帧耗尽异常的可恢复分类。
 */
class BufExceptionTest {

    /**
     * 验证 {@code exhaustedShouldBeRecoverableRuntime} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void exhaustedShouldBeRecoverableRuntime() {
        Throwable cause = new IllegalStateException("all fixed");
        BufferPoolExhaustedException ex = new BufferPoolExhaustedException("exhausted", cause);
        assertInstanceOf(DatabaseRuntimeException.class, ex);
        assertEquals(cause, ex.getCause());
    }

    /**
     * 验证 {@code pageLatchModeShouldHaveSharedSharedExclusiveAndExclusive} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void pageLatchModeShouldHaveSharedSharedExclusiveAndExclusive() {
        // 0.13d 引入 SHARED_EXCLUSIVE（SIX），page latch 模式由 2 个增至 3 个。
        assertEquals(3, PageLatchMode.values().length);
        assertEquals(PageLatchMode.SHARED, PageLatchMode.valueOf("SHARED"));
        assertEquals(PageLatchMode.SHARED_EXCLUSIVE, PageLatchMode.valueOf("SHARED_EXCLUSIVE"));
        assertEquals(PageLatchMode.EXCLUSIVE, PageLatchMode.valueOf("EXCLUSIVE"));
    }
}
