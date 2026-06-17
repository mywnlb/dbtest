package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 固定 buf 基础类型：page latch 模式枚举值与帧耗尽异常的可恢复分类。
 */
class BufExceptionTest {

    @Test
    void exhaustedShouldBeRecoverableRuntime() {
        Throwable cause = new IllegalStateException("all fixed");
        BufferPoolExhaustedException ex = new BufferPoolExhaustedException("exhausted", cause);
        assertInstanceOf(DatabaseRuntimeException.class, ex);
        assertEquals(cause, ex.getCause());
    }

    @Test
    void pageLatchModeShouldHaveSharedAndExclusive() {
        assertEquals(2, PageLatchMode.values().length);
        assertEquals(PageLatchMode.SHARED, PageLatchMode.valueOf("SHARED"));
        assertEquals(PageLatchMode.EXCLUSIVE, PageLatchMode.valueOf("EXCLUSIVE"));
    }
}
