package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 固定 fil 物理 IO 异常的分类与根因保留：可恢复错误归 Runtime，物理结构损坏归 Fatal。
 */
class FilPhysicalExceptionTest {

    /**
     * 验证 {@code recoverableExceptionsShouldExtendRuntime} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test
    void recoverableExceptionsShouldExtendRuntime() {
        assertInstanceOf(DatabaseRuntimeException.class, new PageOutOfBoundsException("oob"));
        assertInstanceOf(DatabaseRuntimeException.class, new TablespaceNotOpenException("not open"));
        assertInstanceOf(DatabaseRuntimeException.class, new DataFilePhysicalException("io"));
    }

    /**
     * 验证 {@code corruptedShouldBeFatalAndKeepCause} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void corruptedShouldBeFatalAndKeepCause() {
        Throwable cause = new IllegalStateException("misaligned");
        DataFileCorruptedException ex = new DataFileCorruptedException("corrupt", cause);
        assertInstanceOf(DatabaseFatalException.class, ex);
        assertEquals(cause, ex.getCause());
    }

    /**
     * 验证 {@code physicalExceptionShouldKeepCause} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test
    void physicalExceptionShouldKeepCause() {
        Throwable cause = new java.io.IOException("disk");
        DataFilePhysicalException ex = new DataFilePhysicalException("write failed", cause);
        assertEquals(cause, ex.getCause());
    }
}
