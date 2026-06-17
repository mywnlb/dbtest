package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseFatalException;
import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 固定 fil 物理 IO 异常的分类与根因保留：可恢复错误归 Runtime，物理结构损坏归 Fatal。
 */
class FilPhysicalExceptionTest {

    @Test
    void recoverableExceptionsShouldExtendRuntime() {
        assertInstanceOf(DatabaseRuntimeException.class, new PageOutOfBoundsException("oob"));
        assertInstanceOf(DatabaseRuntimeException.class, new TablespaceNotOpenException("not open"));
        assertInstanceOf(DatabaseRuntimeException.class, new DataFilePhysicalException("io"));
    }

    @Test
    void corruptedShouldBeFatalAndKeepCause() {
        Throwable cause = new IllegalStateException("misaligned");
        DataFileCorruptedException ex = new DataFileCorruptedException("corrupt", cause);
        assertInstanceOf(DatabaseFatalException.class, ex);
        assertEquals(cause, ex.getCause());
    }

    @Test
    void physicalExceptionShouldKeepCause() {
        Throwable cause = new java.io.IOException("disk");
        DataFilePhysicalException ex = new DataFilePhysicalException("write failed", cause);
        assertEquals(cause, ex.getCause());
    }
}
