package cn.zhangyis.db.common.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 项目异常层次测试确保后续模块不会散落使用 JDK 裸异常，便于统一分类、日志和恢复策略。
 */
class DatabaseExceptionTest {

    @Test
    void runtimeExceptionShouldKeepMessageAndCause() {
        Throwable cause = new IllegalStateException("raw cause");

        DatabaseRuntimeException exception = new DatabaseRuntimeException("domain error", cause);

        assertEquals("domain error", exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertInstanceOf(RuntimeException.class, exception);
    }

    @Test
    void fatalExceptionShouldKeepMessageAndCause() {
        Throwable cause = new IllegalStateException("raw cause");

        DatabaseFatalException exception = new DatabaseFatalException("fatal error", cause);

        assertEquals("fatal error", exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertInstanceOf(DatabaseRuntimeException.class, exception);
    }
}
