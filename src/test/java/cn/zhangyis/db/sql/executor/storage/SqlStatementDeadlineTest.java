package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.executor.storage.exception.SqlStatementTimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/** SQL statement absolute deadline 的值对象边界测试。 */
class SqlStatementDeadlineTest {

    /** 所有下游等待共享同一绝对终点；cap 不能因重新创建相对 timeout 延长语句。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void capsEachWaitAgainstOneAbsoluteDeadline() throws Exception {
        SqlStatementDeadline deadline = SqlStatementDeadline.after(Duration.ofMillis(120));
        Duration first = deadline.cap(Duration.ofSeconds(1), "first wait");
        Thread.sleep(35);
        Duration second = deadline.cap(Duration.ofSeconds(1), "second wait");

        assertTrue(first.compareTo(Duration.ofMillis(120)) <= 0);
        assertTrue(second.compareTo(first) < 0, "remaining budget must shrink instead of resetting");
        Thread.sleep(100);
        assertThrows(SqlStatementTimeoutException.class,
                () -> deadline.remaining("expired wait"));
    }
}
