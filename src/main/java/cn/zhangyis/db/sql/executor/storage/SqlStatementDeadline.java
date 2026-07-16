package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStatementTimeoutException;

import java.time.Duration;

/**
 * 一条 SQL 从 Session admission 到 storage/durability cleanup 共享的绝对单调时钟 deadline。下游只能读取剩余预算或
 * 与自身配置 timeout 取较小值，不能重新创建完整相对 timeout，否则串行 MDL、row lock、redo wait 会突破语句上限。
 */
public final class SqlStatementDeadline {

    /** System.nanoTime 时间轴上的终点；Long.MAX_VALUE 表示 Duration 加法溢出后的可表示上限。 */
    private final long deadlineNanos;

    private SqlStatementDeadline(long deadlineNanos) {
        this.deadlineNanos = deadlineNanos;
    }

    /** 从正 Duration 创建一次绝对终点。 */
    public static SqlStatementDeadline after(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("SQL statement timeout must be positive");
        }
        long nanos;
        try {
            nanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            return new SqlStatementDeadline(Long.MAX_VALUE);
        }
        long now = System.nanoTime();
        try {
            return new SqlStatementDeadline(Math.addExact(now, nanos));
        } catch (ArithmeticException overflow) {
            return new SqlStatementDeadline(Long.MAX_VALUE);
        }
    }

    /** 返回当前剩余预算；耗尽时抛出带阶段名的 SQL 领域异常。 */
    public Duration remaining(String operation) {
        if (operation == null || operation.isBlank()) {
            throw new DatabaseValidationException("statement deadline operation must not be blank");
        }
        long nanos = deadlineNanos == Long.MAX_VALUE
                ? Long.MAX_VALUE : deadlineNanos - System.nanoTime();
        if (nanos <= 0) {
            throw new SqlStatementTimeoutException("statement deadline expired before " + operation);
        }
        return Duration.ofNanos(nanos);
    }

    /** 将模块自己的正 timeout 限制在同一 statement 剩余预算内。 */
    public Duration cap(Duration configured, String operation) {
        if (configured == null || configured.isZero() || configured.isNegative()) {
            throw new DatabaseValidationException("configured child timeout must be positive");
        }
        Duration remaining = remaining(operation);
        return configured.compareTo(remaining) <= 0 ? configured : remaining;
    }
}
