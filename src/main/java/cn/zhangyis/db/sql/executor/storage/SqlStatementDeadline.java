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

    /** 从正 Duration 创建一次绝对终点。
     *
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code after} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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

    /** 返回当前剩余预算；耗尽时抛出带阶段名的 SQL 领域异常。
     *
     * @param operation 传给 {@code remaining} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code remaining} 计算的非负持续时间；零表示无需等待或尚无采样，成功时不返回 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws SqlStatementTimeoutException 操作在约定时限内无法完成时抛出；调用方可回滚或稍后重试
     */
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

    /** 将模块自己的正 timeout 限制在同一 statement 剩余预算内。
     *
     * @param configured 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @param operation 传给 {@code cap} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code cap} 计算的非负持续时间；零表示无需等待或尚无采样，成功时不返回 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Duration cap(Duration configured, String operation) {
        if (configured == null || configured.isZero() || configured.isNegative()) {
            throw new DatabaseValidationException("configured child timeout must be positive");
        }
        Duration remaining = remaining(operation);
        return configured.compareTo(remaining) <= 0 ? configured : remaining;
    }
}
