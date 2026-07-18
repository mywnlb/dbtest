package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;

/** SQL commit 的显式 durability/timeout 请求。
 *
 * @param durabilityMode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
 */
public record SqlCommitRequest(SqlDurabilityMode durabilityMode, Duration timeout) {
    public SqlCommitRequest {
        if (durabilityMode == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("SQL commit durability and positive timeout are required");
        }
    }
}
