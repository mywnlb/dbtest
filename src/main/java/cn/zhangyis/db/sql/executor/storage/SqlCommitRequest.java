package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.time.Duration;

/** SQL commit 的显式 durability/timeout 请求。 */
public record SqlCommitRequest(SqlDurabilityMode durabilityMode, Duration timeout) {
    public SqlCommitRequest {
        if (durabilityMode == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("SQL commit durability and positive timeout are required");
        }
    }
}
