package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.storage.SqlDurabilityMode;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Optional;

/** 进程内 Session 的不可变 SQL 语义与全部有界等待配置；不引用 storage 内部枚举。 */
public record SessionOptions(Optional<String> currentSchema, boolean autocommit,
                             SqlIsolationLevel isolationLevel, SqlDurabilityMode durabilityMode,
                             ZoneId zoneId, Duration statementTimeout, Duration metadataLockTimeout,
                             Duration rowLockTimeout, Duration durabilityTimeout) {
    public SessionOptions {
        if (currentSchema == null || isolationLevel == null || durabilityMode == null || zoneId == null
                || !positive(statementTimeout) || !positive(metadataLockTimeout) || !positive(rowLockTimeout)
                || !positive(durabilityTimeout)) {
            throw new DatabaseValidationException("session options require non-null values and positive timeouts");
        }
        if (currentSchema.isPresent() && currentSchema.orElseThrow().isBlank()) {
            throw new DatabaseValidationException("current schema must not be blank");
        }
        if (metadataLockTimeout.compareTo(statementTimeout) > 0
                || rowLockTimeout.compareTo(statementTimeout) > 0
                || durabilityTimeout.compareTo(statementTimeout) > 0) {
            throw new DatabaseValidationException("child timeout must not exceed statement timeout");
        }
    }

    /** 教学实例默认：无 current schema、autocommit、RR、每提交 fsync、UTC 和 5 秒有界等待。 */
    public static SessionOptions defaults() {
        Duration timeout = Duration.ofSeconds(5);
        return new SessionOptions(Optional.empty(), true, SqlIsolationLevel.REPEATABLE_READ,
                SqlDurabilityMode.FLUSH_ON_COMMIT, ZoneId.of("UTC"), timeout, timeout, timeout, timeout);
    }

    private static boolean positive(Duration value) {
        return value != null && !value.isZero() && !value.isNegative();
    }
}
