package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.storage.SqlDurabilityMode;
import cn.zhangyis.db.sql.executor.storage.SqlIsolationLevel;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Optional;

/** 进程内 Session 的不可变 SQL 语义与全部有界等待配置；不引用 storage 内部枚举。
 *
 * @param currentSchema 可选的 {@code currentSchema}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
 * @param autocommit 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 * @param isolationLevel 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
 * @param durabilityMode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 * @param zoneId 参与 {@code 构造} 的稳定领域标识 {@code ZoneId}；不得为 {@code null}，并须由对应值对象构造校验产生
 * @param statementTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
 * @param metadataLockTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
 * @param rowLockTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
 * @param durabilityTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
 */
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

    /** 教学实例默认：无 current schema、autocommit、RR、每提交 fsync、UTC 和 5 秒有界等待。
     *
     * @return {@code defaults} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     */
    public static SessionOptions defaults() {
        Duration timeout = Duration.ofSeconds(5);
        return new SessionOptions(Optional.empty(), true, SqlIsolationLevel.REPEATABLE_READ,
                SqlDurabilityMode.FLUSH_ON_COMMIT, ZoneId.of("UTC"), timeout, timeout, timeout, timeout);
    }

    private static boolean positive(Duration value) {
        return value != null && !value.isZero() && !value.isNegative();
    }
}
