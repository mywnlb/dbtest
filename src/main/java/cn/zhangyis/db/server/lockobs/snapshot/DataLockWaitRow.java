package cn.zhangyis.db.server.lockobs.snapshot;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * Performance Schema `data_lock_waits` 风格行。一个等待请求可能被多个 granted lock 阻塞，因此同一
 * requesting lock id 可对应多行。
 *
 * @param engine                         存储引擎名。
 * @param requestingEngineLockId         等待锁 id。
 * @param requestingEngineTransactionId  等待事务。
 * @param requestingThreadId             等待线程 id。
 * @param requestingEventId              等待事件 id。
 * @param requestingObjectInstanceId     等待对象实例 id。
 * @param blockingEngineLockId           阻塞锁 id。
 * @param blockingEngineTransactionId    阻塞事务。
 * @param blockingThreadId               阻塞线程 id。
 * @param blockingEventId                阻塞事件 id。
 * @param blockingObjectInstanceId       阻塞对象实例 id。
 */
public record DataLockWaitRow(String engine, String requestingEngineLockId,
                              TransactionId requestingEngineTransactionId, long requestingThreadId,
                              long requestingEventId, String requestingObjectInstanceId,
                              String blockingEngineLockId, TransactionId blockingEngineTransactionId,
                              long blockingThreadId, long blockingEventId, String blockingObjectInstanceId) {

    public DataLockWaitRow {
        if (engine == null || engine.isBlank()) {
            throw new DatabaseValidationException("data_lock_waits engine must not be blank");
        }
        if (requestingEngineLockId == null || requestingEngineLockId.isBlank()) {
            throw new DatabaseValidationException("requesting engine lock id must not be blank");
        }
        if (blockingEngineLockId == null || blockingEngineLockId.isBlank()) {
            throw new DatabaseValidationException("blocking engine lock id must not be blank");
        }
        if (requestingEngineTransactionId == null || requestingEngineTransactionId.isNone()) {
            throw new DatabaseValidationException("requesting transaction id must be real");
        }
        if (blockingEngineTransactionId == null || blockingEngineTransactionId.isNone()) {
            throw new DatabaseValidationException("blocking transaction id must be real");
        }
        if (requestingThreadId < 0 || requestingEventId < 0 || blockingThreadId < 0 || blockingEventId < 0) {
            throw new DatabaseValidationException("thread/event ids must be non-negative");
        }
        if (requestingObjectInstanceId == null || requestingObjectInstanceId.isBlank()) {
            throw new DatabaseValidationException("requesting object instance id must not be blank");
        }
        if (blockingObjectInstanceId == null || blockingObjectInstanceId.isBlank()) {
            throw new DatabaseValidationException("blocking object instance id must not be blank");
        }
    }
}
