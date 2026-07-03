package cn.zhangyis.db.server.lockobs.snapshot;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * Performance Schema `data_locks` 风格行。字段值是诊断输出，不作为执行器或应用协议的稳定输入。
 *
 * @param engine              存储引擎名，当前固定 INNODB。
 * @param engineLockId        观测层稳定锁 id。
 * @param engineTransactionId 持有或等待该锁的事务。
 * @param threadId            请求线程 id；未接观测时为 0。
 * @param eventId             请求事件 id；未接观测时为 0。
 * @param objectSchema        schema 名；DD 未接时为空字符串。
 * @param objectName          table 名；DD 未接时为空字符串。
 * @param indexName           索引诊断名。
 * @param objectInstanceId    诊断对象实例 id。
 * @param lockType            RECORD/GAP/NEXT_KEY/INSERT_INTENTION。
 * @param lockMode            MySQL 风格锁模式摘要。
 * @param lockStatus          GRANTED/WAITING。
 * @param lockData            资源定位摘要。
 */
public record DataLockRow(String engine, String engineLockId, TransactionId engineTransactionId,
                          long threadId, long eventId, String objectSchema, String objectName,
                          String indexName, String objectInstanceId, String lockType, String lockMode,
                          String lockStatus, String lockData) {

    public DataLockRow {
        if (engine == null || engine.isBlank()) {
            throw new DatabaseValidationException("data_locks engine must not be blank");
        }
        if (engineLockId == null || engineLockId.isBlank()) {
            throw new DatabaseValidationException("data_locks engine lock id must not be blank");
        }
        if (engineTransactionId == null || engineTransactionId.isNone()) {
            throw new DatabaseValidationException("data_locks transaction id must be real");
        }
        if (threadId < 0 || eventId < 0) {
            throw new DatabaseValidationException("data_locks thread/event id must be non-negative");
        }
        objectSchema = objectSchema == null ? "" : objectSchema;
        objectName = objectName == null ? "" : objectName;
        if (indexName == null || indexName.isBlank()) {
            throw new DatabaseValidationException("data_locks index name must not be blank");
        }
        if (objectInstanceId == null || objectInstanceId.isBlank()) {
            throw new DatabaseValidationException("data_locks object instance id must not be blank");
        }
        if (lockType == null || lockType.isBlank()) {
            throw new DatabaseValidationException("data_locks lock type must not be blank");
        }
        if (lockMode == null || lockMode.isBlank()) {
            throw new DatabaseValidationException("data_locks lock mode must not be blank");
        }
        if (lockStatus == null || lockStatus.isBlank()) {
            throw new DatabaseValidationException("data_locks lock status must not be blank");
        }
        lockData = lockData == null ? "" : lockData;
    }
}
