package cn.zhangyis.db.server.lockobs.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.trx.lock.ThreadEventId;

/**
 * 当前等待槽快照。一个 Java thread 同时只应有一个 current wait slot；本对象只用于诊断展示，
 * 不携带可唤醒等待线程的 Condition 或 LockManager 内部引用。
 *
 * @param threadEventId   当前等待事件。
 * @param sessionId       session id；session 层未接时为 0。
 * @param statementId     statement id；session 层未接时为 0。
 * @param transactionId   等待事务。
 * @param waitState       WAITING/TIMEOUT/DEADLOCK_VICTIM 等诊断状态。
 * @param eventName       Performance Schema 风格事件名。
 * @param objectInstanceId 被等待对象实例 id。
 * @param timerStartNanos 创建等待槽时的 nanoTime。
 * @param deadlineNanos   等待超时点；无明确 deadline 时为 0。
 */
public record WaitSlotSnapshot(ThreadEventId threadEventId, long sessionId, long statementId,
                               TransactionId transactionId, String waitState, String eventName,
                               String objectInstanceId, long timerStartNanos, long deadlineNanos) {

    public WaitSlotSnapshot {
        if (threadEventId == null || !threadEventId.real()) {
            throw new DatabaseValidationException("wait slot requires a real thread event id");
        }
        if (sessionId < 0 || statementId < 0) {
            throw new DatabaseValidationException("session/statement id must be non-negative");
        }
        if (transactionId == null || transactionId.isNone()) {
            throw new DatabaseValidationException("wait slot transaction id must be real");
        }
        if (waitState == null || waitState.isBlank()) {
            throw new DatabaseValidationException("wait slot state must not be blank");
        }
        if (eventName == null || eventName.isBlank()) {
            throw new DatabaseValidationException("wait slot event name must not be blank");
        }
        if (objectInstanceId == null || objectInstanceId.isBlank()) {
            throw new DatabaseValidationException("wait slot object instance id must not be blank");
        }
        if (timerStartNanos <= 0) {
            throw new DatabaseValidationException("wait slot timer start must be positive");
        }
        if (deadlineNanos < 0) {
            throw new DatabaseValidationException("wait slot deadline must be non-negative");
        }
    }
}
