package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** worker 调度基础设施异常；不同于某条 log 已返回的领域 FAILED 结果。 */
public final class PurgeWorkerExecutionException extends DatabaseRuntimeException {

    /**
     * @param message 区分调度、completion 或 action 包装阶段的诊断消息
     * @param cause 原始运行时或受检异常；不得丢失
     */
    public PurgeWorkerExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
