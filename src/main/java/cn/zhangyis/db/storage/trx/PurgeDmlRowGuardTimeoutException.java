package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** DML 在配置的有界时间内未取得行物理协调 guard；调用方可回滚 statement 或报告锁等待超时。 */
public final class PurgeDmlRowGuardTimeoutException extends DatabaseRuntimeException {

    /**
     * 用领域上下文构造无底层 cause 的有界等待超时。
     *
     * @param message 包含 table id、cluster key 与配置超时的诊断信息。
     */
    public PurgeDmlRowGuardTimeoutException(String message) {
        super(message);
    }

    /**
     * 包装底层计时或等待失败并保留原始原因。
     *
     * @param message 包含目标行 identity 与等待边界的诊断信息。
     * @param cause   导致 row guard 未能取得的底层异常。
     */
    public PurgeDmlRowGuardTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
