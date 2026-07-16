package cn.zhangyis.db.session.exception;

/** storage 已跨 commit/rollback 边界但无法向调用方确认结果，Session 必须进入 FAILED。 */
public final class TransactionOutcomeUnknownException extends SessionStateException {
    public TransactionOutcomeUnknownException(String message, Throwable cause) { super(message, cause); }
}
