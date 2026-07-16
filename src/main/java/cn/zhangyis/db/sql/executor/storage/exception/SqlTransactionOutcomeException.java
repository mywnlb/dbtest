package cn.zhangyis.db.sql.executor.storage.exception;

/** commit/rollback 已跨状态转换边界而响应结果无法普通重试。 */
public final class SqlTransactionOutcomeException extends SqlStorageException {
    private final boolean terminal;
    private final boolean outcomeUncertain;
    public SqlTransactionOutcomeException(String message, boolean terminal, boolean outcomeUncertain,
                                          Throwable cause) {
        super(message, cause);
        this.terminal = terminal;
        this.outcomeUncertain = outcomeUncertain;
    }
    public boolean terminal() { return terminal; }
    public boolean outcomeUncertain() { return outcomeUncertain; }
}
