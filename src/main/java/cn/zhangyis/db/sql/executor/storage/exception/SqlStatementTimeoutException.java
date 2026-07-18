package cn.zhangyis.db.sql.executor.storage.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 单条 SQL 的共享绝对 deadline 已耗尽；调用方可回滚当前 statement/transaction 后继续。 */
public final class SqlStatementTimeoutException extends DatabaseRuntimeException {
    /**
     * 创建 {@code SqlStatementTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public SqlStatementTimeoutException(String message) {
        super(message);
    }
}
