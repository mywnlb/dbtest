package cn.zhangyis.db.sql.executor;

/** 一条 SQL 返回时与用户相关的事务状态，不携带内部事务 id 或 undo 状态。
 *
 * @param autocommit 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 * @param transactionActive 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 * @param rollbackOnly 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 */
public record TransactionStatus(boolean autocommit, boolean transactionActive, boolean rollbackOnly) {
    public TransactionStatus {
        if (!transactionActive && rollbackOnly) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "rollback-only requires an active transaction");
        }
    }

    /**
     * 根据调用参数创建或转换 {@code idle} 返回的 {@code TransactionStatus}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param autocommit 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
     * @return {@code idle} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     */
    public static TransactionStatus idle(boolean autocommit) {
        return new TransactionStatus(autocommit, false, false);
    }
}
