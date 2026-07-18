package cn.zhangyis.db.sql.executor.storage.exception;

/** 写语句失败且 statement rollback 未能确认；真实事务已 rollback-only。 */
public final class SqlStatementRollbackException extends SqlStorageException {
    /**
     * 记录 {@code rollbackOnly} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private final boolean rollbackOnly;
    /**
     * 创建 {@code SqlStatementRollbackException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param rollbackOnly 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public SqlStatementRollbackException(String message, boolean rollbackOnly, Throwable cause) {
        super(message, cause);
        this.rollbackOnly = rollbackOnly;
    }
    public boolean rollbackOnly() { return rollbackOnly; }
}
