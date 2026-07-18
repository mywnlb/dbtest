package cn.zhangyis.db.sql.executor.storage.exception;

/** commit/rollback 已跨状态转换边界而响应结果无法普通重试。 */
public final class SqlTransactionOutcomeException extends SqlStorageException {
    /**
     * 记录 {@code terminal} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private final boolean terminal;
    /**
     * 记录 {@code outcomeUncertain} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    private final boolean outcomeUncertain;
    /**
     * 创建 {@code SqlTransactionOutcomeException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param terminal 资源是否处于删除、空闲、静默、持久化或终态；必须与权威状态机一致，不能由调用方猜测
     * @param outcomeUncertain 恢复容错策略标志；只允许在契约明确的损坏或结果不确定场景放宽校验，不得掩盖其他数据损坏
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public SqlTransactionOutcomeException(String message, boolean terminal, boolean outcomeUncertain,
                                          Throwable cause) {
        super(message, cause);
        this.terminal = terminal;
        this.outcomeUncertain = outcomeUncertain;
    }
    public boolean terminal() { return terminal; }
    public boolean outcomeUncertain() { return outcomeUncertain; }
}
