package cn.zhangyis.db.sql.executor.storage;

import java.time.Duration;

/**
 * Session 使用的事务/XA facade，并组合 Executor 的窄数据端口；真实 transaction、MTR、record 和
 * physical reference 均封装在 adapter 内。
 */
public interface SqlStorageGateway extends SqlDataAccessPort {

    /** 按 Session 请求创建不透明事务句柄；写事务 id 仍由首次 DML 延迟分配。
     *
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code begin} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    SqlTransactionHandle begin(SqlTransactionRequest request);

    /**
     * 在 registry 写 PREPARING 前读取 opaque branch 的稳定 storage 身份。默认实现明确表示
     * 当前 gateway 没有 XA 能力，使已有测试替身和其它实现无需伪造 transaction id。
     *
     * @param transaction 本 gateway 创建且仍为 ACTIVE 的事务能力
     * @return 有写分支返回正 transaction id；只读/未写分支返回 0
     */
    default SqlXaTransactionIdentity xaIdentity(SqlTransactionHandle transaction) {
        throw new cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionStateException(
                "SQL storage gateway does not support XA");
    }

    /**
     * 把已写 PREPARING 的活动写分支强持久为 PREPARED。
     *
     * @param transaction 本 gateway 创建且仍为 ACTIVE 的写事务能力
     * @param timeout phase-one redo durability 的正等待上限
     * @return storage PREPARED durable 结果
     */
    default SqlXaPrepareOutcome prepareXa(SqlTransactionHandle transaction, Duration timeout) {
        throw new cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionStateException(
                "SQL storage gateway does not support XA");
    }

    /**
     * 按已经持久化的提交决议完成 prepared branch；同方向 durability 重试必须幂等。
     *
     * @param transaction prepareXa 返回后保留的 opaque PREPARED 能力
     * @param timeout terminal redo durability 的正等待上限
     * @return durable commit 与锁收尾结果
     */
    default SqlXaCompletionOutcome commitPreparedXa(
            SqlTransactionHandle transaction, Duration timeout) {
        throw new cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionStateException(
                "SQL storage gateway does not support XA");
    }

    /**
     * 按已经持久化的回滚决议完成 prepared branch；同方向 durability 重试必须幂等。
     *
     * @param transaction prepareXa 返回后保留的 opaque PREPARED 能力
     * @param timeout terminal redo durability 的正等待上限
     * @return durable rollback、inverse 与锁收尾结果
     */
    default SqlXaCompletionOutcome rollbackPreparedXa(
            SqlTransactionHandle transaction, Duration timeout) {
        throw new cn.zhangyis.db.sql.executor.storage.exception.SqlTransactionStateException(
                "SQL storage gateway does not support XA");
    }

    /**
     * 在当前 ACTIVE 事务中创建不透明保存点；实现必须同时捕获 undo 与锁获取边界。
     *
     * @param transaction 本 gateway 创建且仍为 ACTIVE 的事务能力
     * @param deadline 当前 SQL 语句的唯一绝对期限；handle 等待不得越过它
     * @return 可用于 rollback/release 的事务归属保存点能力
     */
    SqlSavepointHandle createSavepoint(SqlTransactionHandle transaction, SqlStatementDeadline deadline);

    /**
     * 回滚目标保存点之后的修改并保留目标边界。首写前空边界被消费时，实现返回等价的新能力。
     *
     * @param transaction 保存点所属 ACTIVE 事务
     * @param savepoint 由本 gateway 为同一事务创建且尚未释放的能力
     * @param deadline 当前 SQL 语句的唯一绝对期限
     * @return 回滚后仍代表目标名称的有效能力；可能与输入对象不同
     */
    SqlSavepointHandle rollbackToSavepoint(SqlTransactionHandle transaction,
                                           SqlSavepointHandle savepoint,
                                           SqlStatementDeadline deadline);

    /**
     * 释放单个保存点名称对应的运行期边界，不修改 undo 链或更晚保存点。
     *
     * @param transaction 保存点所属 ACTIVE 事务
     * @param savepoint 由本 gateway 创建且尚未释放的能力
     * @param deadline 当前 SQL 语句的唯一绝对期限
     */
    void releaseSavepoint(SqlTransactionHandle transaction, SqlSavepointHandle savepoint,
                          SqlStatementDeadline deadline);

    /** 提交不透明事务；request 的 timeout 约束 durability 等待，终态结果必须说明是否已持久化。
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param request 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code commit} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request);

    /** 完整回滚不透明事务；只有 storage 已确认终态后才能返回并允许 Session 释放 transaction-duration metadata。
     *
     * @param transaction 调用方持有的 {@code SqlTransactionHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code rollback} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    SqlRollbackOutcome rollback(SqlTransactionHandle transaction);
}
