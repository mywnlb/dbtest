package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStorageException;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalInsert;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalAccess;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeUpdate;

/**
 * Executor 可见的最小数据访问端口。端口接收 SQL 物理计划，但隐藏 B+Tree、record、MTR、ReadView、
 * LOB reference 和 statement guard 等存储内部类型。
 */
public interface SqlDataAccessPort {

    /**
     * 打开单条查询的 cursor scope。默认适配器保留旧端口实现兼容性：cursor 仍由
     * {@link #openCursor(SqlTransactionHandle, PhysicalAccess, SqlStatementDeadline)}
     * 创建；生产 gateway 覆盖本方法以共享 operation lease 与 ReadView。
     *
     * @param transaction 当前 Session 的有效不透明事务能力
     * @param deadline 当前语句唯一绝对期限
     * @return Executor 必须在根节点关闭后关闭的语句 scope
     */
    default SqlCursorScope openCursorScope(
            SqlTransactionHandle transaction,
            SqlStatementDeadline deadline) {
        if (transaction == null || deadline == null) {
            throw new DatabaseValidationException(
                    "cursor scope transaction/deadline must not be null");
        }
        SqlDataAccessPort owner = this;
        return new SqlCursorScope() {
            private boolean closed;

            @Override
            public SqlStorageCursor openCursor(
                    PhysicalAccess access) {
                if (closed) {
                    throw new SqlStorageException(
                            "SQL cursor scope is closed");
                }
                return owner.openCursor(
                        transaction, access, deadline);
            }

            @Override
            public void close() {
                closed = true;
            }
        };
    }

    /**
     * 在调用方事务内执行完整单行 INSERT。
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param plan 已通过 optimizer 校验的完整行物理计划
     * @param deadline 当前语句唯一绝对期限
     * @return affected rows 与 rollback-only 状态
     * @throws DatabaseValidationException transaction、plan 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException metadata 映射或原子写入失败时抛出；实现必须保留底层 cause
     */
    SqlWriteOutcome insert(
            SqlTransactionHandle transaction, PhysicalInsert plan,
            SqlStatementDeadline deadline);

    /**
     * 执行完整聚簇主键点 UPDATE。
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param plan 已通过 optimizer 校验的点修改计划
     * @param deadline 当前语句唯一绝对期限
     * @return affected rows 与 rollback-only 状态
     * @throws DatabaseValidationException transaction、plan 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException 记录重定位、锁、undo 或多索引写入失败时抛出
     */
    SqlWriteOutcome update(
            SqlTransactionHandle transaction, PhysicalPointUpdate plan,
            SqlStatementDeadline deadline);

    /**
     * 执行完整聚簇主键点 DELETE。
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param plan 已通过 optimizer 校验的点删除计划
     * @param deadline 当前语句唯一绝对期限
     * @return affected rows 与 rollback-only 状态
     * @throws DatabaseValidationException transaction、plan 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException 记录重定位、锁、undo 或 delete-mark 失败时抛出
     */
    SqlWriteOutcome delete(
            SqlTransactionHandle transaction, PhysicalPointDelete plan,
            SqlStatementDeadline deadline);

    /**
     * 打开一个只产生完整逻辑候选行的 pull cursor。最终 residual 与 projection 由 Executor
     * Filter/Project 负责；实现只执行 access path、MVCC/current-read、回表和行值适配。
     *
     * <ol>
     *     <li>有界取得 transaction operation lease，并校验 handle 仍为 ACTIVE。</li>
     *     <li>按 sealed access 选择 point、logical secondary prefix 或通用 range cursor。</li>
     *     <li>cursor advance 以短 MTR/锁重定位产生一行，返回前释放所有 page latch/fix。</li>
     *     <li>cursor close 释放 RC ReadView 与 operation lease；事务行锁继续保留到终态。</li>
     * </ol>
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param access Optimizer 选择且不含 residual/projection 的物理访问叶
     * @param deadline 当前语句唯一绝对期限
     * @return 已打开且必须由调用方关闭的 SQL storage cursor
     * @throws DatabaseValidationException transaction、access 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException metadata、ReadView、锁或 cursor 初始化失败时抛出
     */
    SqlStorageCursor openCursor(
            SqlTransactionHandle transaction, PhysicalAccess access,
            SqlStatementDeadline deadline);

    /**
     * 先锁定并物化完整聚簇 identity 集，再在一个 statement guard 内执行 UPDATE。
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param plan 带 candidate range 与 residual 的原子范围修改计划
     * @param deadline 当前语句唯一绝对期限
     * @return affected rows 与 rollback-only 状态
     * @throws DatabaseValidationException transaction、plan 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException candidate 物化、锁或任一写入失败时抛出；statement guard 必须回滚
     */
    SqlWriteOutcome updateRange(
            SqlTransactionHandle transaction, PhysicalRangeUpdate plan,
            SqlStatementDeadline deadline);

    /**
     * 先锁定并物化完整聚簇 identity 集，再在一个 statement guard 内执行 DELETE。
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param plan 带 candidate range 与 residual 的原子范围删除计划
     * @param deadline 当前语句唯一绝对期限
     * @return affected rows 与 rollback-only 状态
     * @throws DatabaseValidationException transaction、plan 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException candidate 物化、锁或任一标删失败时抛出；statement guard 必须回滚
     */
    SqlWriteOutcome deleteRange(
            SqlTransactionHandle transaction, PhysicalRangeDelete plan,
            SqlStatementDeadline deadline);
}
