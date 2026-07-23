package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.SqlRow;
import cn.zhangyis.db.sql.executor.storage.exception.SqlStorageException;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalInsert;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointSelect;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPointUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeDelete;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeSelect;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalRangeUpdate;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalSecondaryRangeSelect;

import java.util.List;
import java.util.Optional;

/**
 * Executor 可见的最小数据访问端口。端口接收 SQL 物理计划，但隐藏 B+Tree、record、MTR、ReadView、
 * LOB reference 和 statement guard 等存储内部类型。
 */
public interface SqlDataAccessPort {

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
     * 执行主键或唯一二级键点查；RC ReadView 必须覆盖 LOB hydration 和公开行投影。
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param plan 已选择访问索引的点查计划
     * @param deadline 当前语句唯一绝对期限
     * @return 未找到或不可见时为空，否则为不含存储引用的公开行
     * @throws DatabaseValidationException transaction、plan 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException metadata、MVCC、回表或 LOB hydration 失败时抛出
     */
    Optional<SqlRow> selectPoint(
            SqlTransactionHandle transaction, PhysicalPointSelect plan,
            SqlStatementDeadline deadline);

    /**
     * 执行单列普通二级索引 logical-prefix range read。
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param plan non-unique secondary prefix 计划
     * @param deadline 当前语句唯一绝对期限
     * @return 完整、不可变且不含 storage reference 的结果列表
     * @throws DatabaseValidationException transaction、plan 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException 扫描、锁、回表、容量或 LOB 阶段失败时抛出；不得返回部分结果
     */
    List<SqlRow> selectRange(
            SqlTransactionHandle transaction, PhysicalSecondaryRangeSelect plan,
            SqlStatementDeadline deadline);

    /**
     * 执行 comparison、复合前缀或聚簇全扫查询，容量超限不得返回 partial rows。
     *
     * @param transaction 当前 Session 持有的有效不透明事务能力
     * @param plan 带完整 residual 的范围查询计划
     * @param deadline 当前语句唯一绝对期限
     * @return 完整、不可变且不含 storage reference 的结果列表
     * @throws DatabaseValidationException transaction、plan 或 deadline 缺失/无效时抛出
     * @throws SqlStorageException 扫描、residual、容量或 LOB 阶段失败时抛出；不得返回部分结果
     */
    List<SqlRow> selectRange(
            SqlTransactionHandle transaction, PhysicalRangeSelect plan,
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
