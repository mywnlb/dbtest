package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundSecondaryRangeSelect;
import cn.zhangyis.db.sql.executor.SqlRow;

import java.util.List;
import java.util.Optional;

/** SQL executor 到存储内核的唯一 port；真实 transaction/MTR/record/physical reference 均封装在 adapter 内。 */
public interface SqlStorageGateway {

    /** 按 Session 请求创建不透明事务句柄；写事务 id 仍由首次 DML 延迟分配。 */
    SqlTransactionHandle begin(SqlTransactionRequest request);

    /**
     * 执行已绑定的单行聚簇 INSERT。adapter 必须让 handle wait、行锁与下游阶段共同受同一绝对 deadline 限制。
     */
    SqlWriteOutcome insert(SqlTransactionHandle transaction, BoundClusteredInsert statement,
                           SqlStatementDeadline deadline);

    /** 执行完整聚簇主键定位的 typed column patch。 */
    SqlWriteOutcome update(SqlTransactionHandle transaction, BoundUpdate statement,
                           SqlStatementDeadline deadline);

    /** 执行完整聚簇主键定位的逻辑删除。 */
    SqlWriteOutcome delete(SqlTransactionHandle transaction, BoundDelete statement,
                           SqlStatementDeadline deadline);

    /**
     * 执行已绑定的聚簇主键点查。RC ReadView 必须存活到 external LOB hydrate 和公开行投影都完成后。
     */
    Optional<SqlRow> selectPoint(SqlTransactionHandle transaction, BoundPointSelect statement,
                                 SqlStatementDeadline deadline);

    /**
     * 执行 non-unique secondary logical-prefix range read；结果必须完整且按稳定索引顺序返回，不能静默截断。
     *
     * @param transaction 由本 gateway 创建且仍为 ACTIVE 的不透明事务句柄；locking 模式必须对应读写事务。
     * @param statement Binder 固定 exact table version、访问索引、logical key、投影和 locking mode 的计划。
     * @param deadline 从 Session 语句入口创建的绝对期限，必须继续约束 handle、锁等待和 LOB hydration。
     * @return 不含 storage reference 的不可变公开行列表；没有可见匹配或 SQL NULL equality 时为空。
     * @throws RuntimeException metadata 映射、MVCC/current-read、锁等待、容量保护或 LOB 读取失败时抛出；
     *         实现必须保留底层 cause，且不得返回部分结果。
     */
    List<SqlRow> selectRange(SqlTransactionHandle transaction, BoundSecondaryRangeSelect statement,
                             SqlStatementDeadline deadline);

    /** 提交不透明事务；request 的 timeout 约束 durability 等待，终态结果必须说明是否已持久化。 */
    SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request);

    /** 完整回滚不透明事务；只有 storage 已确认终态后才能返回并允许 Session 释放 transaction-duration metadata。 */
    SqlRollbackOutcome rollback(SqlTransactionHandle transaction);
}
