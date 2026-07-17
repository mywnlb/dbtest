package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.executor.SqlRow;

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

    /**
     * 执行已绑定的聚簇主键点查。RC ReadView 必须存活到 external LOB hydrate 和公开行投影都完成后。
     */
    Optional<SqlRow> selectPoint(SqlTransactionHandle transaction, BoundPointSelect statement,
                                 SqlStatementDeadline deadline);

    /** 提交不透明事务；request 的 timeout 约束 durability 等待，终态结果必须说明是否已持久化。 */
    SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request);

    /** 完整回滚不透明事务；只有 storage 已确认终态后才能返回并允许 Session 释放 transaction-duration metadata。 */
    SqlRollbackOutcome rollback(SqlTransactionHandle transaction);
}
