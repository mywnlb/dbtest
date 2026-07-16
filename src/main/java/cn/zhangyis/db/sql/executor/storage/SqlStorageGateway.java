package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPrimaryPointSelect;
import cn.zhangyis.db.sql.executor.SqlRow;

import java.util.Optional;

/** SQL executor 到存储内核的唯一 port；真实 transaction/MTR/record/physical reference 均封装在 adapter 内。 */
public interface SqlStorageGateway {
    SqlTransactionHandle begin(SqlTransactionRequest request);
    SqlWriteOutcome insert(SqlTransactionHandle transaction, BoundClusteredInsert statement);
    Optional<SqlRow> selectPoint(SqlTransactionHandle transaction, BoundPrimaryPointSelect statement);
    SqlCommitOutcome commit(SqlTransactionHandle transaction, SqlCommitRequest request);
    SqlRollbackOutcome rollback(SqlTransactionHandle transaction);
}
