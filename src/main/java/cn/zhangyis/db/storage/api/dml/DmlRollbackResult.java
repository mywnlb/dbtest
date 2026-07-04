package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.trx.RollbackSummary;

/**
 * DML facade rollback 结果。summary 来自事务 rollback 执行器，锁释放数量来自 facade 收尾，
 * 二者合在一起便于 session/executor 后续诊断整事务清理结果。
 *
 * @param rollbackSummary  rollback 应用 undo 的摘要；不能为 null。
 * @param releasedLockCount rollback 收尾释放的事务锁数量，必须非负。
 */
public record DmlRollbackResult(RollbackSummary rollbackSummary, int releasedLockCount) {

    public DmlRollbackResult {
        if (rollbackSummary == null) {
            throw new DatabaseValidationException("DML rollback summary must not be null");
        }
        if (releasedLockCount < 0) {
            throw new DatabaseValidationException(
                    "DML rollback releasedLockCount must not be negative: " + releasedLockCount);
        }
    }
}
