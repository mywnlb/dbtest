package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * DML facade commit 结果。读写事务会携带真实提交序号；只读事务或未分配写 id 的事务可能仍为
 * {@link TransactionNo#NONE}。
 *
 * @param transactionNo     事务提交序号；不能为 null。
 * @param durable           是否达到命令指定的 redo durability 策略。
 * @param releasedLockCount commit 收尾释放的事务锁数量，必须非负。
 */
public record DmlCommitResult(TransactionNo transactionNo, boolean durable, int releasedLockCount) {

    public DmlCommitResult {
        if (transactionNo == null) {
            throw new DatabaseValidationException("DML commit transactionNo must not be null");
        }
        if (releasedLockCount < 0) {
            throw new DatabaseValidationException(
                    "DML commit releasedLockCount must not be negative: " + releasedLockCount);
        }
    }
}
