package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;

/**
 * 数据库事务保存点的运行期值对象。保存点同时捕获 INSERT/UPDATE 两个独立局部头；当时尚未创建的 log 用
 * {@link UndoLogicalHead#EMPTY} 表示。它不持有页、slot、latch 或事务锁。
 *
 * @param transaction 所属事务实例，仅用于运行期归属校验。
 * @param insertHead   创建时 INSERT log 的精确逻辑头。
 * @param updateHead   创建时 UPDATE log 的精确逻辑头。
 * @param sequence     同一 UndoContext 内单调的诊断序号。
 */
public record TransactionSavepoint(Transaction transaction, UndoLogicalHead insertHead,
                                   UndoLogicalHead updateHead, long sequence) {
    public TransactionSavepoint {
        if (transaction == null || insertHead == null || updateHead == null) {
            throw new DatabaseValidationException("transaction savepoint fields must not be null");
        }
        if (sequence < 0) {
            throw new DatabaseValidationException("transaction savepoint sequence must be non-negative: " + sequence);
        }
    }
}
