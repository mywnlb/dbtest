package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.UndoNo;

/**
 * 数据库事务保存点的运行期值对象。
 *
 * <p>保存点只记录 undo 逻辑边界，不复制 undo record，也不持有页 latch、事务锁或 MTR 资源。所属事务用
 * {@link Transaction} 引用表达，因为只读或首写前事务可能还没有分配 {@code TransactionId}；该引用只用于运行期
 * 归属校验，不能持久化到 undo/redo。{@code undoNo}/{@code rollPointer} 是创建保存点时的当前逻辑链头，rollback-to
 * 成功后 {@link UndoContext} 会回到这组边界。
 *
 * @param transaction 所属事务实例，运行期归属校验使用，不能为 null。
 * @param undoNo      保存点边界 undoNo；{@link UndoNo#NONE} 表示保存点之前没有有效 undo record。
 * @param rollPointer 保存点边界 roll pointer；{@link RollPointer#NULL} 表示保存点之前没有有效回滚链入口。
 * @param sequence    同一 {@link UndoContext} 内单调递增的创建序号，用于诊断和稳定排序。
 */
public record TransactionSavepoint(
        Transaction transaction,
        UndoNo undoNo,
        RollPointer rollPointer,
        long sequence) {

    public TransactionSavepoint {
        if (transaction == null || undoNo == null || rollPointer == null) {
            throw new DatabaseValidationException("transaction savepoint fields must not be null");
        }
        if (sequence < 0) {
            throw new DatabaseValidationException("transaction savepoint sequence must be non-negative: " + sequence);
        }
    }
}
