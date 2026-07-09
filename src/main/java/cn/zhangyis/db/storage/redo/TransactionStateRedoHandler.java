package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.List;

/**
 * 事务状态 logical redo 的恢复 handler。当前切片只保证 non-page record 可以保序扫描并被诊断性消费；
 * 真正的事务活跃表/提交表恢复仍由后续切片结合 undo/rseg 状态实现。
 */
public final class TransactionStateRedoHandler implements RedoApplyHandler {

    @Override
    public boolean supports(RedoRecord record) {
        return record instanceof TransactionStateDeltaRecord;
    }

    /**
     * 事务状态 redo 不修改物理页，因此不触发表空间 skip predicate，也不访问 PageStore。
     */
    @Override
    public List<PageId> affectedPages(RedoRecord record) {
        if (!(record instanceof TransactionStateDeltaRecord)) {
            throw new DatabaseValidationException("transaction state handler received unsupported record: "
                    + record.getClass().getName());
        }
        return List.of();
    }

    @Override
    public RedoApplyBatchHandler openBatch(LogRange range, RedoApplyContext context) {
        if (range == null || context == null) {
            throw new DatabaseValidationException("transaction state redo apply range/context must not be null");
        }
        return new Batch();
    }

    /** 当前无运行时副作用，只校验 record 类型，保留恢复顺序中的消费点。 */
    private static final class Batch implements RedoApplyBatchHandler {

        @Override
        public void apply(RedoRecord record) {
            if (!(record instanceof TransactionStateDeltaRecord)) {
                throw new DatabaseValidationException("transaction state handler received unsupported record: "
                        + record.getClass().getName());
            }
        }

        @Override
        public void finish() {
        }
    }
}
