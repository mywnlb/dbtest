package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.List;

/**
 * 事务状态 logical redo 的恢复 handler。non-page record 按 batch 顺序交给可注入的
 * {@link TransactionStateDeltaSink}；默认 sink 为 no-op，正式 StorageEngine recovery 注入线程独占事务恢复表。
 */
public final class TransactionStateRedoHandler implements RedoApplyHandler {

    /** 顺序消费端口；默认 no-op 维持通用 page replay 的兼容行为。 */
    private final TransactionStateDeltaSink sink;

    /** 创建不收集事务恢复证据的兼容 handler。 */
    public TransactionStateRedoHandler() {
        this(TransactionStateDeltaSink.NO_OP);
    }

    /** 创建把已解码 record 顺序交给指定 sink 的 handler。 */
    public TransactionStateRedoHandler(TransactionStateDeltaSink sink) {
        if (sink == null) {
            throw new DatabaseValidationException("transaction state delta sink must not be null");
        }
        this.sink = sink;
    }

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
        return new Batch(range, sink);
    }

    /** 单个 redo batch 的交付会话；只持不可变 range/sink，不访问页或事务系统。 */
    private static final class Batch implements RedoApplyBatchHandler {

        private final LogRange range;
        private final TransactionStateDeltaSink sink;

        private Batch(LogRange range, TransactionStateDeltaSink sink) {
            this.range = range;
            this.sink = sink;
        }

        @Override
        public void apply(RedoRecord record) {
            if (!(record instanceof TransactionStateDeltaRecord delta)) {
                throw new DatabaseValidationException("transaction state handler received unsupported record: "
                        + record.getClass().getName());
            }
            sink.accept(range, delta);
        }

        @Override
        public void finish() {
        }
    }
}
