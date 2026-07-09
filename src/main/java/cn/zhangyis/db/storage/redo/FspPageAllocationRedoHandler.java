package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;

import java.util.List;

/**
 * FSP page allocation/free 逻辑 redo handler。0.19b 只把 allocation intent 用于恢复期物理容量对齐：
 * 若崩溃发生在 autoextend 尚未 durable 但 redo 已 durable 的窗口，先把数据文件扩到能容纳目标页，再让后续
 * {@link PageRedoApplyHandler} 重放 {@link PageInitRecord}/{@link PageBytesRecord}。
 *
 * <p>0.19c 起 free intent 也由本 handler 接住，但 apply 为 no-op：它只给 FORCE_SKIP 和诊断提供 affected page，
 * 不重新执行 free-list 或 segment 状态机。FSP 元数据页仍由同一 batch 的 metadata delta/PAGE_BYTES 恢复。
 */
public final class FspPageAllocationRedoHandler implements RedoApplyHandler {

    @Override
    public boolean supports(RedoRecord record) {
        return record instanceof FspPageAllocationRecord || record instanceof FspPageFreeRecord;
    }

    @Override
    public List<PageId> affectedPages(RedoRecord record) {
        if (record instanceof FspPageAllocationRecord allocation) {
            return List.of(allocation.allocatedPageId());
        }
        if (record instanceof FspPageFreeRecord free) {
            return List.of(free.freedPageId());
        }
        throw unsupported(record);
    }

    @Override
    public RedoApplyBatchHandler openBatch(LogRange range, RedoApplyContext context) {
        if (range == null || context == null) {
            throw new DatabaseValidationException("FSP allocation redo range/context must not be null");
        }
        return new Batch(context);
    }

    private static FspPageAllocationRecord requireAllocationRecord(RedoRecord record) {
        if (record instanceof FspPageAllocationRecord allocation) {
            return allocation;
        }
        throw unsupported(record);
    }

    private static RedoLogCorruptedException unsupported(RedoRecord record) {
        return new RedoLogCorruptedException("unsupported FSP intent redo record: "
                + (record == null ? "null" : record.getClass().getName()));
    }

    private static final class Batch implements RedoApplyBatchHandler {

        /** recovery 物理页上下文；本 handler 只调用 PageStore.ensureCapacity，不读取或写回页内容。 */
        private final RedoApplyContext context;

        private Batch(RedoApplyContext context) {
            this.context = context;
        }

        @Override
        public void apply(RedoRecord record) {
            if (record instanceof FspPageAllocationRecord allocation) {
                PageId pageId = allocation.allocatedPageId();
                context.pageStore().ensureCapacity(pageId.spaceId(), PageNo.of(pageId.pageNo().value() + 1));
            } else if (!(record instanceof FspPageFreeRecord)) {
                throw unsupported(record);
            }
        }

        @Override
        public void finish() {
            // FSP allocation intent 没有批末 pageLSN 写回；pageLSN 仍由 page handler 处理物理页。
        }
    }
}
