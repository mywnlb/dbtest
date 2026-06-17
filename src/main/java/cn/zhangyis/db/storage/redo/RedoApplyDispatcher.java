package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * Redo apply 分发器（R1 简化版）。当前只注册 page 物理 handler，负责 PAGE_INIT/PAGE_BYTES。
 */
public final class RedoApplyDispatcher {

    /** PAGE_INIT/PAGE_BYTES 物理页 handler。 */
    private final PageRedoApplyHandler pageHandler;

    private RedoApplyDispatcher(PageRedoApplyHandler pageHandler) {
        this.pageHandler = pageHandler;
    }

    /** 创建只包含 page handler 的恢复分发器。后续逻辑 redo handler 可在此扩展注册表。 */
    public static RedoApplyDispatcher pageDispatcher() {
        return new RedoApplyDispatcher(new PageRedoApplyHandler());
    }

    /** 应用一个 redo 批次。 */
    public void apply(RedoLogBatch batch, RedoApplyContext context) {
        if (batch == null || context == null) {
            throw new DatabaseValidationException("redo apply batch/context must not be null");
        }
        pageHandler.apply(batch, context);
    }

    /** 按文件顺序应用多个 redo 批次。 */
    public void applyAll(List<RedoLogBatch> batches, RedoApplyContext context) {
        if (batches == null) {
            throw new DatabaseValidationException("redo apply batches must not be null");
        }
        for (RedoLogBatch batch : batches) {
            apply(batch, context);
        }
    }
}
