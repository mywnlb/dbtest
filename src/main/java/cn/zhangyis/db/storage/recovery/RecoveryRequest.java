package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.flush.DoublewriteRecoveryScanner;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;

import java.util.List;

/**
 * R2 crash recovery 输入。它显式携带 redo、checkpoint、doublewrite 和 page apply 依赖，避免 Recovery 模块读取各模块内部状态。
 *
 * @param mode 恢复模式。
 * @param checkpointStore redo control checkpoint label 仓储。
 * @param redoRepository redo data file 仓储。
 * @param dispatcher redo apply 分发器。
 * @param applyContext redo page apply 上下文。
 * @param doublewriteScanner 可选 doublewrite 修复器；为空表示跳过该阶段的实际页修复。
 * @param pagesToRepair R2 显式传入的待检查页；后续 tablespace discovery 会替代该列表来源。
 */
public record RecoveryRequest(RecoveryMode mode,
                              RedoCheckpointStore checkpointStore,
                              RedoLogFileRepository redoRepository,
                              RedoApplyDispatcher dispatcher,
                              RedoApplyContext applyContext,
                              DoublewriteRecoveryScanner doublewriteScanner,
                              List<PageId> pagesToRepair) {

    public RecoveryRequest {
        if (mode == null || checkpointStore == null || redoRepository == null
                || dispatcher == null || applyContext == null) {
            throw new DatabaseValidationException("recovery request core dependencies must not be null");
        }
        pagesToRepair = pagesToRepair == null ? List.of() : List.copyOf(pagesToRepair);
        if (doublewriteScanner == null && !pagesToRepair.isEmpty()) {
            throw new DatabaseValidationException("recovery pages require a doublewrite scanner");
        }
    }

    /**
     * 创建 NORMAL 模式请求。默认不检查 doublewrite 页，适合纯 redo replay 测试或后续 discovery 之前的空扫描。
     */
    public static RecoveryRequest normal(RedoCheckpointStore checkpointStore,
                                         RedoLogFileRepository redoRepository,
                                         RedoApplyDispatcher dispatcher,
                                         RedoApplyContext applyContext) {
        return new RecoveryRequest(RecoveryMode.NORMAL, checkpointStore, redoRepository,
                dispatcher, applyContext, null, List.of());
    }

    /**
     * 返回带 doublewrite repair 阶段输入的新请求。保持请求不可变，避免启动恢复过程中外部修改页列表。
     */
    public RecoveryRequest withDoublewriteRepair(DoublewriteRecoveryScanner scanner, List<PageId> pages) {
        if (scanner == null) {
            throw new DatabaseValidationException("doublewrite recovery scanner must not be null");
        }
        return new RecoveryRequest(mode, checkpointStore, redoRepository, dispatcher, applyContext, scanner, pages);
    }
}
