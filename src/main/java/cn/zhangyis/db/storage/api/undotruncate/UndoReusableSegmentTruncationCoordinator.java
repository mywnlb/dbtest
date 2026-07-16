package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.trx.UndoSegmentReuseDirectory;
import cn.zhangyis.db.storage.undo.RollbackSegmentHeaderRepository;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoSpaceAllocator;

/**
 * cache/free 统一 truncate drain 的公开语义入口。包内父类封装批量校验、FSP 回收与 page3 重写细节，
 * 生产组合根只依赖本类型，避免向上暴露 truncate 内部阶段。
 */
public final class UndoReusableSegmentTruncationCoordinator
        extends UndoReusableSegmentTruncationSupport {

    /**
     * 创建与同一 StorageEngine page3/FSP/运行期目录绑定的 reusable-owner drain 协作者。
     *
     * @param mtrManager truncate 校验及物理批次的 MTR 来源。
     * @param undoAccess cache/free 首页格式与 owner 校验入口。
     * @param allocator FSP inode 检查和 segment drop 入口。
     * @param headerRepository page3 active/cache/free owner 仓储。
     * @param rollbackSegmentId 当前唯一 rollback segment 标识。
     * @param slotCapacity page3 active slot 容量。
     * @param cacheCapacityPerKind page3 每类 cache 容量。
     * @param reuseDirectory page3 可复用 owner 的统一运行期投影。
     */
    public UndoReusableSegmentTruncationCoordinator(
            MiniTransactionManager mtrManager,
            UndoLogSegmentAccess undoAccess,
            UndoSpaceAllocator allocator,
            RollbackSegmentHeaderRepository headerRepository,
            RollbackSegmentId rollbackSegmentId,
            int slotCapacity,
            int cacheCapacityPerKind,
            UndoSegmentReuseDirectory reuseDirectory) {
        super(mtrManager, undoAccess, allocator, headerRepository, rollbackSegmentId,
                slotCapacity, cacheCapacityPerKind, reuseDirectory);
    }
}
