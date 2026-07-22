package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.domain.SpaceId;

/** scheduler 的包内自动 truncate 端口；生产实现是共享的 {@link UndoTablespaceTruncationService}。 */
@FunctionalInterface
interface UndoTruncationAttemptTarget {

    /**
     * @param spaceId 当前系统 undo space 的稳定身份
     * @param minReclaimableExtents 配置的正 extent 门槛
     * @return 完成、跳过或 deferred 的稳定结果
     */
    UndoTruncationAttemptResult tryTruncate(SpaceId spaceId, int minReclaimableExtents);
}
