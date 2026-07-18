package cn.zhangyis.db.storage.api.undotruncate;
import cn.zhangyis.db.storage.fil.state.TablespaceState;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;

/** 完成一次新截断或 TRUNCATING 续作后的稳定结果。
 *
 * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
 * @param truncateEpoch 参与 {@code 构造} 的单调版本值 {@code truncateEpoch}；必须非负，回退或与权威快照冲突时拒绝
 * @param targetSizeInPages 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
 * @param markerLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
 * @param finalState 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
 */
public record UndoTablespaceTruncationResult(
        SpaceId spaceId,
        long truncateEpoch,
        PageNo targetSizeInPages,
        Lsn markerLsn,
        TablespaceState finalState) {

    public UndoTablespaceTruncationResult {
        if (spaceId == null || targetSizeInPages == null || markerLsn == null || finalState == null) {
            throw new DatabaseValidationException("undo truncation result fields must not be null");
        }
    }
}
