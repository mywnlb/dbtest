package cn.zhangyis.db.storage.api.undotruncate;
import cn.zhangyis.db.storage.fil.state.TablespaceState;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;

/** 完成一次新截断或 TRUNCATING 续作后的稳定结果。 */
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
