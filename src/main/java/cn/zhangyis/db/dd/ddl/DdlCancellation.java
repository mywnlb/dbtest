package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 与CANCEL_REQUESTED同批持久化的固定宽度诊断信息。
 *
 * @param reasonCode 用户/生命周期取消的稳定原因；不得伪装自动DDL失败
 * @param requestedAtEpochMillis 仅供诊断的非负wall-clock时间，不参与恢复排序
 * @param requesterId admin/session/system的opaque正identity
 */
public record DdlCancellation(DdlCancellationReason reasonCode,
                              long requestedAtEpochMillis,
                              long requesterId) {
    public DdlCancellation {
        if (reasonCode == null || requestedAtEpochMillis < 0 || requesterId <= 0) {
            throw new DatabaseValidationException("invalid DDL cancellation fields");
        }
    }
}
