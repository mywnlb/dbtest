package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * admin/system发起的固定宽度取消请求；不允许把用户名、SQL或任意诊断字符串写入恢复核心记录。
 *
 * @param reasonCode 稳定取消原因
 * @param requesterId admin/session/system opaque正identity
 * @param privileged 调用入口是否已经完成admin/system权限判断
 */
public record OnlineDdlCancelRequest(
        DdlCancellationReason reasonCode, long requesterId, boolean privileged) {

    public OnlineDdlCancelRequest {
        if (reasonCode == null || requesterId <= 0) {
            throw new DatabaseValidationException("invalid Online DDL cancel request");
        }
    }

    /** @return 已由admin/system入口授权的取消请求。 */
    public static OnlineDdlCancelRequest admin(
            DdlCancellationReason reasonCode, long requesterId) {
        return new OnlineDdlCancelRequest(reasonCode, requesterId, true);
    }

    /** @return 用于验证权限拒绝的未授权请求；public facade不得执行其CAS。 */
    public static OnlineDdlCancelRequest unprivileged(
            DdlCancellationReason reasonCode, long requesterId) {
        return new OnlineDdlCancelRequest(reasonCode, requesterId, false);
    }
}
