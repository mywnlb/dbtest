package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;

/** 正在 queue 中等待的 MDL 诊断行。 */
public record WaitingMetadataLock(long requestId, MdlOwnerId owner, MdlKey key, MdlMode mode,
                                  MdlDuration duration, MdlRequestState state, long waitNanos) {
}
