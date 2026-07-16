package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;

/** 已授予 MDL 的只读诊断行。 */
public record GrantedMetadataLock(long requestId, MdlOwnerId owner, MdlKey key,
                                  MdlMode mode, MdlDuration duration) {
}
