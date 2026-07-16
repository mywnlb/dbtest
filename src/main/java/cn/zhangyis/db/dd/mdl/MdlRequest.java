package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;

/** 一次不可变 MDL 申请；ticket 生命周期由 manager 授予后建立。 */
public record MdlRequest(MdlOwnerId owner, MdlKey key, MdlMode mode, MdlDuration duration) {
    public MdlRequest {
        if (owner == null || key == null || mode == null || duration == null) {
            throw new DatabaseValidationException("metadata lock request fields must not be null");
        }
    }
}
