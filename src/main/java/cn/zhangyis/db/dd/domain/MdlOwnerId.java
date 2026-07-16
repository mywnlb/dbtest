package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** MDL owner 身份；独立于 session 包，使 DD 内核不反向依赖尚未实现的协议/session 生命周期。 */
public record MdlOwnerId(long value) {
    public MdlOwnerId {
        if (value <= 0) {
            throw new DatabaseValidationException("metadata lock owner id must be positive: " + value);
        }
    }

    public static MdlOwnerId of(long value) {
        return new MdlOwnerId(value);
    }
}
