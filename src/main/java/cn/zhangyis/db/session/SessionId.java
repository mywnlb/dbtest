package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 数据库实例内单调分配的 Session 身份，同时可安全映射为稳定 MDL owner。 */
public record SessionId(long value) {
    public SessionId {
        if (value <= 0) throw new DatabaseValidationException("session id must be positive");
    }
    public static SessionId of(long value) { return new SessionId(value); }
}
