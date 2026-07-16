package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** Atomic DDL 日志身份；跨 crash 保持稳定且永不复用。 */
public record DdlId(long value) {
    public DdlId {
        if (value <= 0) {
            throw new DatabaseValidationException("ddl id must be positive: " + value);
        }
    }

    public static DdlId of(long value) {
        return new DdlId(value);
    }
}
