package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 稳定 index 身份；B+Tree page header 和 undo record 通过它解析正确的 schema/key definition。 */
public record IndexId(long value) {
    public IndexId {
        if (value <= 0) {
            throw new DatabaseValidationException("index id must be positive: " + value);
        }
    }

    public static IndexId of(long value) {
        return new IndexId(value);
    }
}
