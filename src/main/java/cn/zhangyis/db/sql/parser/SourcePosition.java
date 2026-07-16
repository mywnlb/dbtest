package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** UTF-16 输入中的零基 offset 与一基 line/column。 */
public record SourcePosition(int offset, int line, int column) {
    public SourcePosition {
        if (offset < 0 || line <= 0 || column <= 0) {
            throw new DatabaseValidationException("invalid SQL source position");
        }
    }
}
