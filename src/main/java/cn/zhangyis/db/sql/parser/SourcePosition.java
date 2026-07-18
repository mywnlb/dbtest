package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** UTF-16 输入中的零基 offset 与一基 line/column。
 *
 * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
 * @param line 源文本中的一基 {@code line}；必须大于零，用于稳定定位语法或协议错误
 * @param column 源文本中的一基 {@code column}；必须大于零，用于稳定定位语法或协议错误
 */
public record SourcePosition(int offset, int line, int column) {
    public SourcePosition {
        if (offset < 0 || line <= 0 || column <= 0) {
            throw new DatabaseValidationException("invalid SQL source position");
        }
    }
}
