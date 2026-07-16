package cn.zhangyis.db.sql.parser.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.sql.parser.SourcePosition;

/** 带稳定 source position 和安全截断上下文的 SQL 语法/shape 错误。 */
public final class SqlSyntaxException extends DatabaseRuntimeException {
    private final SourcePosition position;
    public SqlSyntaxException(String message, SourcePosition position, String sql) {
        super(message + " at " + position.line() + ":" + position.column() + " near '" + context(sql, position) + "'");
        this.position = position;
    }
    public SourcePosition position() { return position; }
    private static String context(String sql, SourcePosition position) {
        if (sql == null) return "";
        int from = Math.max(0, position.offset() - 12);
        int to = Math.min(sql.length(), position.offset() + 20);
        return sql.substring(from, to).replace('\n', ' ');
    }
}
