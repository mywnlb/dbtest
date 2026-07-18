package cn.zhangyis.db.sql.parser.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.sql.parser.SourcePosition;

/** 带稳定 source position 和安全截断上下文的 SQL 语法/shape 错误。 */
public final class SqlSyntaxException extends DatabaseRuntimeException {
    /**
     * 构造或发布时固定的 {@code position} 版本、文件身份或源位置；必须来自当前对象上下文，诊断、并发校验和恢复路径依赖其稳定性。
     */
    private final SourcePosition position;
    /**
     * 创建 {@code SqlSyntaxException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param position SQL 源文本中的稳定位置；不得为 {@code null}，行列信息必须指向当前待绑定或解析语句
     * @param sql 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
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
