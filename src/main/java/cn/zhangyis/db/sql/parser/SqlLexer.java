package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;

import java.util.ArrayList;
import java.util.List;

/** 每次实例只持输入 cursor 的线性 lexer；不使用正则回溯。 */
final class SqlLexer {
    /**
     * 构造时冻结的 {@code sql} 文本；保存规范化标识、SQL 或诊断上下文，不得为空白，字符顺序在本对象生命周期内保持不变。
     */
    private final String sql;
    /**
     * 记录 {@code offset} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
     */
    private int offset;
    /**
     * 记录 {@code line} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private int line = 1;
    /**
     * 记录 {@code column} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private int column = 1;

    /**
     * 创建 {@code SqlLexer}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param sql 传给 {@code 构造} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     */
    SqlLexer(String sql) { this.sql = sql; }

    /**
     * 从当前 SQL token 与局部游标解析 {@code lex} 对应的语法结构；成功推进到确定边界，失败报告位置且不发布半解析 AST。
     *
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    List<Token> lex() {
        List<Token> tokens = new ArrayList<>();
        while (true) {
            skipWhitespace();
            SourcePosition position = position();
            if (offset == sql.length()) {
                tokens.add(new Token(TokenType.EOF, "", "", position));
                return List.copyOf(tokens);
            }
            char ch = sql.charAt(offset);
            switch (ch) {
                case ',' -> tokens.add(single(TokenType.COMMA));
                case '.' -> tokens.add(single(TokenType.DOT));
                case '(' -> tokens.add(single(TokenType.LPAREN));
                case ')' -> tokens.add(single(TokenType.RPAREN));
                case '=' -> tokens.add(single(TokenType.EQUALS));
                case '<' -> tokens.add(lessThan());
                case '>' -> tokens.add(greaterThan());
                case '*' -> tokens.add(single(TokenType.STAR));
                case ';' -> tokens.add(single(TokenType.SEMICOLON));
                case '`' -> tokens.add(quotedIdentifier());
                case '\'' -> tokens.add(stringToken(TokenType.STRING, false));
                default -> {
                    if ((ch == 'X' || ch == 'x' || ch == 'B' || ch == 'b') && peek(1) == '\'') {
                        tokens.add(stringToken(ch == 'X' || ch == 'x' ? TokenType.HEX : TokenType.BIT, true));
                    } else if (isIdentifierStart(ch)) {
                        tokens.add(identifier());
                    } else if (Character.isDigit(ch)
                            || (ch == '+' || ch == '-') && Character.isDigit(peek(1))) {
                        tokens.add(number());
                    } else {
                        throw syntax("unexpected character '" + ch + "'", position);
                    }
                }
            }
        }
    }

    private Token single(TokenType type) {
        SourcePosition start = position();
        char ch = take();
        return new Token(type, Character.toString(ch), Character.toString(ch), start);
    }

    /**
     * 读取小于或小于等于 token；{@code <>} 不属于当前 slice，必须整体拒绝，
     * 不能拆成两个可被后续语法误解释的 token。
     *
     * @return 保留完整词形与起始位置的比较 token
     * @throws SqlSyntaxException 遇到尚未支持的 {@code <>} 时抛出
     */
    private Token lessThan() {
        SourcePosition start = position();
        int begin = offset;
        take();
        if (peek(0) == '>') {
            take();
            throw syntax("operator '<>' is not supported", start);
        }
        TokenType type = peek(0) == '=' ? takeAndReturn(TokenType.LESS_EQUAL) : TokenType.LESS_THAN;
        String lexeme = sql.substring(begin, offset);
        return new Token(type, lexeme, lexeme, start);
    }

    /**
     * 读取大于或大于等于 token，并把二字符运算符作为一个不可分割词法单元。
     *
     * @return 保留完整词形与起始位置的比较 token
     */
    private Token greaterThan() {
        SourcePosition start = position();
        int begin = offset;
        take();
        TokenType type = peek(0) == '=' ? takeAndReturn(TokenType.GREATER_EQUAL) : TokenType.GREATER_THAN;
        String lexeme = sql.substring(begin, offset);
        return new Token(type, lexeme, lexeme, start);
    }

    private TokenType takeAndReturn(TokenType type) {
        take();
        return type;
    }

    private Token identifier() {
        SourcePosition start = position();
        int begin = offset;
        take();
        while (offset < sql.length() && isIdentifierPart(sql.charAt(offset))) take();
        String text = sql.substring(begin, offset);
        return new Token(TokenType.IDENT, text, text, start);
    }

    private Token quotedIdentifier() {
        SourcePosition start = position();
        int begin = offset;
        take();
        StringBuilder value = new StringBuilder();
        while (offset < sql.length()) {
            char ch = take();
            if (ch == '`') {
                if (peek(0) == '`') {
                    take();
                    value.append('`');
                } else {
                    if (value.isEmpty()) throw syntax("empty quoted identifier", start);
                    return new Token(TokenType.IDENT, value.toString(), sql.substring(begin, offset), start);
                }
            } else {
                value.append(ch);
            }
        }
        throw syntax("unterminated quoted identifier", start);
    }

    private Token stringToken(TokenType type, boolean prefixed) {
        SourcePosition start = position();
        int begin = offset;
        if (prefixed) take();
        if (take() != '\'') throw syntax("literal quote expected", start);
        StringBuilder value = new StringBuilder();
        while (offset < sql.length()) {
            char ch = take();
            if (ch == '\'') {
                if (peek(0) == '\'' && type == TokenType.STRING) {
                    take();
                    value.append('\'');
                } else {
                    String text = value.toString();
                    if (type == TokenType.HEX && (text.length() & 1) != 0) {
                        throw syntax("hex literal requires an even number of digits", start);
                    }
                    if (type == TokenType.HEX && !text.chars().allMatch(SqlLexer::isHex)) {
                        throw syntax("hex literal contains a non-hex digit", start);
                    }
                    if (type == TokenType.BIT && !text.chars().allMatch(c -> c == '0' || c == '1')) {
                        throw syntax("bit literal contains a non-bit digit", start);
                    }
                    return new Token(type, text, sql.substring(begin, offset), start);
                }
            } else {
                value.append(ch);
            }
        }
        throw syntax("unterminated literal", start);
    }

    private Token number() {
        SourcePosition start = position();
        int begin = offset;
        if (peek(0) == '+' || peek(0) == '-') take();
        while (Character.isDigit(peek(0))) take();
        if (peek(0) == '.') {
            take();
            if (!Character.isDigit(peek(0))) throw syntax("fraction requires digits", start);
            while (Character.isDigit(peek(0))) take();
        }
        if (peek(0) == 'e' || peek(0) == 'E') {
            take();
            if (peek(0) == '+' || peek(0) == '-') take();
            if (!Character.isDigit(peek(0))) throw syntax("exponent requires digits", start);
            while (Character.isDigit(peek(0))) take();
        }
        String lexeme = sql.substring(begin, offset);
        return new Token(TokenType.NUMBER, lexeme, lexeme, start);
    }

    private void skipWhitespace() {
        while (offset < sql.length() && Character.isWhitespace(sql.charAt(offset))) take();
    }

    private char take() {
        char ch = sql.charAt(offset++);
        if (ch == '\n') { line++; column = 1; } else { column++; }
        return ch;
    }

    private char peek(int ahead) {
        int index = offset + ahead;
        return index >= 0 && index < sql.length() ? sql.charAt(index) : '\0';
    }

    private SourcePosition position() { return new SourcePosition(offset, line, column); }
    private static boolean isIdentifierStart(char ch) { return Character.isLetter(ch) || ch == '_'; }
    private static boolean isIdentifierPart(char ch) { return Character.isLetterOrDigit(ch) || ch == '_' || ch == '$'; }
    private static boolean isHex(int ch) { return Character.digit(ch, 16) >= 0; }
    private SqlSyntaxException syntax(String message, SourcePosition position) {
        return new SqlSyntaxException(message, position, sql);
    }
}
