package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;

import java.util.ArrayList;
import java.util.List;

/** 每次实例只持输入 cursor 的线性 lexer；不使用正则回溯。 */
final class SqlLexer {
    private final String sql;
    private int offset;
    private int line = 1;
    private int column = 1;

    SqlLexer(String sql) { this.sql = sql; }

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
