package cn.zhangyis.db.sql.parser;

/** Lexer token；关键字保留为 IDENT，由 parser 以 Locale.ROOT 大小写无关匹配。 */
enum TokenType { IDENT, STRING, NUMBER, HEX, BIT, COMMA, DOT, LPAREN, RPAREN, EQUALS, STAR, SEMICOLON, EOF }
