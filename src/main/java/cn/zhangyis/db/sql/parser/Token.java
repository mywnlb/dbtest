package cn.zhangyis.db.sql.parser;

/** 单个不可变 token。text 对 STRING/HEX/BIT 已去除引号并完成 framing 解码前校验。 */
record Token(TokenType type, String text, String lexeme, SourcePosition position) { }
