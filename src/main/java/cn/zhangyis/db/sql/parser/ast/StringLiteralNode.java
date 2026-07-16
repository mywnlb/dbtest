package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.sql.parser.SourcePosition;
public record StringLiteralNode(String value, String lexeme, SourcePosition position) implements LiteralNode { }
