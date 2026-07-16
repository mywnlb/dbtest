package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.sql.parser.SourcePosition;
public record HexLiteralNode(String value, String lexeme, SourcePosition position) implements LiteralNode { }
