package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.sql.parser.SourcePosition;
public record NullLiteralNode(String lexeme, SourcePosition position) implements LiteralNode {
    @Override public Object value() { return null; }
}
