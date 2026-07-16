package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.sql.parser.SourcePosition;

/** 未绑定 literal；value 仅为 parser 级文本/字节语义，不含目标列类型。 */
public sealed interface LiteralNode permits NullLiteralNode, NumericLiteralNode, StringLiteralNode,
        HexLiteralNode, BitLiteralNode {
    Object value();
    String lexeme();
    SourcePosition position();
}
