package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.sql.parser.SourcePosition;

/** 未绑定 literal；value 仅为 parser 级文本/字节语义，不含目标列类型。 */
public sealed interface LiteralNode permits NullLiteralNode, NumericLiteralNode, StringLiteralNode,
        HexLiteralNode, BitLiteralNode {
    /**
     * 从当前 SQL token 与局部游标解析 {@code value} 对应的语法结构；成功推进到确定边界，失败报告位置且不发布半解析 AST。
     *
     * @return {@code value} 保存的非空 SQL 字面量值；具体 Java 类型由字面量种类决定，并保持解析阶段的精确值语义
     */
    Object value();
    /**
     * 从当前 SQL token 与局部游标解析 {@code lexeme} 对应的语法结构；成功推进到确定边界，失败报告位置且不发布半解析 AST。
     *
     * @return {@code lexeme} 生成的非空文本表示；字符顺序保持 SQL、标识符或诊断格式约定，无结果时返回空串而非 {@code null}
     */
    String lexeme();
    /**
     * 从当前 SQL token 与局部游标解析 {@code position} 对应的语法结构；成功推进到确定边界，失败报告位置且不发布半解析 AST。
     *
     * @return {@code position} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    SourcePosition position();
}
