package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

import java.util.List;
import java.util.Optional;

/**
 * 尚未绑定的列引用。一个标识符表示当前作用域中的未限定列，两个标识符表示
 * {@code table-or-alias.column}；schema/table/column 三段列名留给后续切片。
 *
 * @param parts 保持源顺序的一段或两段标识符
 */
public record ColumnReferenceNode(List<IdentifierNode> parts) {

    public ColumnReferenceNode {
        if (parts == null || parts.size() < 1 || parts.size() > 2
                || parts.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "column reference must contain one or two identifiers");
        }
        parts = List.copyOf(parts);
    }

    /**
     * 把旧单表 AST 的裸标识符提升为未限定列引用。
     *
     * @param column Parser 读取的列标识符
     */
    public ColumnReferenceNode(IdentifierNode column) {
        this(List.of(column));
    }

    /**
     * 返回最终列名，兼容既有 AST 消费者的 {@code column().value()} 读取形状。
     *
     * @return 源文本中的列名，不包含 qualifier
     */
    public String value() {
        return parts.getLast().value();
    }

    /**
     * 返回可选表名或别名限定符。
     *
     * @return 两段引用的第一段；未限定列为空
     */
    public Optional<IdentifierNode> qualifier() {
        return parts.size() == 2
                ? Optional.of(parts.getFirst()) : Optional.empty();
    }

    /**
     * 诊断位置从限定符或裸列名的第一个 token 开始。
     *
     * @return 当前完整引用的源起始位置
     */
    public SourcePosition position() {
        return parts.getFirst().position();
    }
}
