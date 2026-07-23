package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

/**
 * SQL NOT 语法节点。
 *
 * @param operand 被否定的 boolean 子树；连续 NOT 通过递归节点显式保留
 * @param position NOT token 的源起始位置，用于稳定语法与绑定诊断
 */
public record NegationExpressionNode(
        BooleanExpressionNode operand,
        SourcePosition position) implements BooleanExpressionNode {

    /**
     * 创建保留 NOT token 位置的不可变否定节点。
     *
     * @param operand 被否定的完整 boolean 子树
     * @param position NOT token 在同一 SQL 文本中的位置
     * @throws DatabaseValidationException operand 或 position 缺失时抛出
     */
    public NegationExpressionNode {
        if (operand == null || position == null) {
            throw new DatabaseValidationException(
                    "boolean negation operand/position must not be null");
        }
    }
}
