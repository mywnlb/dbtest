package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

import java.util.List;

/**
 * 保持用户书写顺序的 SQL AND 语法节点。
 *
 * @param operands 至少两个 boolean operand；列表顺序决定诊断与后续规则的稳定顺序
 */
public record ConjunctionExpressionNode(
        List<BooleanExpressionNode> operands)
        implements BooleanExpressionNode {

    /**
     * 冻结至少两个 boolean operand；失败时不发布部分 AST。
     *
     * @param operands 按用户书写顺序排列的 operand
     * @throws DatabaseValidationException 列表缺失、元素不足或包含 Java null 时抛出
     */
    public ConjunctionExpressionNode {
        if (operands == null || operands.size() < 2
                || operands.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "boolean conjunction requires at least two operands");
        }
        operands = List.copyOf(operands);
    }

    /**
     * AND 自身不引入独立运行时值，诊断位置继承最左 operand。
     *
     * @return 最左 operand 的稳定源位置
     */
    @Override
    public SourcePosition position() {
        return operands.getFirst().position();
    }
}
