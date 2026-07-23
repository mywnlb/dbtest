package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

import java.util.List;

/**
 * 保持用户顺序的 SQL AND 表达式。
 *
 * @param operands 至少两个 boolean operand；嵌套结构由规则层展平
 */
public record BoundConjunction(List<BoundExpression> operands)
        implements BoundExpression {

    public BoundConjunction {
        if (operands == null || operands.size() < 2
                || operands.stream().anyMatch(java.util.Objects::isNull)
                || operands.stream().anyMatch(operand ->
                !(operand.type() instanceof BoundExpressionType.BooleanResult))) {
            throw new DatabaseValidationException(
                    "bound conjunction requires at least two boolean operands");
        }
        operands = List.copyOf(operands);
    }

    @Override
    public BoundExpressionType type() {
        return new BoundExpressionType.BooleanResult(
                operands.stream().map(BoundExpression::type)
                        .map(BoundExpressionType.BooleanResult.class::cast)
                        .anyMatch(BoundExpressionType.BooleanResult::nullable));
    }

    @Override
    public SourcePosition position() {
        return operands.getFirst().position();
    }
}
