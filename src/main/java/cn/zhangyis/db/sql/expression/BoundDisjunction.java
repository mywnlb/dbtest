package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

import java.util.List;

/**
 * 保持用户顺序的 typed SQL OR 表达式。
 *
 * @param operands 至少两个 boolean operand；嵌套结构由规则层展平
 */
public record BoundDisjunction(
        List<BoundExpression> operands) implements BoundExpression {

    /**
     * 冻结 typed OR operand 并校验每个 child 都产生 boolean 结果。
     *
     * @param operands 至少两个、保持用户顺序的 typed boolean operand
     * @throws DatabaseValidationException 列表缺失、元素不足、含空值或 scalar operand 时抛出
     */
    public BoundDisjunction {
        if (operands == null || operands.size() < 2
                || operands.stream().anyMatch(java.util.Objects::isNull)
                || operands.stream().anyMatch(operand ->
                !(operand.type()
                        instanceof BoundExpressionType.BooleanResult))) {
            throw new DatabaseValidationException(
                    "bound disjunction requires at least two boolean operands");
        }
        operands = List.copyOf(operands);
    }

    /**
     * 返回保守的 SQL boolean nullable；任一 operand 可能 UNKNOWN 时结果类型允许 UNKNOWN。
     *
     * @return 不可变 boolean 类型，nullable 是全部 operand nullable 的并集
     */
    @Override
    public BoundExpressionType type() {
        return new BoundExpressionType.BooleanResult(
                operands.stream().map(BoundExpression::type)
                        .map(BoundExpressionType.BooleanResult.class::cast)
                        .anyMatch(BoundExpressionType.BooleanResult::nullable));
    }

    /**
     * OR 规则生成节点继承最左用户表达式位置。
     *
     * @return 最左 operand 的稳定源位置
     */
    @Override
    public SourcePosition position() {
        return operands.getFirst().position();
    }
}
