package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

/**
 * 两个 exact-type scalar 的 SQL comparison。
 *
 * @param left 左 scalar；规则允许在不改变语义时调整操作数方向
 * @param operator comparison 操作符
 * @param right 右 scalar
 */
public record BoundComparison(
        BoundExpression left, BoundComparisonOperator operator,
        BoundExpression right) implements BoundExpression {

    public BoundComparison {
        if (left == null || operator == null || right == null
                || !(left.type() instanceof BoundExpressionType.Scalar leftType)
                || !(right.type() instanceof BoundExpressionType.Scalar rightType)
                || !leftType.definition().equals(rightType.definition())) {
            throw new DatabaseValidationException(
                    "bound comparison requires matching exact scalar types");
        }
    }

    @Override
    public BoundExpressionType type() {
        BoundExpressionType.Scalar leftType =
                (BoundExpressionType.Scalar) left.type();
        BoundExpressionType.Scalar rightType =
                (BoundExpressionType.Scalar) right.type();
        return new BoundExpressionType.BooleanResult(
                leftType.nullable() || rightType.nullable());
    }

    @Override
    public SourcePosition position() {
        return left.position();
    }
}
