package cn.zhangyis.db.sql.expression;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.SourcePosition;

/**
 * Typed SQL IS NULL / IS NOT NULL 表达式。
 *
 * @param operand 已绑定 exact type 的 scalar；M3 Parser 只产生列引用，literal 形状供规则测试与后续扩展
 * @param operator 显式 null-test 操作符
 * @param position IS 左侧表达式的稳定源位置
 */
public record BoundNullTest(
        BoundExpression operand,
        BoundNullTestOperator operator,
        SourcePosition position) implements BoundExpression {

    /**
     * 创建 typed null-test；只接受 scalar operand，结果固定为 non-nullable boolean。
     *
     * @param operand 已绑定 exact type 的 scalar
     * @param operator IS NULL 或 IS NOT NULL
     * @param position 左侧 scalar 的稳定源位置
     * @throws DatabaseValidationException operand 非 scalar 或任一字段缺失时抛出
     */
    public BoundNullTest {
        if (operand == null || operator == null || position == null
                || !(operand.type() instanceof BoundExpressionType.Scalar)) {
            throw new DatabaseValidationException(
                    "bound null-test requires a scalar operand, operator and position");
        }
    }

    /**
     * SQL null-test 对 NULL 也产生确定结果，因此永不返回 UNKNOWN。
     *
     * @return nullable=false 的 boolean 类型
     */
    @Override
    public BoundExpressionType type() {
        return new BoundExpressionType.BooleanResult(false);
    }
}
